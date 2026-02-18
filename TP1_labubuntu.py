"""
Laboratorio de Datos - Verano 2026

Integrantes:
- Lanabere, Delfina Daniela (LU: 246/24)
- Muhafra, Micaela Abril (LU: 1327/24)
- Gomez Arreaza, Catherine De Jesus (LU: 980/24)

Breve descripción del contenido:
Este archivo contiene el flujo de trabajo completo de procesamiento de datos para el análisis
de las diversas instituciones de salud y la mortalidad en Argentina. Incluyendo datos
censales del 2010 y 2022. 

Dicho flujo de trabajo se divide en:
1. Limpieza: Procesamiento de archivos Excel (Censo 2010/2022) con lógica 
   de estados para normalizar jurisdicciones, coberturas y rangos etarios.
2. Modelado SQL: Transformación de datos sin procesar en tablas relacionales 
   (Habitantes, Defunciones, Establecimientos) mediante consultas complejas y uniones.
3. Reportes: Generación de 5 reportes que incluyen tasas de mortalidad 
   normalizadas y análisis de frecuencias de causas de muerte.
4. Visualización: Implementación de gráficos para mejor interpretación de los datos
   y comparaciones.

Datos relevantes:
- Se normalizaron las jurisdicciones (ej. CABA y Tierra del Fuego) para asegurar 
  la integridad referencial entre tablas.
- El análisis de mortalidad se calculó con tasas cada 1.000 habitantes para 
  permitir comparaciones interprovinciales justas.
"""

#%% LIBRERIAS

import pandas as pd
import duckdb as dd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

#%% DATASETS

# Carga de archivos.

carpeta = '/home/itmedelfi/Desktop/EXACTAS/LABO_DATOS/tp1/Archivos originales'

censo2010               = pd.read_excel(carpeta + "/censo2010.xlsX", skiprows=14)
censo2022               = pd.read_excel(carpeta + "/censo2022.xlsX", skiprows=14)
archivo_defunciones     = pd.read_csv(carpeta + "/defunciones.csv")
instituciones_de_salud  = pd.read_excel(carpeta + "/instituciones_de_salud.xlsx")
categoriasDefunciones   = pd.read_csv(carpeta + "/categoriasDefunciones.csv")

#%% CENSO 2010

censo2010.columns = ['vacia', 'col_cobertura', 'col_edad', 'col_varon', 'col_mujer', 'col_total']

# Estructuras temporales para manejar datos de rango etario, provincia, cobertura y sexo.
suma_total = {} 
provincia_actual = ""
cobertura_actual = ""

for _, fila in censo2010.iterrows():
    val_cobertura = str(fila['col_cobertura'])
    val_edad = str(fila['col_edad'])
    
    if "AREA" in val_cobertura:
        provincia_actual = str(fila['col_edad'])
        if str(fila['col_edad']) == 'Tierra del Fuego':
            provincia_actual = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
    # Detecta el cambio de jurisdiccion y se actualiza el estado
    # y, en caso de encotrar la jurisdiccion 'Tierra del Fuego' le cambia el nombre
    # para que coincida en la tabla de provincias
    
    elif "RESUMEN" in val_cobertura:
        break
    # Evita procesar datos demas, marca el fin de los datos utiles.
    
    if val_cobertura == "Total":
        cobertura_actual = "nan" 
    if val_cobertura != "nan" and val_cobertura != "Total":
        cobertura_actual = val_cobertura
    # Actualizaciones de estado de cobertura_actual.

    if val_edad.isdigit() and cobertura_actual != "nan" :
        edad_nro = int(val_edad)
        if edad_nro <= 14: rango = '0-14'
        elif edad_nro <= 34: rango = '15-34'
        elif edad_nro <= 54: rango = '35-54'
        elif edad_nro <= 74: rango = '55-74'
        else: rango = '75+'
    # Agrupacion de los rangos etarios y limpieza de valores nulos.
        
        mujeres = float(fila['col_mujer']) if str(fila['col_mujer']) != "-" else 0
        varones = float(fila['col_varon']) if str(fila['col_varon']) != "-" else 0

        clave = (provincia_actual, cobertura_actual, rango)

        if clave not in suma_total:
            suma_total[clave] = {'mujer': 0, 'varon': 0}
        
        suma_total[clave]['mujer'] += mujeres
        suma_total[clave]['varon'] += varones
        # Suma de habitantes separados por sexo al diccionario.

# Conversión del diccionario completo a un dataframe.
lista_final = []
for clave, valor in suma_total.items():
    fila = {'Provincia': clave[0],
            'Cobertura': clave[1],
            'Rango etario': clave[2],
            'Mujer': valor['mujer'],
            'Varón': valor['varon']}
    lista_final.append(fila)

df_censo_2010 = pd.DataFrame(lista_final)
df_censo_2010.to_csv("df_censo_2010.csv", index = False)

#%% CENSO 2022

# Mismos procesos que con censo 2010.

censo2022.columns = ['vacia', 'col_cobertura', 'col_edad', 'col_varon', 'col_mujer', 'col_total']

suma_total = {} 
provincia_actual = ""
cobertura_actual = ""

for _, fila in censo2022.iterrows():
    val_cobertura = str(fila['col_cobertura'])
    val_edad = str(fila['col_edad'])
    
    if "AREA" in val_cobertura:
        provincia_actual = str(fila['col_edad'])
        if str(fila['col_edad']) == 'Caba':
            provincia_actual = 'Ciudad Autónoma de Buenos Aires'
        elif str(fila['col_edad']) == 'Tierra del Fuego':
            provincia_actual = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
        
    elif "RESUMEN" in val_cobertura:
        break

    if val_cobertura == "Total":
            cobertura_actual = "nan" 

    if val_cobertura != "nan" and val_cobertura != "Total":
            cobertura_actual = val_cobertura
            
    elif val_edad.isdigit() and cobertura_actual != "nan":
        edad_nro = int(val_edad)
        if edad_nro <= 14: rango = '0-14'
        elif edad_nro <= 34: rango = '15-34'
        elif edad_nro <= 54: rango = '35-54'
        elif edad_nro <= 74: rango = '55-74'
        else: rango = '75+'
        
        m = float(fila['col_mujer']) if str(fila['col_mujer']) != "-" else 0
        v = float(fila['col_varon']) if str(fila['col_varon']) != "-" else 0

        clave = (provincia_actual, cobertura_actual, rango)

        if clave not in suma_total:
            suma_total[clave] = {'mujer': 0, 'varon': 0}
        
        suma_total[clave]['mujer'] += m
        suma_total[clave]['varon'] += v

lista_final = []
for clave, valor in suma_total.items():
    fila = {'Provincia': clave[0],
            'Cobertura': clave[1],
            'Rango etario': clave[2],
            'Mujer': valor['mujer'],
            'Varón': valor['varon']}
    lista_final.append(fila)

df_censo_2022 = pd.DataFrame(lista_final)
df_censo_2022.to_csv("df_censo_2022.csv", index = False)

#%% Tabla de defunciones

# Creamos el dataframe df_defunciones con las defunciones agregadas por: Categoría de defunción, año, provincia, grupo etario 
# y sexo. Para ello, realizamos un INNER JOIN entre la tabla derivada del archivo "defunciones.csv"  y la tabla 
# "categoriasDefunciones.csv", permitiendo agrupar los datos por todas las variables mencionadas y calcular la suma de 
# defunciones para cada combinación.
# Además, filtramos los casos en que no tengamos información conclusiva en las categorias de Grupo etario y Sexo.

consultaSQL = """
                SELECT 
                    c.categorias AS Descripción,
                    d.anio AS Año,
                    d.jurisdiccion_de_residencia_id AS 'Provincia ID',
                    CASE
                        WHEN d.grupo_edad LIKE '01.%' THEN '0-14'
                        WHEN d.grupo_edad LIKE '02.%' THEN '15-34'
                        WHEN d.grupo_edad LIKE '03.%' THEN '35-54'
                        WHEN d.grupo_edad LIKE '04.%' THEN '55-74'
                        WHEN d.grupo_edad LIKE '05.%' THEN '75+'
                        END AS 'Grupo etario',
                    CASE
                        WHEN d.Sexo = 'femenino' THEN 'F'
                        WHEN d.Sexo = 'masculino' THEN 'M'
                        END AS Sexo,
                    SUM(cantidad) AS Cantidad
                    FROM archivo_defunciones AS d
                    INNER JOIN categoriasDefunciones AS c
                    ON c.codigo_def = d.cie10_causa_id
                    WHERE d.grupo_edad NOT LIKE '06.%' AND d.Sexo IN ('femenino','masculino')
                    GROUP BY c.categorias, d.grupo_edad, d.Sexo, d.anio, d.jurisdiccion_de_residencia_id;
            """

df_defunciones = dd.sql(consultaSQL).df()
df_defunciones.to_csv("df_defunciones.csv", index = False)

#%% Tabla de provincias

# Creamos el dataframe df_provincias con las provincias agregadas por: ID de la provincia y su nombre correspondiente
# con los datos generados por la tabla de "defunciones.csv", filtrando los registros repetidos y aquellos casos que sean NULL 
# o no proporcionen información.

consultaSQL = """
                SELECT DISTINCT jurisdiccion_de_residencia_id AS ID, jurisdicion_residencia_nombre AS Nombre
                FROM archivo_defunciones
                WHERE jurisdicion_residencia_nombre IS NOT NULL AND jurisdicion_residencia_nombre <> 'Sin Información'
                ORDER BY id;
              """
            
df_provincias = dd.sql(consultaSQL).df()
df_provincias.to_csv("df_provincias.csv", index = False)

#%% Tabla de departamentos

# Creamos el dataframe df_departamentos con los departamentos agregados por: ID del departamento, su nombre correspondiente
# y el ID de la provincia a la que pertenecen, con los datos generados por la tabla de "instituciones_de_salud.xlsx".
# Aplicamos un CASE WHEN para normalizar los IDs de aquellos departamentos que se llaman igual y están en la misma provincia 
# pero tenían códigos distintos.
# Además, accedemos a la columna "Nombre" para capitalizar el nombre de los departamentos asegurando consistencia con el 
# formato del dataframe de provincias.

consultaSQL = """
                SELECT DISTINCT 
                    CASE 
                        WHEN departamento_nombre = 'ZAPALA' AND provincia_id = '58'  THEN 112
                        WHEN departamento_nombre = 'QUILMES' AND provincia_id = '6' THEN 658
                        WHEN departamento_nombre = 'COMUNA 1' AND provincia_id = '2' THEN 1
                        ELSE departamento_id
                        END AS "Departamento ID",
                        departamento_nombre AS Nombre, provincia_id AS "Provincia ID"
                FROM instituciones_de_salud;
              """
            
df_departamentos = dd.sql(consultaSQL).df()

df_departamentos["Nombre"] = df_departamentos["Nombre"].str.title()
    
df_departamentos.to_csv("df_departamentos.csv", index = False)

#%% Tabla de habitantes 

# Creamos el dataframe df_habitantes con los habitantes agregados por: año del censo en el que fueron registrados, 
# ID de la provincia en la que residen, condición de Cobertura Social, grupo etario y su sexo.
# Para ello, unimos cuatro tablas mediante un UNION ALL, cada tabla tiene las mismas categorias ya mencionadas pero están 
# separadas por el sexo (pasamos de tener dos columnas 'Mujer', 'Varon', a tener una sola 'Sexo', como esta ilustrado en
# el modelo relacional) y el año del censo en el que aparecen, información proveniente de las tablas creadas por los archivos
# "censo2010.xlsX" y "censo2022.xlsX".
# La columna de cobertura social se clasifica en: 'Privada' para quienes tienen obra social o prepaga,
# 'Pública' para quienes reciben programas estatales y  'No tiene cobertura social' para los demás casos.
# Además, realizamos un INNER JOIN con `df_provincias` para asignar el ID de la provincia, comparando los nombres de la provincia 
# en ambas tablas en minúscula y sin espacios.

consultaSQL = """
                SELECT 
                    2010 AS "Año del censo", p.ID AS "Provincia ID", 
                    CASE 
                        WHEN c.Cobertura LIKE 'Obra%'  OR c.Cobertura LIKE 'Prepaga%'THEN 'Privada'
                        WHEN c.Cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.Cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS Cobertura, 
                    c."Rango etario" AS "Grupo etario", 
                    'F' AS Sexo,
                    c.Mujer AS Cantidad
                FROM df_censo_2010 AS c
                INNER JOIN df_provincias AS p
                ON LOWER(TRIM(c.provincia)) = LOWER(TRIM(p.nombre))
                
                UNION ALL
                
                SELECT 
                    2010 AS "Año del censo", p.ID AS "Provincia ID", 
                    CASE 
                        WHEN c.Cobertura LIKE 'Obra%'  OR c.Cobertura LIKE 'Prepaga%'THEN 'Privada'
                        WHEN c.Cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.Cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS "Cobertura Social", 
                    c."Rango etario" AS "Grupo etario", 
                    'M' AS Sexo,
                    c.Varón AS Cantidad
                FROM df_censo_2010 AS c
                INNER JOIN df_provincias AS p
                ON LOWER(TRIM(c.provincia)) = LOWER(TRIM(p.nombre))
                
                UNION ALL
                
                SELECT 
                    2022 AS "Año del censo", p.ID AS "Provincia ID", 
                    CASE 
                        WHEN c.Cobertura LIKE 'Obra%' THEN 'Privada'
                        WHEN c.Cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.Cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS Cobertura_Social, 
                    c."Rango etario" AS "Grupo etario", 
                    'F' AS Sexo,
                    c.Mujer AS Cantidad
                FROM df_censo_2022 AS c
                INNER JOIN df_provincias AS p
                ON LOWER(TRIM(c.provincia)) = LOWER(TRIM(p.nombre))
                
                UNION ALL
                
                SELECT 
                    2022 AS "Año del censo", p.ID AS "Provincia ID", 
                    CASE 
                        WHEN c.Cobertura LIKE 'Obra%' THEN 'Privada'
                        WHEN c.Cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.Cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS Cobertura_Social, 
                    c."Rango etario" AS "Grupo etario", 
                    'M' AS Sexo,
                    c.Varón AS Cantidad
                FROM df_censo_2022 AS c
                INNER JOIN df_provincias AS p
                ON LOWER(TRIM(c.provincia)) = LOWER(TRIM(p.nombre));
            """

df_habitantes = dd.sql(consultaSQL).df()
df_habitantes.to_csv("df_habitantes.csv", index = False)

#%% Tabla de establecimientos_medicos

# Creamos el dataframe df_establecimientos_medicos con los establecimientos agregados por: ID del establecimiento, 
# su nombre correspondiente, el ID del departamento al que pertenecen y su origen de financiamiento, con los datos
# generados por la tabla de "instituciones_de_salud.xlsx".
# Además, accedemos a la columna "Nombre" para capitalizar el nombre de los establecimientos asegurando consistencia con el 
# formato del dataframe de provincias y de departamentos.

consultaSQL = """
                SELECT DISTINCT 
                    establecimiento_id AS ID, establecimiento_nombre AS Nombre, 
                    departamento_id AS "Departamento ID", origen_financiamiento AS Financiamiento,
                    CASE 
                        WHEN LOWER(tipologia_nombre) LIKE '%terapia intensiva%' THEN 'Sí'
                        ELSE 'No'
                        END AS "Terapia intensiva"
                FROM instituciones_de_salud;
            """

df_establecimientos_medicos = dd.sql(consultaSQL).df()
df_establecimientos_medicos["Nombre"] = df_establecimientos_medicos["Nombre"].str.title()
df_establecimientos_medicos.to_csv("df_establecimientos_medicos.csv", index = False)

#%% Tabla provincia_x_habitantes

# Tabla provincia_x_habitantes igual a la tabla habitantes solo que en vez del ID de la provincia tiene el nombre.
unir_provincias_habitantes = """
                SELECT p.Nombre AS "Provincia", h.Sexo, h."Año del censo", h.Cantidad,
                h."Grupo etario", h.Cobertura
                FROM df_provincias AS p
                INNER JOIN df_habitantes AS h
                ON p.ID == h."Provincia ID";
              """

provincia_x_habitantes = dd.query(unir_provincias_habitantes).df()

#%% REPORTE i

# Vamos a usar la tabla provincia_x_habitantes pues nos piden el nombre y no el ID de las provincias.
# Seleccionamos las columnas de la tabla que necesitamos.
# Luego armamos las que nos faltan:
# Primero armamos las columnas del 2010:
# donde separamos entre habitantes con o sin cobertura y vamos sumando los valores de la columna Cantidad según corresponda.
# Luego hacemos lo mismo con los datos del censo de 2022.

# i)
consulta_i = """ 
                SELECT Provincia, "Grupo etario", 
                       -- Columnas para el año 2010
                       SUM(CASE WHEN "Año del censo" = 2010 AND (Cobertura = 'Privada' OR Cobertura = 'Pública') 
                           THEN cantidad ELSE 0 END) 
                           AS "Habitantes con cobertura en 2010",
                       SUM(CASE WHEN "Año del censo" = 2010 AND Cobertura = 'No tiene cobertura social' 
                           THEN cantidad ELSE 0 END) 
                           AS "Habitantes sin cobertura en 2010",
                       
                       -- Columnas para el año 2022
                       SUM(CASE WHEN "Año del censo" = 2022 AND (Cobertura = 'Privada' OR Cobertura = 'Pública')
                           THEN cantidad ELSE 0 END) 
                           AS "Habitantes con cobertura en 2022",
                       SUM(CASE WHEN "Año del censo" = 2022 AND Cobertura = 'No tiene cobertura social' 
                           THEN cantidad ELSE 0 END) 
                           AS "Habitantes sin cobertura en 2022"
                FROM provincia_x_habitantes
                GROUP BY Provincia, "Grupo etario"
                ORDER BY Provincia, "Grupo etario";
            """

reporte_i = dd.query(consulta_i).df()

#%% REPORTE ii

# Vamos a usar las tablas df_departamentos, df_provincias y df_establecimientos_medicos.
# De df_provincia nos qudamos con los nombres de las provincias.
# De df_establecimientos_medicos con el tipo de financiación.
# Luego para cada provincia y tipo de financiación, contamos cuántos establecimientos médicos hay que además tengan terapia intensiva.
# Podemos ver en que provincia esta cada establecimientos médico pues en df_establecimientos_medicos contamos con el ID del departamento
# en el que se encuentra este, al unirlo con la tabla df_departamentos , para cada departamento tenemos el id de la provincia y luego
# lo unimos a la tabla df_provincias.

# ii)
consulta_ii = """
                SELECT p."Nombre",
                CASE 
                    WHEN e."Financiamiento" LIKE 'Privad%' THEN 'Privado'
                    ELSE 'Estatal'
                    END AS "Financiamiento",
                SUM(CASE WHEN e."Terapia intensiva" = 'Sí' THEN 1 ELSE 0 END) AS "Cantidad de establecimientos"
                FROM df_establecimientos_medicos AS e
                INNER JOIN df_departamentos AS d
                    ON e."Departamento ID" = d."Departamento ID"
                INNER JOIN df_provincias AS p
                    ON p."ID" = d."Provincia ID"
                GROUP BY p."Nombre", 
                CASE 
                    WHEN e."Financiamiento" LIKE 'Privad%' THEN 'Privado'
                    ELSE 'Estatal'
                    END
                ORDER BY p."Nombre", "Financiamiento";
            """


reporte_ii = dd.query(consulta_ii).df()

#%% REPORTE iii

# Creamos una tabla temporal, frecuencia_defunciones, y se utiliza ROW_NUMBER para enumerar las filas,
# el cual asigna un número único a cada fila dentro de un grupo especifico.
# Dentro del ROW_NUMBER hay un OVER que se puede dividir en dos partes:
# primero un PARTITION BY que hace que se reinicie el contador cada vez que se cambia de sexo o grupo etario
# despues hay un ORDER BY que va ordenando, dentro de las particiones, las causas de muerte de mayor a menor
# Esto se usa dos veces, una para las defunciones mas frecunetes y otra para las menos.

# iii)
consulta_iii = """
                WITH frecuencia_defunciones AS
                    (SELECT "Sexo", "Grupo etario", "Descripción", 
                            SUM("Cantidad") as "Total de muertes",
                            ROW_NUMBER() OVER(PARTITION BY "Sexo", "Grupo etario" 
                                              ORDER BY SUM("Cantidad") DESC) as mas_frecuentes,
                            ROW_NUMBER() OVER(PARTITION BY "Sexo", "Grupo etario" 
                                              ORDER BY SUM("Cantidad") ASC) as menos_frecuentes
                     FROM df_defunciones
                     GROUP BY "Sexo", "Grupo etario", "Descripción")

                SELECT "Sexo", "Grupo etario", "Descripción", 
                       "Total de muertes",
                       CASE 
                           WHEN mas_frecuentes <= 5 THEN '5 más frecuentes'
                           WHEN menos_frecuentes <= 5 THEN '5 menos frecuentes'
                       END AS Frecuencia
                FROM frecuencia_defunciones
                WHERE mas_frecuentes <= 5 OR menos_frecuentes <= 5
                ORDER BY 
                    CASE WHEN "Grupo etario" = '0-14' THEN 1
                         WHEN "Grupo etario" = '15-34' THEN 2
                         WHEN "Grupo etario" = '35-54' THEN 3
                         WHEN "Grupo etario" = '55-74' THEN 4
                         ELSE 5 END, 
                    "Sexo" ASC, "Total de muertes" DESC;
            """

reporte_iii = dd.query(consulta_iii).df()

#%% REPORTE iv

# iv)
# Calculamos la tasa de mortalidad por provincia y grupo etario para el año 2022, normalizada cada 1000 habitantes
# Primero definimos dos tablas temporales:
# defunciones_agrupadas: agrupa el total de muertes por jurisdicción y edad (para el año 2022).
# habitantes_agrupados: obtiene la población total a partir del censo correspondiente.
# Luego, realizamos un INNER JOIN entre ambas tablas utilizando una clave compuesta (Provincia ID y Grupo etario),
# asegurando que cada total de muertes se divida por su población correspondiente.
# Por ultimo, integramos la tabla df_provincias para reemplazar los IDs por nombres legibles.
# El cálculo final (muertes * 1000 / población) permite comparar la mortalidad entre provincias con poblaciones de tamaños muy diferentes.

consulta_iv = """
                WITH defunciones_agrupadas AS 
                    (SELECT "Provincia ID", "Grupo etario", 
                            SUM(Cantidad) AS "Muertes totales"
                     FROM df_defunciones
                     WHERE año = 2022
                     GROUP BY "Provincia ID", "Grupo etario"),
       
                habitantes_agrupados AS
                    (SELECT "Provincia ID", "Grupo etario", 
                            SUM(Cantidad) AS "Población total"
                     FROM df_habitantes
                     WHERE "Año del censo" = 2022
                     GROUP BY "Provincia ID", "Grupo etario")
        
                SELECT p.Nombre AS Provincia, 
                       h."Grupo etario", 
                       (d."Muertes totales" * 1000.0 / h."Población total") 
                           AS "Tasa de mortalidad"
                FROM habitantes_agrupados h
                INNER JOIN defunciones_agrupadas d 
                    ON h."Provincia ID" = d."Provincia ID" 
                    AND h."Grupo etario" = d."Grupo etario"
                INNER JOIN df_provincias p 
                    ON h."Provincia ID" = p.ID
                ORDER BY p.Nombre, h."Grupo etario";
            """


reporte_iv = dd.query(consulta_iv).df()

#%% REPORTE v

# Usamos la tabla df_defunciones, de la cual nos vamos a guardar la columna Descripcion.
# Para cada enfermedad, al valor de la columna Cantidad cuando el valor de la columna Año es 2022, le restamos el valor de Cantidad para el Año 2010.
# Esta información se guarda en una columna nueva llamda Diferencia de defunciones.

# v)
consulta_v = """
                SELECT Descripción,
                       SUM(CASE WHEN Año = 2022 THEN cantidad ELSE 0 END) - 
                       SUM(CASE WHEN Año = 2010 THEN cantidad ELSE 0 END) 
                           AS "Diferencia de defunciones"
                FROM df_defunciones
                GROUP BY Descripción
                ORDER BY "Diferencia de defunciones" DESC;
            """


reporte_v = dd.query(consulta_v).df()

#%% Tabla provincias cortas

#Tabla igual a la de provincias, pero con ciertos nombres más cortos para mejorar legibilidad en los gráficos.

df_provincias_copia = df_provincias.copy()
df_provincias_copia.loc[0,"Nombre"] = 'CABA'
df_provincias_copia.loc[23,"Nombre"] = 'Tierra del Fuego'

#%% GRAFICO i

# Generamos un dataframe para sacar el total de habitantes por provincia y año de censo.
# y usamos CASE WHEN para acortar los nombres de Tierra del Fuego y CABA así no se amontonan en el gráfico.

consulta_totales = """
                SELECT 
                    "Año del censo",
                    CASE
                        WHEN "Provincia" = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
                            THEN 'Tierra del Fuego'
                        WHEN "Provincia" = 'Ciudad Autónoma de Buenos Aires'
                            THEN 'CABA'
                        ELSE "Provincia"
                        END AS "Provincia",
                    SUM(Cantidad) AS "Total habitantes" 
                FROM provincia_x_habitantes
                GROUP BY 
                    "Año del censo",
                    CASE
                        WHEN "Provincia" = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
                            THEN 'Tierra del Fuego'
                        WHEN "Provincia" = 'Ciudad Autónoma de Buenos Aires'
                            THEN 'CABA'
                        ELSE "Provincia"
                        END
                ORDER BY "Provincia", "Año del censo";
            """

totales = dd.query(consulta_totales).df()

fig, ax = plt.subplots(figsize=(12,9)) 

# Filtramos los datos en dos dataframe para separar las barras del 2010 y 2022.
df_2010 = totales[totales["Año del censo"] == 2010]
df_2022 = totales[totales["Año del censo"] == 2022]

# Definimos la posición de las barras en el eje x y extreamos los valores de población total.
x = np.arange(len(df_2010))
A_2010 = df_2010["Total habitantes"]
A_2022 = df_2022["Total habitantes"]

# Definimos el ancho de las barras, desplazadas para que queden una al lado de la otra.
width = 0.4
ax.bar(x - width/2, A_2010, width=width, label='Año 2010', color = '#08519c')
ax.bar(x + width/2, A_2022, width=width, label='Año 2022', color = '#9ecae1')

# Agregamos títulos, etiquetas y rotamos los nombres de las provincias para que se lean bien.
ax.set_title('Cantidad de habitantes por provincia')
ax.set_xlabel('Provincias')
ax.set_ylabel('Habitantes')
ax.set_xticks(x)

# Ajustamos las provincias en eje x.
ax.set_xticklabels(df_2010["Provincia"], rotation=60, ha="right")

ax.legend()
fig.savefig('Cantidad de habitantes por provincia.png',bbox_inches = 'tight')
plt.show()

#%% GRAFICO ii versión 1

defunciones_por_anio = """
                SELECT Descripción, Año, SUM(cantidad) AS Cantidad
                FROM df_defunciones
                GROUP BY Año, Descripción
                ORDER BY Descripción, Año;
            """

dpa = dd.query(defunciones_por_anio).df()

# Primero identificamos las 10 categorías con mayor cantidad de muertes totales.

# Agrupa por descripcion y suma las cantidades.
agrupar = dpa.groupby('Descripción')['Cantidad'].sum()

# Ordena de mayor a menor.
ordenado = agrupar.sort_values(ascending=False)

# Toma los primeros 10.
primeros_10 = ordenado.head(10)

# Pasamos los primeros 10 a una lista.
diez_mayores_categorias = primeros_10.index.tolist()


# Segundo separamos en dos: las 10 primeras y el resto
df_10_primeras = dpa[dpa['Descripción'].isin(diez_mayores_categorias)]
df_otras = dpa[~dpa['Descripción'].isin(diez_mayores_categorias)]

# Tercero agrupamos "las otras causas" por año para tener solo una línea de "Otras causas".
otras_agrupadas = df_otras.groupby('Año')['Cantidad'].sum().reset_index()
otras_agrupadas['Descripción'] = 'Otras causas'

fig, ax = plt.subplots()

# Graficamos cada una de las 10 principales
for categoria in diez_mayores_categorias:
    subset = df_10_primeras[df_10_primeras['Descripción'] == categoria]
    ax.plot(subset['Año'], subset['Cantidad'], marker='o', label=categoria)

# Graficamos la línea "otras" con diferente estilo para diferenciar.
ax.plot(otras_agrupadas['Año'], otras_agrupadas['Cantidad'], 
        marker='s', linestyle='--', color='gray', label='Otras categorías')

ax.set_title('Defunciones por categoría')
ax.set_xlabel('Año')
ax.set_ylabel('Cantidad de defunciones')

# Como los años son muy largos y quedarian amontonados, optamos por tomar los ultimos dos dígitos.

# Lista de años del 2005 al 2022 inclusive
anios_completos = list(range(2005, 2023))

# Nos salteamos los primeros dos digitos del año
anios_cortos = [str(anio)[2:] for anio in anios_completos]

ax.set_xticks(anios_completos)
ax.set_xticklabels(anios_cortos)

ax.legend(title="Categorías", bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()
fig.savefig('Defunciones por categoríavs1.png',bbox_inches = 'tight')
plt.show()

#%% GRAFICO ii versión 2

defunciones_por_anio = """
                SELECT Descripción, Año, SUM(cantidad) AS Cantidad
                FROM df_defunciones
                GROUP BY Año, Descripción
                ORDER BY Descripción, Año;
            """

dpa = dd.query(defunciones_por_anio).df()

# Primero identificamos las categorías con mayor cantidad de muertes totales.

# Agrupa por descripción y suma las cantidades.
agrupar = dpa.groupby('Descripción')['Cantidad'].sum()

# Ordena de mayor a menor.
ordenar = agrupar.sort_values(ascending=False)

# Segundo creamos una lista del Top 5 sin contar covid todavía para no duplicarlo.
top_5_general = ordenar.head(5).index.tolist()

# Tercero armamos los grupos finales para cada gráfico.
# Optamos por usar un conjunto (set) para asegurar que COVID aparezca en el primer gráfico.
grupo_principal = list(set(top_5_general + ['COVID-19']))

# El resto son todas las categorías que no estan en el grupo_principal.
resto_categorias = [c for c in ordenar.index if c not in grupo_principal]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(22, 9))

# Como los años son muy largos y quedarian amontonados, optamos por tomar los ultimos dos dígitos.

# Lista de años del 2005 al 2022 inclusive.
anios_completos = list(range(2005, 2023))

# Nos salteamos los primeros dos dígitos del año.
anios_cortos = [str(anio)[2:] for anio in anios_completos]

# Subplot 1, categorías de mayor impacto + covid.
for categoria in grupo_principal:
    subset = dpa[dpa['Descripción'] == categoria]
    ax1.plot(subset['Año'], subset['Cantidad'], marker='o', linewidth=2.5, label=categoria)

ax1.set_title('Defunciones por categoria principales', fontsize=14, fontweight='bold')
ax1.set_ylabel('Cantidad de defunciones')
ax1.set_xlabel('Año')
ax1.set_xticks(anios_completos)
ax1.set_xticklabels(anios_cortos)
ax1.legend(title="Principales", bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=2)
ax1.grid(True, linestyle=':', alpha=0.6)

# subplot 2, otras categorías.
for categoria in resto_categorias:
    subset = dpa[dpa['Descripción'] == categoria]
    ax2.plot(subset['Año'], subset['Cantidad'], marker='.', linewidth=1.5, label=categoria)

ax2.set_title('Defunciones por categoria secundarias', fontsize=14, fontweight='bold')
ax2.set_ylabel('Cantidad de defunciones')
ax2.set_xlabel('Año')
ax2.set_xticks(anios_completos)
ax2.set_xticklabels(anios_cortos)
ax2.legend(title="Otras causas", bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
ax2.grid(True, linestyle=':', alpha=0.6)

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
fig.savefig('Defunciones por categoríavs2.png',bbox_inches = 'tight')
plt.show()

#%% GRAFICO iii

# Consulta para la tasa total.
consulta_tasa_total = """
                WITH muertes_prov AS
                    (SELECT "Provincia ID", SUM(Cantidad) AS muertes
                     FROM df_defunciones
                     WHERE año = 2022
                     GROUP BY "Provincia ID"),

                poblacion_prov AS
                    (SELECT "Provincia ID", SUM(Cantidad) AS poblacion
                     FROM df_habitantes
                     WHERE "Año del censo" = 2022
                     GROUP BY "Provincia ID")

                SELECT
                (m.muertes * 1000.0 / pbl.poblacion) AS tasa_mortalidad_total, -- Muertes cada mil hab
                p."Nombre" AS provincia
                FROM muertes_prov AS m
                JOIN poblacion_prov AS pbl
                    ON m."Provincia ID" = pbl."Provincia ID"
                JOIN df_provincias_copia AS p
                    ON m."Provincia ID" = p.ID
                ORDER BY tasa_mortalidad_total ASC;
            """

# Consulta por edad.
consulta_edades = """
                WITH muertes_prov_edad AS -- defuncioens en 2022 separadas x prov y grupo et
                    (SELECT "Provincia ID", "Grupo etario", SUM(Cantidad) AS muertes
                    FROM df_defunciones
                    WHERE año = 2022
                    GROUP BY "Provincia ID", "Grupo etario"),
                    
                poblacion_prov AS
                     (SELECT "Provincia ID", SUM(Cantidad) AS poblacion
                     FROM df_habitantes
                     WHERE "Año del censo" = 2022
                     GROUP BY "Provincia ID")
                    
                SELECT m."Grupo etario", (m.muertes * 1000.0 / pbl.poblacion) AS tasa_mortalidad,
                       p."Nombre" AS provincia
                FROM muertes_prov_edad AS m
                JOIN poblacion_prov AS pbl
                    ON m."Provincia ID" = pbl."Provincia ID"
                JOIN df_provincias_copia AS p
                    ON m."Provincia ID" = p.ID
            """

df_tasa_total = dd.query(consulta_tasa_total).df() # Tasa de mortalidad total
df_edades = dd.query(consulta_edades).df() # Tasa de mortalidad total separada por edades

# Grafico con 2 subplots.
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

# Gráfico 1
# Convertimos a lista.
provincias = df_tasa_total["provincia"].tolist()
tasas = df_tasa_total["tasa_mortalidad_total"].tolist()

ax1.bar(provincias, tasas, color='steelblue', edgecolor='black', alpha=0.8)
ax1.set_title('Mortalidad total por Provincia', fontsize=14, fontweight='bold')
ax1.set_ylabel('Muertes cada 1000 habitantes')
ax1.set_xticklabels(provincias, rotation=60, ha='right')

# Gráfico 2
# Definimos los grupos etarios ordenados.
grupos_etarios = ['0-14', '15-34', '35-54', '55-74', '75+']
piso = np.zeros(len(provincias))
# Esto sirve para que no se sobrepongan las barras por sobre la otra,
# que cada una empiece por arriba del rango etario anterior.

# Iteramos por cada grupo etario.
for grupo in grupos_etarios:

    valores_grupo = []
    for p in provincias:
        # Buscamos el valor en el df para esa provincia y ese grupo.
        dato = df_edades[(df_edades['provincia'] == p) & (df_edades['Grupo etario'] == grupo)]
        
        if not dato.empty:
            valores_grupo.append(dato['tasa_mortalidad'].values[0])
        else:
            valores_grupo.append(0)
    
    # Convertimos la lista a un array con numpy para poder sumar mas facil.
    valores_array = np.array(valores_grupo)
    
    ax2.bar(provincias, valores_array, bottom=piso, label=grupo, edgecolor='white', linewidth=0.5)
    
    # Actualizamos el piso.
    piso += valores_array

ax2.set_title('Mortalidad total por provincia y grupo etario', fontsize=14, fontweight='bold')
ax2.set_ylabel('Mortalidad por edad')
ax2.set_xticklabels(provincias, rotation=60, ha='right')

plt.tight_layout()

fig.savefig('Mortalidad total por provincia y grupo etario.png',bbox_inches = 'tight')
plt.show()

#%% GRAFICO iv

# Ordenamos la tabla df_defunciones para que las claves sean solo Sexo y Grupo Etario, el resto de claves las eliminamos y sumamos las cantidades.
consulta_g_iv = """
                SELECT "Grupo etario" AS GE, Sexo, SUM(cantidad) AS Cantidad
                FROM df_defunciones
                GROUP BY Sexo, "Grupo etario"
                ORDER BY "Grupo etario", Sexo
            """;        
defunciones_por_ge_y_por_sexo = dd.query(consulta_g_iv).df()

# Separamos la columna "Sexo" en 2 columnas distintas ("Femenino" y "Masculino"), en cada celda de estas columnas se 
# muestra la cantidad de Gente de ese Sexo del GE dedo por la columna de "Grupo etario".
# Usamos la tabla defunciones_por_ge_sexo.
# Generamos una tabla que tiene cada grupo etario.
# Armamos otra tabla f que tiene para cada grupo etario la cantidad de defunciones de sexo femenino.
# Unimos la tabla a la que solo tiene grupos etarios, y a la nueva columna la llamamos Femenino.
# Hacemos lo analogo para el sexo masculino.
# Luego ordenamos por grupo etario. 
consulta_g_iv2 = """
    SELECT ge.GE AS 'Grupo etario', f.cantidad AS Femenino, m.cantidad AS Masculino,
    FROM
    (SELECT DISTINCT GE FROM defunciones_por_ge_y_por_sexo) AS ge
    JOIN
        (SELECT GE, cantidad FROM defunciones_por_ge_y_por_sexo WHERE sexo = 'F') AS f
    ON ge.GE = f.GE
    JOIN
        (SELECT GE, cantidad FROM defunciones_por_ge_y_por_sexo WHERE sexo = 'M') m
    ON ge.GE = m.GE
    ORDER BY ge.GE
    """
defunciones_x_ge_sexo = dd.query(consulta_g_iv2).df()

# Borramos la tabla "defunciones_por_ge_y_por_sexo"  porque ya no la vamos a usar.
del defunciones_por_ge_y_por_sexo

# Calculamos la cantidad de habitantes por grupo etario desde la tabla habitantes.
consulta_g_iv3 = """
    SELECT "Grupo etario",
        SUM(Cantidad) As 'Poblacion Total'
        FROM df_habitantes
        GROUP BY "Grupo etario"
        ORDER BY "Grupo etario"
        """

total_hab_x_ge = dd.query(consulta_g_iv3).df()

# Normalizamos la cantidad de defunciones por grupo etario cada 100.000 habitantes.
consulta_g_iv4 = """
    SELECT d."Grupo etario",
            (d.Femenino / h."Poblacion Total")*100000 AS 'Tasa Femenina',
            (d.Masculino / h."Poblacion Total")*100000 AS 'Tasa Masculina'
    FROM defunciones_x_ge_sexo AS d
    JOIN
        total_hab_x_ge AS h ON d."Grupo etario" = h."Grupo etario"
    ORDER BY d."Grupo etario"
    """
defunciones_x_ge_sexo_normalizadas = dd.query(consulta_g_iv4).df()    

# Borramos las tablas "defunciones_x_ge_sexo" y "total_hab_x_ge" porque ya no las vamos a usar.
del defunciones_x_ge_sexo
del total_hab_x_ge

# Armamos el gráfico de barras.
fig, ax = plt.subplots()

# Guardamos las etiquetas del eje x (grupos etarios)
labels = defunciones_x_ge_sexo_normalizadas['Grupo etario']

# Armamos un arreglo con las posiciones para saber dónde estan las barras
# hay una posición por grupo etario.
x = np.arange(len(labels))
# Guardamos las cantidades de defunciones de cada sexo.
f = defunciones_x_ge_sexo_normalizadas['Tasa Femenina']
m = defunciones_x_ge_sexo_normalizadas['Tasa Masculina']

width = 0.25

# Dibujamos las barras para cada sexo desplazándolas horizontalmente para que no se superpongan dentro de cada grupo etario.
ax.bar(x - width/2, f, width=width, label='Mujeres')
ax.bar(x + width/2, m, width=width, label='Hombres')

# Agregamos el titulo.
ax.set_title('Defunciones por Grupo Etario y Sexo normalizadas \n (cada 100000 habitantes del mismo grupo etario)')
# Nombramos el eje X.
ax.set_xlabel('Grupo Etario')
# Indicamos las posiciones y agregamos las etiquetas (grupos etarios) del eje x.
ax.set_xticks(x)
ax.set_xticklabels(labels)
# Nombramos el eje Y.
ax.set_ylabel('Defunciones cada 100000 habitantes \n del mismo grupo etario')
# Mostramos la leyenda para distinguir cada sexo.
ax.legend()
fig.savefig('Defunciones por Grupo Etario y Sexo normalizadas.png',bbox_inches = 'tight')
plt.show()

#%% GRAFICO v

# Generamos el dataframe para contar cuántos centros de salud hay en cada departamento.
# Además, unimos con la tabla de df_provincias_copia para tener el nombre de las provincias relacionado.

consulta_establecimientos_x_departamento = """
                WITH establecimientos_x_departamentos AS
                    (SELECT "Departamento ID", COUNT(*) AS "Total establecimientos"
                     FROM df_establecimientos_medicos
                     GROUP BY "Departamento ID")
                SELECT p."Nombre" AS "Provincia", d."Nombre" AS "Departamento", e."Total establecimientos", p."ID"
                FROM df_provincias_copia AS p
                INNER JOIN df_departamentos AS d
                ON d."Provincia ID" = p."ID"
                INNER JOIN establecimientos_x_departamentos AS e
                ON e."Departamento ID" = d."Departamento ID"
                ORDER BY "Provincia", "Departamento";
            """

establecimientos_x_departamento = dd.query(consulta_establecimientos_x_departamento).df()

fig, ax = plt.subplots(figsize=(12,8))

# Creamos un boxplot para ver la distribución de establecimientos por provincia. 
# El parámetro showmeans=True permite visualizar la media (promedio)
establecimientos_x_departamento.boxplot(by = ["Provincia"], column = ["Total establecimientos"],
                                         ax = ax, grid = False, showmeans = True)

# Eliminamos títulos automáticos generados por pandas.
fig.suptitle('')

# Agregamos títulos y etiquetas.
ax.set_title("Distribución de establecimientos de salud")
ax.set_xlabel("Provincias")
ax.set_ylabel("Total establecimientos por departamento")

# Ajustamos las provincias en eje x.
ax.set_xticklabels(df_provincias_copia["Nombre"], rotation=45, ha="right")

fig.savefig('Distribución de establecimientos de salud.png',bbox_inches = 'tight')
plt.show()

#%% GRAFICO vi

# Generamos un dataframe para unir defunciones, población y establecimientos.
# El objetivo es calcular la tasa de mortalidad cada 100.000 habitantes por provincia para el año 2022, 
# y compararla con la cantidad de establecimientos médicos normalizada por cada 100.000 habitantes
# que existen en esa provincia.

consulta_mortalidad_establecimientos = """
                
                WITH defunciones_2022 AS
                    (SELECT "Provincia ID", 
                         SUM("Cantidad") AS "Defunciones"
                    FROM df_defunciones
                    WHERE "Año" = 2022
                    GROUP BY "Provincia ID"),
                    
                poblacion_2022 AS
                    (SELECT "Provincia ID", 
                         SUM("Cantidad") AS "Poblacion"
                    FROM df_habitantes
                    WHERE "Año del censo" = 2022
                    GROUP BY "Provincia ID"),
                    
                establecimientos_x_provincias AS
                    (SELECT "ID", 
                         SUM("Total establecimientos") AS "Establecimientos"
                    FROM establecimientos_x_departamento
                    GROUP BY "ID")
        
                SELECT p.Nombre AS "Provincia", p.ID AS "ID", 
                    ep."Establecimientos" * 100000.0 / p2."Poblacion" AS "Establecimientos",
                    d."Defunciones" * 100000.0 / p2."Poblacion" AS "Defunciones_x_Provincia"
                FROM defunciones_2022 AS d
                INNER JOIN poblacion_2022 AS p2
                ON d."Provincia ID" = p2."Provincia ID"
                INNER JOIN df_provincias_copia AS p
                ON d."Provincia ID" = p."ID"
                INNER JOIN establecimientos_x_provincias AS ep
                ON p."ID" = ep."ID"
                ORDER BY "Provincia";
            """


df_consulta_mortalidad_establecimientos = dd.query(consulta_mortalidad_establecimientos).df()

fig, ax = plt.subplots(figsize=(10,4))

# Generamos una columna extra en nuestra tabla que permite agrupar el ID con el nombre de la provincia.
df_consulta_mortalidad_establecimientos["Leyenda"] = (
    df_consulta_mortalidad_establecimientos["ID"].astype(str) + " - " + 
    df_consulta_mortalidad_establecimientos["Provincia"]
)
# Gráfico de dispersión:
# Eje X → cantidad de establecimientos
# Eje Y → tasa de defunciones cada 100.000 habitantes
# Cada punto representa una provincia.
sns.scatterplot(
    data=df_consulta_mortalidad_establecimientos,
    x="Establecimientos",
    y="Defunciones_x_Provincia",
    hue="Leyenda",
    palette='husl',
    s=60,
    ax=ax
)
x = len(df_consulta_mortalidad_establecimientos)

# Agregamos el ID de provincia en el scatterplot a través de un for que itera sobre cada punto.
for i in range(x):
    ax.text(
        x=df_consulta_mortalidad_establecimientos["Establecimientos"].iloc[i] + 0.1, 
        y=df_consulta_mortalidad_establecimientos["Defunciones_x_Provincia"].iloc[i], 
        s=df_consulta_mortalidad_establecimientos["ID"].iloc[i],
        fontweight='bold',
        fontsize=4,
        alpha=0.7
    )

# Título y etiquetas de los ejes.
ax.set_title('Establecimientos de salud por defunciones por provincia en 2022 por cada 100.000 habitantes')
ax.set_xlabel('Establecimientos normalizados')
ax.set_ylabel('Tasa de defunciones por provincia')

# Ubicamos la leyenda fuera del gráfico para evitar superposición.
ax.legend(
    title="Referencias",
    bbox_to_anchor=(1.05, 1),
    loc='upper left',
    fontsize=4
)

plt.tight_layout()
fig.savefig('Establecimientos de salud por defunciones por provincia en 2022.png',bbox_inches = 'tight')
plt.show()


#%% Tablas auxiliares

# Tablas auxiliares para ver la consistencia y completitud de las tablas 'archivo_defunciones' y 'instituciones_de_salud'
# Generamos una tabla que muestra el porcentaje de registros que poseen información en la columna "Sexo"
# y el porcentaje de registros que no tienen.

consulta_sexo = """
                SELECT
                    100.0 * SUM(CASE WHEN Sexo IN ('femenino', 'masculino') THEN Cantidad ELSE 0 END) / SUM(Cantidad)
                    AS 'Con informacion',
                    100.0 * SUM(CASE WHEN Sexo NOT IN ('femenino','masculino') THEN Cantidad ELSE 0 END) / SUM(Cantidad)
                    AS 'Sin informacion'
                FROM archivo_defunciones;
            """
df_sexo = dd.query(consulta_sexo).df()

# Generamos una tabla que muestra el porcentaje de registros que poseen información en la columna "grupo_edad"
# y el porcentaje de registros que no tienen.
consulta_grupo_edad = """
                SELECT
                    100.0 * SUM(CASE WHEN grupo_edad NOT LIKE '06.%' THEN Cantidad ELSE 0 END) / SUM(Cantidad)
                    AS 'Con informacion',
                    100.0 * SUM(CASE WHEN grupo_edad LIKE '06.%' THEN Cantidad ELSE 0 END) / SUM(Cantidad)
                    AS 'Sin informacion'
                FROM archivo_defunciones;
            """
df_grupo_edad = dd.query(consulta_grupo_edad).df()

# Generamos una tabla que muestra el porcentaje de registros que poseen información en la columna "jurisdicion_residencia_nombre"
# y el porcentaje de registros que no tienen (valores nulos o 'Sin Información').
consulta_residencia_nombre = """
                SELECT
                    SUM(CASE WHEN jurisdicion_residencia_nombre IS NOT NULL AND jurisdicion_residencia_nombre <> 'Sin Información' 
                             THEN Cantidad ELSE 0 END) * 100.0 / SUM(Cantidad)
                    AS 'Con informacion',
                    SUM(CASE WHEN jurisdicion_residencia_nombre IS NULL OR jurisdicion_residencia_nombre = 'Sin Información'
                             THEN Cantidad ELSE 0 END) *100.0 / SUM(Cantidad)
                    AS 'Sin informacion'
                FROM archivo_defunciones;
            """
df_residencia_nombre = dd.query(consulta_residencia_nombre).df()

# Generamos una tabla verifica la consistencia entre el nombre del departamento, su departamento_id y la provincia a la que
# pertenecen dentro de la tabla instituciones_de_salud.
# Contamos el total de los registros agrupados por "nombre" y "provincia_id" y se cuenta cuantos IDs distintos tiene, llamando a 
# los registros que tienen mas de un ID, como duplicados.

consulta_departamentos_id = """
                WITH departamentos_unicos AS (
                    SELECT DISTINCT LOWER(TRIM(departamento_nombre)) AS nombre, provincia_id, 
                        COUNT(DISTINCT departamento_id) as conteo_ids
                    FROM instituciones_de_salud
                    GROUP BY LOWER(TRIM(departamento_nombre)), provincia_id),
                
                total AS (
                    SELECT COUNT (*) AS total_registrados
                    FROM departamentos_unicos),
                
                duplicados AS (
                    SELECT COUNT(*) AS cantidad 
                    FROM departamentos_unicos 
                    WHERE conteo_ids > 1)
                
                SELECT 
                    100.0 * (t.total_registrados - d.cantidad) / t.total_registrados AS porcentaje_consistente,
                    100.0 * d.cantidad / t.total_registrados AS porcentaje_inconsistente
                    FROM total AS t
                    CROSS JOIN duplicados AS d;
            """
df_departamentos_id = dd.query(consulta_departamentos_id).df()

# Generamos una tabla que muestra cuáles son los departamentos que presentan inconsistencias con el ID.
consulta_departamentos_cant = """
                WITH departamentos_unicos AS (
                    SELECT LOWER(TRIM(departamento_nombre)) AS nombre, provincia_id, 
                        COUNT(DISTINCT departamento_id) as conteo_ids
                    FROM instituciones_de_salud
                    GROUP BY LOWER(TRIM(departamento_nombre)), provincia_id)
                
                SELECT nombre, conteo_ids
                    FROM departamentos_unicos 
                    WHERE conteo_ids > 1;
            """

df_departamentos_cant = dd.query(consulta_departamentos_cant).df()