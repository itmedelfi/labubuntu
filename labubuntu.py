import pandas as pd
import duckdb as dd
import numpy as np
import matplotlib.pyplot as plt 

#%% DATASETS

# Carga de archivos
carpeta = ''

censo2010               = pd.read_excel(carpeta + "/censo2010.xlsX", skiprows=14)
censo2022               = pd.read_excel(carpeta + "/censo2022.xlsX", skiprows=14)
archivo_defunciones     = pd.read_csv(carpeta + "/defunciones.csv")
instituciones_de_salud  = pd.read_excel(carpeta + "/instituciones_de_salud.xlsx")
categoriasDefunciones   = pd.read_csv(carpeta + "/categoriasDefunciones.csv")

#%% CENSO 2010

censo2010.columns = ['vacia', 'col_cobertura', 'col_edad', 'col_varon', 'col_mujer', 'col_total']

# Estructuras temporales para manejar datos de rango etario, provincia, cobertura y sexo
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
    # Evita procesar datos demas, marca el fin de los datos utiles
    
    if val_cobertura == "Total":
        cobertura_actual = "nan" 
    if val_cobertura != "nan" and val_cobertura != "Total":
        cobertura_actual = val_cobertura
    # Actualizaciones de estado de cobertura_actual

    if val_edad.isdigit() and cobertura_actual != "nan" :
        edad_nro = int(val_edad)
        if edad_nro <= 14: rango = '0-14'
        elif edad_nro <= 34: rango = '15-34'
        elif edad_nro <= 54: rango = '35-54'
        elif edad_nro <= 74: rango = '55-74'
        else: rango = '75+'
    # Agrupacion de los rangos etarios y limpieza de valores nulos
        
        mujeres = float(fila['col_mujer']) if str(fila['col_mujer']) != "-" else 0
        varones = float(fila['col_varon']) if str(fila['col_varon']) != "-" else 0

        clave = (provincia_actual, cobertura_actual, rango)

        if clave not in suma_total:
            suma_total[clave] = {'mujer': 0, 'varon': 0}
        
        suma_total[clave]['mujer'] += mujeres
        suma_total[clave]['varon'] += varones
        # Suma de habitantes separados por sexo al diccionario

# Conversión del diccionario completo a un dataframe
lista_final = []
for clave, valor in suma_total.items():
    fila = {'Provincia': clave[0],
            'Cobertura': clave[1],
            'Rango etario': clave[2],
            'Mujer': valor['mujer'],
            'Varón': valor['varon']}
    lista_final.append(fila)

df_censo_2010 = pd.DataFrame(lista_final)

#%% CENSO 2022

# Mismos procesos que con censo 2010

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

#%% Tabla de defunciones

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
                    GROUP BY c.categorias, d.grupo_edad, d.Sexo, d.anio, d.jurisdiccion_de_residencia_id;
            """

df_defunciones = dd.sql(consultaSQL).df()
df_defunciones.to_csv("df_defunciones.csv", index = False)
#hay sexos como None
#tambien hay grupo_etarios como None

#%% Tabla de provincias

consultaSQL = """
                SELECT DISTINCT jurisdiccion_de_residencia_id AS ID, jurisdicion_residencia_nombre AS Nombre
                FROM archivo_defunciones
                ORDER BY id;
              """
            
df_provincias = dd.sql(consultaSQL).df()
df_provincias.to_csv("df_provincias.csv", index = False)

#aca aun tenemos que ver que hacemos con los nan y los sin informacion
#ademas, cambie a que todos los nombres de provincias y departamentos tuvieran el nombre en minuscula para que haya continuidad
#en todas las tablas (tambien lo cambie cuando se hace el df)
#esta bien que todo sea 'provincia o provincia_id' cuando son jurisdicciones?

#%% Tabla de departamentos

consultaSQL = """
                SELECT DISTINCT departamento_id AS "Departamento ID", departamento_nombre AS Nombre, provincia_id AS "Provincia ID"
                FROM instituciones_de_salud;
              """
            
df_departamentos = dd.sql(consultaSQL).df()

df_departamentos["Nombre"] = df_departamentos["Nombre"].str.title()
    
df_departamentos.to_csv("df_departamentos.csv", index = False)
#COMUNA01 tiene dos ID's distintos pero vienen de la misma provincia


#%% Tabla de habitantes 

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

#Aca pase las columnas mujer y varon a una sola columna como sexo, si no, hay que cambiar el DER me parece, tambien use UNION ALL 
#que creo que no lo vimos en clase
#esta tabla funciona bajo el supuesto de que en df_censo_2010/2022 cambie el nombre de CABA, tierra del fuego y rio negro
df_habitantes = dd.sql(consultaSQL).df()
df_habitantes.to_csv("df_habitantes.csv", index = False)

#%% Tabla de establecimientos_medicos

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

#la tabla provincia_x_habitantes es igual a la tabla habitantes solo que en vez del id de la provincia tiene el nombre
unir_provincias_habitantes = """
                SELECT p.Nombre AS "Nombre de la provincia", h.Sexo, h."Año del censo", h.Cantidad,
                h."Grupo etario", h.Cobertura
                FROM df_provincias AS p
                INNER JOIN df_habitantes AS h
                ON p.ID == h."Provincia ID;"
              """

provincia_x_habitantes = dd.query(unir_provincias_habitantes).df()

#%% REPORTE i

# i)
consulta_i = """ 
                SELECT "Provincia ID" AS Provincia, "Grupo etario", 
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
                FROM df_habitantes
                GROUP BY Provincia, "Grupo etario"
                ORDER BY Provincia, "Grupo etario;"
            """


reporte_i = dd.sql(consulta_i).df()

#%% REPORTE ii

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
                ORDER BY p."Nombre", "Financiamiento;"
            """


reporte_ii = dd.sql(consulta_ii).df()

#%% REPORTE iii

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
                     WHERE "Sexo" IS NOT NULL AND "Grupo etario" IS NOT NULL
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


reporte_iii = dd.sql(consulta_iii).df()

#%% REPORTE iv

# iv)
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
                ORDER BY p.Nombre, h."Grupo etario;"
            """


reporte_iv = dd.sql(consulta_iv).df()

#%% REPORTE v

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

#%% GRAFICO i

consulta_totales = """
                SELECT 
                    "Año del censo",
                    CASE
                        WHEN "Nombre de la provincia" = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
                            THEN 'Tierra del Fuego'
                        WHEN "Nombre de la provincia" = 'Ciudad Autónoma de Buenos Aires'
                            THEN 'CABA'
                        ELSE "Nombre de la provincia"
                        END AS "Nombre de la provincia",
                    SUM(Cantidad) AS "Total habitantes" 
                FROM provincia_x_habitantes
                GROUP BY 
                    "Año del censo",
                    CASE
                        WHEN "Nombre de la provincia" = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
                            THEN 'Tierra del Fuego'
                        WHEN "Nombre de la provincia" = 'Ciudad Autónoma de Buenos Aires'
                            THEN 'CABA'
                        ELSE "Nombre de la provincia"
                        END
                ORDER BY "Nombre de la provincia", "Año del censo";
            """

totales = dd.query(consulta_totales).df()

fig, ax = plt.subplots() 
df_2010 = totales[totales["Año del censo"] == 2010]
df_2022 = totales[totales["Año del censo"] == 2022]
x = np.arange(len(df_2010))
A_2010 = df_2010["Total habitantes"]
A_2022 = df_2022["Total habitantes"]

width = 0.4

ax.bar(x - width/2, A_2010, width=width, label='Año 2010')
ax.bar(x + width/2, A_2022, width=width, label='Año 2022')

ax.set_title('Cantidad de habitantes por provincia')
ax.set_xlabel('Provincias')
ax.set_xticks(x)
ax.set_xticklabels(df_2010["Nombre de la provincia"], rotation=60, ha="right")
ax.set_ylabel('Habitantes')

ax.legend()

plt.show()



