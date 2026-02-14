import pandas as pd
import duckdb as dd

#%% DATASETS

#directorio de los archivos de ambos censos, defunciones e instituciones de salud
carpeta = r'C:\Users\Delfina\Desktop\EXACTAS\LABO_DATOS\tps\tp1'

censo2010               = pd.read_excel(carpeta + "/censo2010.xlsX", skiprows=14) #salto hasta la fila que interesa
censo2022               = pd.read_excel(carpeta + "/censo2022.xlsX", skiprows=14)
archivo_defunciones         = pd.read_csv(carpeta + "/defunciones.csv")
instituciones_de_salud  = pd.read_excel(carpeta + "/instituciones_de_salud.xlsx")
categoriasDefunciones   = pd.read_csv(carpeta + "/categoriasDefunciones.csv")

#%% CENSO 2010

#normalizacion de nombres de las columnas del censo 2010
censo2010.columns = ['vacia', 'col_cobertura', 'col_edad', 'col_varon', 'col_mujer', 'col_total']

#estructuras temporales para manejar datos de rango etario, provincia, cobertura y sexo
suma_total = {} 
provincia_actual = ""
cobertura_actual = ""

for _, fila in censo2010.iterrows():
    val_cobertura = str(fila['col_cobertura'])
    val_edad = str(fila['col_edad'])
    
    if "AREA" in val_cobertura:
        provincia_actual = str(fila['col_edad'])
    #detecta el cambio de jurisdiccion y se actualiza el estado
    
    elif "RESUMEN" in val_cobertura:
        break
    #evita procesar datos demas, marca el fin de los datos utiles
    
    if val_cobertura == "Total":
        cobertura_actual = "nan" 
    if val_cobertura != "nan" and val_cobertura != "Total":
        cobertura_actual = val_cobertura
    #actualizaciones de estado de cobertura_actual

    if val_edad.isdigit() and cobertura_actual != "nan" :
        edad_nro = int(val_edad)
        if edad_nro <= 14: rango = '0-14'
        elif edad_nro <= 34: rango = '15-34'
        elif edad_nro <= 54: rango = '35-54'
        elif edad_nro <= 74: rango = '55-74'
        else: rango = '75+'
    #agrupacion de los rangos etarios y limpieza de valores nulos
        
        mujeres = float(fila['col_mujer']) if str(fila['col_mujer']) != "-" else 0
        varones = float(fila['col_varon']) if str(fila['col_varon']) != "-" else 0

        clave = (provincia_actual, cobertura_actual, rango)

        if clave not in suma_total:
            suma_total[clave] = {'mujer': 0, 'varon': 0}
        
        suma_total[clave]['mujer'] += mujeres
        suma_total[clave]['varon'] += varones
        #suma de habitantes separados por sexo al diccionario

#conversión del diccionario completo a un dataframe
lista_final = []
for clave, valor in suma_total.items():
    fila = {'provincia': clave[0],
            'cobertura': clave[1],
            'rango_etario': clave[2],
            'mujer': valor['mujer'],
            'varon': valor['varon']}
    lista_final.append(fila)

df_censo_2010 = pd.DataFrame(lista_final)

#%% CENSO 2022

#mismos procesos que con censo 2010

censo2022.columns = ['vacia', 'col_cobertura', 'col_edad', 'col_varon', 'col_mujer', 'col_total']

suma_total = {} 
provincia_actual = ""
cobertura_actual = ""

for _, fila in censo2022.iterrows():
    val_cobertura = str(fila['col_cobertura'])
    val_edad = str(fila['col_edad'])
    
    if "AREA" in val_cobertura:
        provincia_actual = str(fila['col_edad'])
        
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
    fila = {'provincia': clave[0],
            'cobertura': clave[1],
            'rango_etario': clave[2],
            'mujer': valor['mujer'],
            'varon': valor['varon']}
    lista_final.append(fila)

df_censo_2022 = pd.DataFrame(lista_final)

#%% Tabla de defunciones

consultaSQL = """
                SELECT 
                    c.categorias AS descripcion,
                    d.anio AS año,
                    d.jurisdiccion_de_residencia_id AS provincia_id,
                    CASE
                        WHEN d.grupo_edad LIKE '01.%' THEN '0-14'
                        WHEN d.grupo_edad LIKE '02.%' THEN '15-34'
                        WHEN d.grupo_edad LIKE '03.%' THEN '35-54'
                        WHEN d.grupo_edad LIKE '04.%' THEN '55-74'
                        WHEN d.grupo_edad LIKE '05.%' THEN '75+'
                        END AS grupo_etario,
                    CASE
                        WHEN d.Sexo = 'femenino' THEN 'F'
                        WHEN d.Sexo = 'masculino' THEN 'M'
                        END AS sexo,
                    SUM(cantidad) AS cantidad
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
                SELECT DISTINCT jurisdiccion_de_residencia_id AS id, LOWER(jurisdicion_residencia_nombre) AS nombre
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
                SELECT DISTINCT departamento_id, LOWER(departamento_nombre) AS nombre, provincia_id
                FROM instituciones_de_salud;
            """
            
df_departamentos = dd.sql(consultaSQL).df()
df_departamentos.to_csv("df_departamentos.csv", index = False)
#COMUNA01 tiene dos ID's distintos pero vienen de la misma provincia

#%% Tabla de habitantes 

consultaSQL = """
                SELECT 
                    2010 AS año_censo, 
                    p.id AS provincia_id, 
                    CASE 
                        WHEN c.cobertura LIKE 'Obra%'  OR c.cobertura LIKE 'Prepaga%'THEN 'Privada'
                        WHEN c.cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS cobertura, 
                    c.rango_etario AS grupo_etario, 
                    'F' AS sexo,
                    c.mujer AS cantidad
                FROM df_censo_2010 AS c
                INNER JOIN df_provincias AS p
                ON LOWER(TRIM(c.provincia)) = LOWER(TRIM(p.nombre))
                
                UNION ALL
                
                SELECT 
                    2010 AS año_censo, 
                    p.id AS provincia_id, 
                    CASE 
                        WHEN c.cobertura LIKE 'Obra%'  OR c.cobertura LIKE 'Prepaga%'THEN 'Privada'
                        WHEN c.cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS Cobertura_Social, 
                    c.rango_etario AS grupo_etario, 
                    'M' AS sexo,
                    c.varon AS cantidad
                FROM df_censo_2010 AS c
                INNER JOIN df_provincias AS p
                ON LOWER(TRIM(c.provincia)) = LOWER(TRIM(p.nombre))
                
                UNION ALL
                
                SELECT 
                    2022 AS año_censo, 
                    p.id AS provincia_id, 
                    CASE 
                        WHEN c.cobertura LIKE 'Obra%' THEN 'Privada'
                        WHEN c.cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS Cobertura_Social, 
                    c.rango_etario AS grupo_etario, 
                    'F' AS sexo,
                    c.mujer AS cantidad
                FROM df_censo_2022 AS c
                INNER JOIN df_provincias AS p
                ON LOWER(TRIM(c.provincia)) = LOWER(TRIM(p.nombre))
                
                UNION ALL
                
                SELECT 
                    2022 AS año_censo, 
                    p.id AS provincia_id, 
                    CASE 
                        WHEN c.cobertura LIKE 'Obra%' THEN 'Privada'
                        WHEN c.cobertura LIKE 'Programas%' THEN 'Pública'
                        WHEN c.cobertura LIKE 'No%' THEN 'No tiene cobertura social'
                        END AS Cobertura_Social, 
                    c.rango_etario AS grupo_etario, 
                    'M' AS sexo,
                    c.varon AS cantidad
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
                    establecimiento_id AS id, 
                    LOWER(establecimiento_nombre) AS nombre, 
                    departamento_id,
                    origen_financiamiento AS financiamiento,
                    CASE 
                        WHEN LOWER(tipologia_nombre) LIKE '%terapia intensiva%' THEN 'si'
                        ELSE 'no'
                        END AS terapia_intensiva
                FROM instituciones_de_salud;
            """

df_establecimientos_medicos = dd.sql(consultaSQL).df()
df_establecimientos_medicos.to_csv("df_establecimientos_medicos.csv", index = False)

#%% Tabla provincia_x_habitantes

#la tabla provincia_x_habitantes es igual a la tabla habitantes solo que en vez del id de la provincia tiene el nombre
unir_provincias_habitantes = """
                SELECT p.nombre AS nombre_prov, h.sexo, h.año_censo, h.cantidad, h.grupo_etario, h.cobertura
                FROM df_provincias AS p
                INNER JOIN df_habitantes AS h
                ON p.id == h.provincia_id
              """

provincia_x_habitantes = dd.query(unir_provincias_habitantes).df()

#%% REPORTE i

# i)
consulta_i = """ 
    SELECT provincia_id AS Provincia, grupo_etario, 
    
    -- Columnas para el año 2010
    SUM(CASE WHEN año_censo = 2010 AND cobertura = 'Privada' THEN cantidad ELSE 0 END) 
        AS Habitantes_con_cobertura_en_2010,
    SUM(CASE WHEN año_censo = 2010 AND cobertura = 'Pública' THEN cantidad ELSE 0 END) 
        AS Habitantes_sin_cobertura_en_2010,
    
    -- Columnas para el año 2022
    SUM(CASE WHEN año_censo = 2022 AND cobertura = 'Privada' THEN cantidad ELSE 0 END) 
        AS Habitante_con_cobertura_en_2022,
    SUM(CASE WHEN año_censo = 2022 AND cobertura = 'Pública' THEN cantidad ELSE 0 END) 
        AS Habitantes_sin_cobertura_en_2022
FROM df_habitantes
GROUP BY Provincia, grupo_etario
ORDER BY Provincia, grupo_etario"""

reporte_i = dd.sql(consulta_i).df()

#%% REPORTE ii

# ii)
consulta_ii = """
    SELECT d.provincia_id, e.financiamiento,
    SUM(CASE WHEN e.terapia_intensiva = 'si' THEN 1 ELSE 0 END) AS cantidad_establecimientos
    FROM df_establecimientos_medicos AS e
    
    JOIN df_departamentos AS d 
        ON e.departamento_id = d.departamento_id
    GROUP BY d.provincia_id, e.financiamiento
    ORDER BY d.provincia_id, e.financiamiento
"""

reporte_ii = dd.sql(consulta_ii).df()

#%% REPORTE iii
# iii)
consulta_iii = """


 """

reporte_iii = dd.query(consulta_iii).df()


#%% REPORTE iv

# iv)
consulta_iv = """
    -- aca hay q agrupar las defunciones para que no se dupliquen dsp del join
    WITH defunciones_agrupadas AS 
       (SELECT provincia_id, grupo_etario, SUM(cantidad) AS total_muertes
        FROM df_defunciones
        WHERE año = 2022
        GROUP BY provincia_id, grupo_etario),
       
    -- dsp se agrupan a todos los habitantes x prov y grupo etario sin discriminar sexo ni cobertura
    habitantes_agrupados AS
       (SELECT provincia_id, grupo_etario, SUM(cantidad) AS total_poblacion
        FROM df_habitantes
        WHERE año_censo = 2022
        GROUP BY provincia_id, grupo_etario)
        
    SELECT p.nombre AS Provincia, h.grupo_etario, (d.total_muertes * 1000.0 / h.total_poblacion) AS tasa_mortalidad_1000
    FROM habitantes_agrupados h
    INNER JOIN defunciones_agrupadas d 
        ON h.provincia_id = d.provincia_id 
        AND h.grupo_etario = d.grupo_etario
    INNER JOIN df_provincias p 
        ON h.provincia_id = p.id
    ORDER BY p.nombre, h.grupo_etario
            """

reporte_iv = dd.sql(consulta_iv).df()

#%% REPORTE v

# v)
consulta_v = """
    SELECT 
        descripcion,
        SUM(CASE WHEN Año = 2022 THEN cantidad ELSE 0 END) - 
        SUM(CASE WHEN Año = 2010 THEN cantidad ELSE 0 END) AS diferencia_defunciones
    FROM df_defunciones
    GROUP BY descripcion
    ORDER BY diferencia_defunciones DESC
 """

reporte_v = dd.query(consulta_v).df()