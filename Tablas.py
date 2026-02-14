import pandas as pd
import duckdb as dd

carpeta = '/home/catherine/Downloads/Archivos TP 1-20260207/Archivos'

#importo los datasets

df_censo_2010 = pd.read_csv("df_censo_2010.csv")
df_censo_2022 = pd.read_csv("df_censo_2022.csv")
defunc = pd.read_csv("defunciones.csv")
instituciones_de_salud  = pd.read_excel("instituciones_de_salud.xlsx")
categoria_defun = pd.read_csv("categoriasDefunciones.csv")


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
                    FROM defunc AS d
                    INNER JOIN categoria_defun AS c
                    ON c.codigo_def = d.cie10_causa_id
                    GROUP BY c.categorias, d.grupo_edad, d.Sexo, d.anio, d.jurisdiccion_de_residencia_id;
            """

defunciones = dd.sql(consultaSQL).df()
defunciones.to_csv("defunciones.csv", index = False)
#hay sexos como None
#tambien hay grupo_etarios como None

#%% Tabla de provincias

consultaSQL = """
                SELECT DISTINCT jurisdiccion_de_residencia_id AS id, LOWER(jurisdicion_residencia_nombre) AS nombre
                FROM defunc
                ORDER BY id;
            """
provincias = dd.sql(consultaSQL).df()
provincias.to_csv("provincias.csv", index = False)

#aca aun tenemos que ver que hacemos con los nan y los sin informacion
#ademas, cambie a que todos los nombres de provincias y departamentos tuvieran el nombre en minuscula para que haya continuidad
#en todas las tablas (tambien lo cambie cuando se hace el df)
#esta bien que todo sea 'provincia o provincia_id' cuando son jurisdicciones?
#%% Tabla de departamentos

consultaSQL = """
                SELECT DISTINCT departamento_id, LOWER(departamento_nombre) AS nombre, provincia_id
                FROM instituciones_de_salud;
            """
            
departamentos = dd.sql(consultaSQL).df()
departamentos.to_csv("departamentos.csv", index = False)
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
                INNER JOIN provincias AS p
                ON c.provincia = p.nombre
                
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
                INNER JOIN provincias AS p
                ON c.provincia = p.nombre
                
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
                INNER JOIN provincias AS p
                ON c.provincia = p.nombre
                
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
                INNER JOIN provincias AS p
                ON c.provincia = p.nombre;
                
            """
#Aca pase las columnas mujer y varon a una sola columna como sexo, si no, hay que cambiar el DER me parece, tambien use UNION ALL 
#que creo que no lo vimos en clase
#esta tabla funciona bajo el supuesto de que en df_censo_2010/2022 cambie el nombre de CABA, tierra del fuego y rio negro
habitantes = dd.sql(consultaSQL).df()
habitantes.to_csv("habitantes.csv", index = False)
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

establecimientos_medicos = dd.sql(consultaSQL).df()
establecimientos_medicos.to_csv("establecimientos_medicos.csv", index = False)
