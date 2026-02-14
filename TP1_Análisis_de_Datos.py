# -*- coding: utf-8 -*-
"""
Sección analisis de datos

Consultas

"""
#%%
#importamos las librerias
import pandas as pd
import duckdb as dd

#%%
#Importamos nuestros datasets
carpeta = 'ubicacion...'

provincias                       = pd.read_csv(carpeta + "/provincias.csv")
departamentos                    = pd.read_csv(carpeta + "/departamentos.csv")
habitantes                       = pd.read_csv(carpeta + "/habitantes.csv")
defunciones                      = pd.read_csv(carpeta + "/defunciones.csv")
establecimientos_medicos         = pd.read_csv(carpeta + "/establecimientos_medicos.csv")

#%%
# TABLAS ÚTILES

#la tabla provincia_x_habitantes es igual a la tabla habitantes solo que en vez del id de la provincia tiene el nombre
unir_provincias_habitantes = """
                SELECT p.nombre AS nombre_prov, h.sexo, h.año_censo, h.cantidad, h.grupo_etario, h.cobertura
                FROM provincias AS p
                INNER JOIN habitantes AS h
                ON p.id == h.provincia_id
              """

provincia_x_habitantes = dd.query(unir_provincias_habitantes).df()

#%% ###REPORTES
#%%
# i)
consulta_i = """ 
    SELECT nombre_prov AS Provincia, grupo_etario, 
    
    -- Columnas para el año 2010
    SUM(CASE WHEN año = 2010 AND cobertura = 'Privada' THEN cantidad ELSE 0 END) 
        AS Habitantes_con_cobertura_en_2010,
    SUM(CASE WHEN año = 2010 AND cobertura = 'Pública' THEN cantidad ELSE 0 END) 
        AS Habitantes_sin_cobertura_en_2010,
    
    -- Columnas para el año 2022
    SUM(CASE WHEN año = 2022 AND cobertura = 'Privada' THEN cantidad ELSE 0 END) 
        AS Habitante_con_cobertura_en_2022,
    SUM(CASE WHEN año = 2022 AND cobertura = 'Pública' THEN cantidad ELSE 0 END) 
        AS Habitantes_sin_cobertura_en_2022
FROM Habitantes
GROUP BY Provincia, grupo_etario
ORDER BY Provincia, grupo_etario"""


reporte_i = dd.query(consulta_i).df()

#%%
# ii)
consulta_ii = """
    SELECT d.provincia_id, e.financiamiento,
    SUM(CASE WHEN e.terapia_intensiva = 'si' THEN 1 ELSE 0 END) AS cantidad_establecimientos
    FROM establecimientos_medicos AS e
    
    JOIN departamentos AS d 
        ON e.departamento_id = d.departamento_id
    GROUP BY d.provincia_id, e.financiamiento
    ORDER BY d.provincia_id, e.financiamiento
"""

reporte_ii = dd.query (consulta_ii).df()

#%%
# iii)
consulta_iii = """


 """

reporte_iii = dd.query (consulta_iii).df()


#%%
# iv)
consulta_iv = """
    SELECT 
        h.nombre, 
        h.grupo_etario,
        (SUM(d.cantidad) * 1000.0 / SUM(h.cantidad)) AS tasa_mortalidad_1000
    FROM provincias_x_habitantes AS h
    JOIN defunciones d ON h.provincia_id = d.provincia_id 
        AND h.grupo_etario = d.grupo_etario
        AND h.año = d.año
    WHERE h.año = 2022
    GROUP BY h.nombre, h.grupo_etario
    ORDER BY h.nombre, h.grupo_etario
 """

reporte_iv = dd.query (consulta_iv).df()

#%%
# v)
consulta_v = """
    SELECT 
        descripcion,
        SUM(CASE WHEN año = 2022 THEN cantidad ELSE 0 END) - 
        SUM(CASE WHEN año = 2010 THEN cantidad ELSE 0 END) AS diferencia_defunciones
    FROM defunciones
    GROUP BY descripcion
    ORDER BY diferencia_defunciones DESC
 """

reporte_v = dd.query (consulta_v).df()




