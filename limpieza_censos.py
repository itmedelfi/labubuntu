import pandas as pd
import duckdb as dd

#%% DATASETS

#directorio de los archivos de ambos censos, defunciones e instituciones de salud
carpeta = r'C:\Users\Delfina\Desktop\EXACTAS\LABO_DATOS\tps\tp1'

censo2010               = pd.read_excel(carpeta + "/censo2010.xlsX", skiprows=14) #salto hasta la fila que interesa
censo2022               = pd.read_excel(carpeta + "/censo2022.xlsX", skiprows=14)
defunc                  = pd.read_csv(carpeta + "/defunciones.csv")
instituciones_de_salud  = pd.read_excel(carpeta + "/instituciones_de_salud.xlsx")

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
        provincia_actual = str(fila['col_edad']).lower()
        if str(fila['col_edad']) == 'Tierra del Fuego':
            provincia_actual = 'tierra del fuego, antártida e islas del atlántico sur'
    #detecta el cambio de jurisdiccion y se actualiza el estado en minúsculas
    #y, en caso de encotrar la jurisdiccion 'Tierra del Fuego' le cambia el nombre
    #para que coincida en la tabla de provincias
    
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
        provincia_actual = str(fila['col_edad']).lower()
        if str(fila['col_edad']) == 'Caba':
            provincia_actual = 'ciudad autónoma de buenos aires'
        elif str(fila['col_edad']) == 'Tierra del Fuego':
            provincia_actual = 'tierra del fuego, antártida e islas del atlántico sur'

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

