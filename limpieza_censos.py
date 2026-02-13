import pandas as pd

carpeta = '/home/Estudiante/Descargas/Archivos TP 1-20260210'

censo2010               = pd.read_excel(carpeta + "/censo2010.xlsX", skiprows=14)
censo2022               = pd.read_excel(carpeta + "/censo2022.xlsX", skiprows=14)
defunciones             = pd.read_csv(carpeta + "/defunciones.csv")
instituciones_de_salud  = pd.read_excel(carpeta + "/instituciones_de_salud.xlsx")

##### 2010

censo2010.columns = ['vacia', 'col_cobertura', 'col_edad', 'col_varon', 'col_mujer', 'col_total']

suma_total = {} 
provincia_actual = ""
cobertura_actual = ""

for _, fila in censo2010.iterrows():
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

    if val_edad.isdigit() and cobertura_actual != "nan" :
        edad_nro = int(val_edad)
        if edad_nro <= 14: rango = '0-14'
        elif edad_nro <= 34: rango = '15-34'
        elif edad_nro <= 54: rango = '35-54'
        elif edad_nro <= 74: rango = '55-74'
        else: rango = '75+'
        
        m = float(fila['col_mujer']) if str(fila['col_mujer']) != "-" else 0
        v = float(fila['col_varon']) if str(fila['col_varon']) != "-" else 0

        k = (provincia_actual, cobertura_actual, rango)

        if k not in suma_total:
            suma_total[k] = {'mujer': 0, 'varon': 0}
        
        suma_total[k]['mujer'] += m
        suma_total[k]['varon'] += v

lista_final = []
for k, v in suma_total.items():
    fila = {'provincia': k[0],
            'cobertura': k[1],
            'rango_etario': k[2],
            'mujer': v['mujer'],
            'varon': v['varon']}
    lista_final.append(fila)

df_censo_2010 = pd.DataFrame(lista_final)

##### 2022

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

        k = (provincia_actual, cobertura_actual, rango)

        if k not in suma_total:
            suma_total[k] = {'mujer': 0, 'varon': 0}
        
        suma_total[k]['mujer'] += m
        suma_total[k]['varon'] += v

lista_final = []
for k, v in suma_total.items():
    fila = {'provincia': k[0],
            'cobertura': k[1],
            'rango_etario': k[2],
            'mujer': v['mujer'],
            'varon': v['varon']}
    lista_final.append(fila)

df_censo_2022 = pd.DataFrame(lista_final)
