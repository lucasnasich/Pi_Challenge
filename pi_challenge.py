
#__________________________________________________________________________________________________________________________________________

#   ------- | DESCARGO LIBRERIAS NECESARIAS
#__________________________________________________________________________________________________________________________________________

from datetime import datetime
import pyodbc 
import pandas as pd
import os
import logging

#__________________________________________________________________________________________________________________________________________

#   ------- | DECLARO VARIABLES
#__________________________________________________________________________________________________________________________________________

#Url para descargar archivo CSV  
url = "https://gen2cluster.blob.core.windows.net/challenge/csv/nuevas_filas.csv?sp=r&st=2020-10-30T14:05:08Z&se=2020-11-30T22:05:08Z&spr=https&sv=2019-12-12&sr=b&sig=UCK4aQvPAIHz19h%2By2NNAYdzs2RF9myeVAFQkwP3Iuc%3D"

#Variables para realizar la conexión a SQL Server
server = 'localhost' 
database = 'Testing_ETL' 
username = 'SA' 
password = 'Password_Challenge123!'

#__________________________________________________________________________________________________________________________________________

#   ------- | DEFINO FUNCIONES DEL SCRIPT
#__________________________________________________________________________________________________________________________________________


#   ------<FUNCIÓN PARA DESCARGAR CSV DE LA URL OTORGADA POR EL NEGOCIO
#   -------------------------------------------------------------------

def download_CSV_URL():
    print('INICIO MODULO "download_CSV_URL"')
    print('- - - - - - - - - - - - - - - - ')
    #Descargamos el archivo utilizando la libreria pandas.
    print('Descargando CSV')
    data = pd.read_csv(url)
    print('---------------')

    #Aquí defino la fecha actual en la cual descargo el archivo csv; dentro del campo "FECHA_COPIA".
    print('Definiendo la Fecha actual en todos los registros del campo "FECHA_COPIA"')
    data["FECHA_COPIA"] = pd.to_datetime('now')
    print('-------------------------------------------------------------------------')

    #Vemos las dimensiones del dataframe = (REGISTROS, CAMPOS).
    dmn_data = str(data.shape)
    print('Dimensiones del dataset: '+dmn_data)
    print('-----------------------------------')
    
    #Vemos campos con valores únicos repetidos en todos sus registros.
    repeating_columns= [x for x in data.columns if data[x].nunique()==1]
    campos = str(repeating_columns)
    print('Campos con todos sus registros un solo valor único: '+campos)
    print('-------------------------------------------------------------------------------------------')

    #Dado el caso en el que vengan registros duplicados, los eliminamos:
    print('Verificando que no existan registros duplicados')
    data.drop_duplicates()
    data.drop_duplicates(['ID', 'MUESTRA', 'RESULTADO'])
    print('-----------------------------------------------')

    print('CSV-1 importado exitosamente')
    print('')
    print('')
    
    return data


#   ------<FUNCIÓN PARA GENERAR LA CONEXIÓN
#   ---------------------------------------

def generate_SQL_Connection():
    print('INICIO MÓDULO "generate_SQL_Connection"')
    print('- - - - - - - - - - - - - - - - - - - -')
    #Establecemos la conexión a SQL SERVER.
    print('Estableciendo conexión a SQL Server') 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    print('------------------------------------')

    print('Conexión generada Exitosamente')
    print('')
    print('')

    return cnxn 


#   ------<FUNCIÓN PARA DESCARGAR CSV DEL SERVIDOR DE SQL SERVER
#   ------------------------------------------------------------

def generate_CSV_SQLServer():
    cnxn = generate_SQL_Connection()
    cursor = cnxn.cursor()

    print('INICIO MÓDULO "generate_CSV_SQLServer"')
    print('- - - - - - - - - - - - - - - - - - - -')
    #Importamos la tabla "Unificado" proveniente de SQL SERVER, utilizando SQL y Pandas.
    print('Importando Tabla de datos desde SQL Server')
    sql_query = pd.read_sql_query('select * from Testing_ETL.dbo.Unificado',cnxn)
    #'cnxn' es la variable que contiene toda la informacion de la conexión a la base de datos.
    print('-------------------------------------------')

    #Creamos el CSV para manipularlo facilmente con pandas.
    print('Transformando tabla en un archivo CSV')
    df = pd.DataFrame(sql_query)
    print('--------------------------------------')

    dmn_df = str(df.shape)
    print('Dimensiones del dataset:'+dmn_df)
    print('-------------------------------------')

    #Chequeamos si existen duplicados
    duplicate = df[df.duplicated(['ID', 'MUESTRA', 'RESULTADO'])].value_counts().any()
    duplicate

    #ELimino los registros duplicados
    print('Eliminando registros duplicados')
    df = df.drop_duplicates(['ID', 'MUESTRA', 'RESULTADO'])
    print('--------------------------------')

    #Cierro conexion
    cursor.close()

    print('CSV-2 importado exitosamente')
    print('')
    print('')

    return df


#   ------<FUNCIÓN PARA UNIR LOS DOS DATASETS IMPORTADOS
#   ----------------------------------------------------

def datasets_concat(data, df):
    print('INICIO MÓDULO "datasets_concat"')
    print('- - - - - - - - - - - - - - - -')
    #Concatenamos ambos datasets
    print('Realizando concatenación entre ambos datasets')
    print('---------------------------------------------')
    df_row = pd.concat([data, df], ignore_index=True)
    print('Los dos datasets han sido unidos correctamente')
    print('----------------------------------------------')
    dmn_final = str(df_row.shape)
    print('Dimensiones del dataset unificado:'+dmn_final)
    print('---------------------------------------------------')

    #Ordenamos el dataset desde los registros más recientes a los más viejos.
    df_row = df_row.sort_values(["FECHA_COPIA"], ascending=False, ignore_index=True)
    print('Dataset Ordenado según FECHA_COPIA descendentemente')
    print('---------------------------------------------------')

    #En el caso de existir registros duplicados pos concatenación, los eliminamos:
    print('Verificando que no existan registros duplicados')
    print('-----------------------------------------------')
    df_row = df_row.drop_duplicates(['ID', 'MUESTRA', 'RESULTADO'], keep = 'first')

    print('Unión realizada correctamente')
    print('')
    print('')

    return df_row


#   ------<FUNCIÓN PARA EXPORTAR DATASET UNIFICADO A SQL SERVER
#   -----------------------------------------------------------

def export_to_msserver(df_row):
    print('INICIO MÓDULO "export_to_msserver"')
    print('- - - - - - - - - - - - - - - - -')
    #Primero Eliminamos los datos de la Tabla para insertar los datos del nuevo CSV
    cnxn = generate_SQL_Connection()
    cursor = cnxn.cursor()
    cursor.execute('TRUNCATE Table Testing_ETL.dbo.Unificado')
    print('Datos eliminados de la Tabla "Testing_ETL.dbo.Unificado"')
    print('-------------------------------------------------------')

    # Por ultimo, insertamos los datos del dataset en la Tabla de SQL Server:
    for index, row in df_row.iterrows():
        cursor.execute("INSERT INTO Testing_ETL.dbo.Unificado (CHROM,POS,ID,REF,ALT,QUAL,FILTER,INFO,FORMAT,MUESTRA,VALOR,ORIGEN,FECHA_COPIA,RESULTADO) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.CHROM, row.POS, row.ID, row.REF, row.ALT, row.QUAL, row.FILTER, row.INFO, row.FORMAT, row.MUESTRA, row.VALOR, row.ORIGEN, row.FECHA_COPIA, row.RESULTADO)
    print('Datos del CSV "df_row" insertados correctamente en la Tabla "Testing_ETL.dbo.Unificado" ')
    print('--------------------------------------------------------------------------------------')

    print('Exportación EXITOSA')
        
    cnxn.commit()
    cursor.close()


if __name__ == '__main__':
    
    data = download_CSV_URL()
    generate_SQL_Connection()
    df = generate_CSV_SQLServer()
    df_row = datasets_concat(data, df)
    export_to_msserver(df_row)