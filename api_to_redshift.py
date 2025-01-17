# src/api_to_redshift.py

import requests
import json
from datetime import datetime
from redshift_connection import connect_to_redshift, close_connection
import pandas as pd
import numpy as np

# Definir la URL de la API
url = 'https://www.windguru.net/int/iapi.php?q=live_update&lat=-34.598&lon=-58.402&WGCACHEABLE=30#'

def fetch_data_from_api():
    """Obtiene datos de la API de Windguru."""
    response = requests.get(url)
    #Si esta todo OK
    if response.status_code == 200:
        try:
            #Para eliminar el error por el BOM
            d = json.loads(response.content.decode('utf-8-sig'))
            data = pd.DataFrame(d)
            print(f"DataFrame resultante: {data.head()}")  # Mostrar las primeras filas del DataFrame
            return data
        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}")
            return None
    else:
        print(f"Error: {response.status_code}")
        return None
    
    
#Procesamiento y carga de datos en redshift
def process_and_load_data(data):
    conn = connect_to_redshift()
    cursor = conn.cursor()
    
    # Eliminar y crear la tabla
    drop_table_query = "DROP TABLE IF EXISTS estaciones;"
    create_table_query = """
    CREATE TABLE estaciones (
        id_station INT,
        datetime TIMESTAMP,
        wind_avg FLOAT,
        wind_max FLOAT,
        wind_min FLOAT,
        temperature FLOAT,
        wind_direction INT,
        consulta_date TIMESTAMP
    );
    """
    #No estoy segura donde seria mejor ubicar esto, pero dado que esta mas vinculado con el procesamiento para la carga de datos lo pongo aca
    # Chequear si hay algún valor NaN en el DataFrame
    if data.isnull().values.any():
        print("Hay valores NaN en el DataFrame, se procederá a rellenarlos.")
        data.fillna(method='ffill', inplace=True)
        data.fillna(method='bfill', inplace=True)
    else:
        print("No hay valores NaN en el DataFrame.")
    
    try:
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabla 'estaciones' eliminada y creada exitosamente.")
    except Exception as e:
        print(f"Error al eliminar o crear la tabla: {e}")
        conn.rollback()
    
    estaciones = []
    unixtime = data['unixtime']
    dt = []
    #Si existe la fecha la convierto a texto para que sea legible por JSON, si no le paso fecha del dia del fin de mundo jeje
    for day in unixtime:
        dt.append(datetime.fromtimestamp(day).strftime('%Y-%m-%d %H:%M:%S') if day != 0 else '2021-12-21 24:00:00')
    
    for _, row in data.iterrows():
        estaciones.append((
            row['id_station'].item(),
            datetime.fromtimestamp(row['unixtime'].item()),
            row['wind_avg'].item(),
            row['wind_max'].item(),
            row['wind_min'].item(),
            row['temperature'].item(),
            row['wind_direction'].item(),
            datetime.today()  # Agregando la fecha actual
        ))

    print(f"Estaciones tiene el valor {estaciones}")
    # Inserto datos en la tabla
    insert_query = """
    INSERT INTO estaciones (id_station, datetime, wind_avg, wind_max, wind_min, temperature, wind_direction, consulta_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cursor.executemany(insert_query, estaciones)
        conn.commit()
    except Exception as e:
        print(f"Error al insertar dato: {e}")
        conn.rollback()
    finally:
        cursor.close()
        close_connection(conn)

#Para chequear la funcion
#process_and_load_data(data)
