# src/redshift_connection.py
import psycopg2
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de la conexión
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

def connect_to_redshift():
    """Establece y devuelve una conexión a la base de datos Redshift."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Conexión exitosa a Redshift")
        return conn
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}")
        raise

def close_connection(conn):
    """Cierra la conexión a la base de datos."""
    try:
        conn.close()
        print("Conexión cerrada")
    except Exception as e:
        print(f"Error al cerrar la conexión: {e}")
        raise
