import sys
import os

# Añadir el directorio raíz al PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# src/main.py
from api_to_redshift import fetch_data_from_api, process_and_load_data

def main():
    """Función principal que orquesta la obtención y carga de datos."""
    data = fetch_data_from_api()
    if data:
        process_and_load_data(data)
    else:
        print("No se pudo obtener datos de la API.")

if __name__ == "__main__":
    main()
