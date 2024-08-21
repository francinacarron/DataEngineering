import sys
import os

# Añadir el directorio raíz al PYTHONPATH
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# src/main.py
from api_to_redshift import fetch_data_from_api, process_and_load_data

def main():
    """Función principal que orquesta la obtención y carga de datos."""
    data = fetch_data_from_api()
    print(f"Toma la data {data}")
    #Chequear que no hay datos duplicados
    if data.duplicated().any():
        print("Filtrando datos duplicados...")
        data = data.drop_duplicates().any()
        process_and_load_data(data)
    #Una vez filtrados los duplicados, cargar los datos  ..
    else:  
        process_and_load_data(data)

if __name__ == "__main__":
    main()
