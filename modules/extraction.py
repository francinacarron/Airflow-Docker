# modules/extraction.py
import requests
import json
import pandas as pd
from datetime import datetime

# Definir la URL de la API
url = 'https://www.windguru.net/int/iapi.php?q=live_update&lat=-34.598&lon=-58.402&WGCACHEABLE=30#'

def fetch_data_and_save_to_csv(path, date):
    """Obtiene datos de la API de Windguru y los guarda en un archivo CSV."""
    response = requests.get(url)
    if response.status_code == 200:
        try:
            # Para eliminar el error por el BOM
            data = json.loads(response.content.decode('utf-8-sig'))
            df = pd.DataFrame(data)
            print(f"DataFrame resultante: {df.head()}")  # Mostrar las primeras filas del DataFrame
            
            # Generar el nombre del archivo CSV usando path y date
            file_path = f"{path}/windguru_data_{date}.csv"
            df.to_csv(file_path, index=False)
            print(f"Datos guardados en {file_path}")
            
            return file_path  # Devolver la ruta del archivo CSV
        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}")
            return None
    else:
        print(f"Error: {response.status_code}")
        return None
