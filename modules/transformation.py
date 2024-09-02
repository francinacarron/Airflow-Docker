# modules/transformation.py
import pandas as pd

def transform_data(csv_file_path):
    """Realiza la transformación de los datos del archivo CSV."""
    df = pd.read_csv(csv_file_path)
    
    # Chequear si hay valores NaN y tratarlos
    if df.isnull().values.any():
        print("Hay valores NaN en el DataFrame, se procederá a rellenarlos.")
        df.fillna(method='ffill', inplace=True)
        df.fillna(method='bfill', inplace=True)
    else:
        print("No hay valores NaN en el DataFrame.")
    
    # Eliminar duplicados
    if df.duplicated().any():
        print("Filtrando datos duplicados...")
        df = df.drop_duplicates()
    
    # Guardar las transformaciones nuevamente en el mismo archivo CSV
    df.to_csv(csv_file_path, index=False)
    print(f"Datos transformados guardados en {csv_file_path}")
    
    return csv_file_path  # Devolver la ruta del archivo CSV transformado
