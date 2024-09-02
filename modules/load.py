# modules/load.py
import pandas as pd
from datetime import datetime
from redshift_connection import connect_to_redshift, close_connection

def load_data_to_redshift(csv_file_path):
    """Carga los datos desde el archivo CSV a la tabla en Redshift."""
    df = pd.read_csv(csv_file_path)
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
    
    try:
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabla 'estaciones' eliminada y creada exitosamente.")
    except Exception as e:
        print(f"Error al eliminar o crear la tabla: {e}")
        conn.rollback()
    
    estaciones = []
    for _, row in df.iterrows():
        estaciones.append((
            row['id_station'],
            datetime.fromtimestamp(row['unixtime']),
            row['wind_avg'],
            row['wind_max'],
            row['wind_min'],
            row['temperature'],
            row['wind_direction'],
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
        print("Datos cargados exitosamente en Redshift.")
    except Exception as e:
        print(f"Error al insertar datos: {e}")
        conn.rollback()
    finally:
        cursor.close()
        close_connection(conn)
