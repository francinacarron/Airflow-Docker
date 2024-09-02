# main_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from modules.extraction import fetch_data_and_save_to_csv
from modules.transformation import transform_data
from modules.load import load_data_to_redshift

# Definir los parámetros de path y date
path = "/opt/airflow/dags/data"
date = datetime.now().strftime('%Y%m%d')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

dag = DAG(
    'windguru_etl',
    default_args=default_args,
    description='DAG para extraer, transformar y cargar datos de Windguru a Redshift',
    schedule_interval='@daily',  # Ejecutar diariamente
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=fetch_data_and_save_to_csv,
    op_args=[path, date],
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[f"{path}/windguru_data_{date}.csv"],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_redshift,
    op_args=[f"{path}/windguru_data_{date}.csv"],
    dag=dag,
)

extract_task >> transform_task >> load_task
