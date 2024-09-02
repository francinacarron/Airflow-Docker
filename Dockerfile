# Dockerfile
FROM python:3.9-slim

# Instalación de dependencias
RUN pip install apache-airflow pandas requests psycopg2-binary

# Copiar el DAG y los módulos
COPY main_dag.py /opt/airflow/dags/
COPY modules/ /opt/airflow/dags/modules/

# Crear el directorio para guardar el CSV
RUN mkdir -p /opt/airflow/dags/data

# Comando para iniciar Airflow
CMD ["airflow", "scheduler"]
