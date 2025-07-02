from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import os
import logging

# Configuración básica
PROJECT_ID = "sage-artifact-464700-d6"
BUCKET_NAME = "etl-sri-bucket"
BLOB_NAME = "SRI - CATASTRO (RUC).xlsx - Datos .csv"
DATASET_ID = "dw_sri"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.ruc_azuay"
LOCAL_FILE = "/opt/airflow/include/sri_ruc.csv"
CREDENTIALS_PATH = "/opt/airflow/include/keys/gcp-credentials.json"

# Autenticación
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
storage_client = storage.Client(credentials=credentials)
bigquery_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Función: Descargar archivo desde GCS
def download_file_from_gcs():
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)
    blob.download_to_filename(LOCAL_FILE)
    logging.info(f"Archivo descargado a {LOCAL_FILE}")

# Función: Transformar con pandas y cargar a BigQuery
def transform_and_load_to_bigquery():
    df = pd.read_csv(LOCAL_FILE)
    
    # Ejemplo de limpieza (puedes personalizar según tus columnas)
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df.dropna(subset=['ruc'], inplace=True)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )
    bigquery_client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    logging.info(f"Datos cargados exitosamente a {TABLE_ID}")

# Configuración del DAG
default_args = {
    'owner': 'emilio',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_sri_catastro_ruc',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG para ETL de Catastro SRI desde GCS a BigQuery',
    tags=['sri', 'etl', 'gcs', 'bigquery'],
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    descargar_archivo = PythonOperator(
        task_id='descargar_archivo_gcs',
        python_callable=download_file_from_gcs
    )

    cargar_bigquery = PythonOperator(
        task_id='transformar_y_cargar_bigquery',
        python_callable=transform_and_load_to_bigquery
    )

    fin = EmptyOperator(task_id='fin')

    # Flujo
    inicio >> descargar_archivo >> cargar_bigquery >> fin
