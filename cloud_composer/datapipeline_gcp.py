from airflow import DAG
from airflow import models

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from datetime import timedelta, datetime
import logging
import requests
import random

DEFAULT_ARGS = {
    'owner': 'Ronny',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}

task_arguments = {
    'url_campaigns_csv': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/CAMPAIGNS_CSV',
    'url_transactions_csv': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/TRANSACTIONS_CSV',
    'url_campaigns_txt': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/CAMPAIGNS_TXT',
    'url_transactions_txt': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/TRANSACTIONS_TXT'
}

def make_curl_request(url):
    quantity = random.randint(500, 1000)
    url = f'{url}?quantity={quantity}'
    headers = {'accept': 'application/json'}

    try:
        response = requests.post(url, headers=headers, data='')
        response.raise_for_status()
        logging.info('Request successful')
        logging.info(f'Response text: {response.text}')
    except requests.exceptions.RequestException as e:
        logging.error(f'Request failed: {str(e)}')

with DAG(
    'datapipeline_gcp',
    default_args=DEFAULT_ARGS,
    description='Data Pipeline with Some Services',
    catchup=False,
    start_date=datetime(2023, 7, 1),
    schedule_interval='@once',
    tags=['ingest', 'csv', 'bigquery']
) as dag:
    
    ingest_campaigns_txt = PythonOperator(
        task_id='ingest_campaigns_txt',
        python_callable=make_curl_request,
        op_args=[task_arguments['url_campaigns_txt']]
    )

    ingest_transactions_txt = PythonOperator(
        task_id='ingest_transactions_txt',
        python_callable=make_curl_request,
        op_args=[task_arguments['url_transactions_txt']]
    )

    ingest_campaigns_csv = PythonOperator(
        task_id='ingest_campaigns_csv',
        python_callable=make_curl_request,
        op_args=[task_arguments['url_campaigns_csv']]
    )

    ingest_transactions_csv = PythonOperator(
        task_id='ingest_transactions_csv',
        python_callable=make_curl_request,
        op_args=[task_arguments['url_transactions_csv']]
    )

