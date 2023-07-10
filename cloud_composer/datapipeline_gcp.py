from airflow import DAG
from airflow import models
from airflow.utils.task_group import TaskGroup

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

TASK_ARGUMENTS = {
    'url_campaigns_csv': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/CAMPAIGNS_CSV',
    'url_transactions_csv': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/TRANSACTIONS_CSV',
    'url_campaigns_txt': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/CAMPAIGNS_TXT',
    'url_transactions_txt': 'https://ronny-apis-5ga5qexs6q-uc.a.run.app/create/TRANSACTIONS_TXT'
}

PATHS = {
    'campaigns_csv': 'gs://dev-ronny-datalake-raw/ingested/csv/campaigns_*.csv',
    'transactions_csv': 'gs://dev-ronny-datalake-raw/ingested/csv/transactions_*.csv',
    'campaigns_txt': 'gs://dev-ronny-datalake-raw/ingested/txt/campaigns_*.txt',
    'transactions_txt': 'gs://dev-ronny-datalake-raw/ingested/txt/transactions_*.txt',
    'loaded' : 'gs://dev-ronny-datalake-raw/loaded'
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
    default_args = DEFAULT_ARGS,
    description = 'Data Pipeline with Some Services',
    catchup = False,
    start_date = datetime(2023, 7, 1),
    schedule_interval = None,
    tags = ['ingest', 'csv', 'bigquery']

) as dag:
    
    start_datapipeline = DummyOperator(
        task_id = 'start_datapipeline'
    )

    with TaskGroup('api_local') as api_local:
        with TaskGroup('ingest_from_api_local') as ingest_from_api_local:
            ingest_campaigns_csv = PythonOperator(
                task_id = 'ingest_campaigns_csv',
                python_callable = make_curl_request,
                op_args = [TASK_ARGUMENTS['url_campaigns_csv']]
            )

            ingest_transactions_csv = PythonOperator(
                task_id = 'ingest_transactions_csv',
                python_callable = make_curl_request,
                op_args = [TASK_ARGUMENTS['url_transactions_csv']]
            )

            [ingest_campaigns_csv , ingest_transactions_csv]
        


        with TaskGroup('load_to_bq_raw') as load_to_bq_raw:

            campaigns_ingested_sensor = GCSObjectsWithPrefixExistenceSensor(
                task_id='campaigns_ingested_sensor',
                bucket='dev-ronny-datalake-raw',
                prefix='ingested/csv/campaigns_{{ ds_nodash }}_',
                google_cloud_conn_id='google_cloud_default',
                timeout = 15
            )    

            transactions_ingested_sensor = GCSObjectsWithPrefixExistenceSensor(
                task_id='transactions_ingested_sensor',
                bucket='dev-ronny-datalake-raw',
                prefix='ingested/csv/transactions_{{ ds_nodash }}_',
                google_cloud_conn_id='google_cloud_default',
                timeout = 15
            )

            starting_loading_to_raw = DummyOperator(
                task_id = 'starting_loading_to_raw'
            )

            load_campaign = GoogleCloudStorageToBigQueryOperator(
                task_id = "load_campaign",
                bucket = 'dev-ronny-datalake-raw',
                source_objects = ['ingested/csv/campaigns_*.csv'],    
                destination_project_dataset_table ='raw_layer.r_campaigns',

                schema_fields=[
                    {'name': 'campaign_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'cost', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'date_time', 'type': 'STRING', 'mode': 'NULLABLE'}
                ],
                create_disposition = 'CREATE_IF_NEEDED',
                skip_leading_rows =1,
                write_disposition = 'WRITE_APPEND'
            )

            load_transaction = GoogleCloudStorageToBigQueryOperator(
                task_id = "load_transaction",
                bucket = 'dev-ronny-datalake-raw',
                source_objects = ['ingested/csv/transactions_*.csv'],    
                destination_project_dataset_table ='raw_layer.r_transactions',

                schema_fields=[
                    {'name': 'transaction_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'income', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'date_time', 'type': 'STRING', 'mode': 'NULLABLE'}
                ],
                create_disposition = 'CREATE_IF_NEEDED',
                skip_leading_rows =1,
                write_disposition = 'WRITE_APPEND'
            )

            smart_cleaner = BashOperator(
                task_id = 'smart_cleaner',
                bash_command = """
                    gsutil mv {{ params.path_campaigns }} {{ params.target }} && 
                    gsutil mv {{ params.path_transactions }} {{ params.target }}         
                """,        
                params = {
                    'path_campaigns': PATHS['campaigns_csv'],
                    'path_transactions': PATHS['transactions_csv'],
                    'target': PATHS['loaded']
                }
            )

            [campaigns_ingested_sensor, transactions_ingested_sensor] >> starting_loading_to_raw 
            
            starting_loading_to_raw >> [load_campaign, load_transaction] >> smart_cleaner
        
        ingest_from_api_local >> load_to_bq_raw


    with TaskGroup('api_global') as api_global:
        ingest_campaigns_txt = PythonOperator(
            task_id = 'ingest_campaigns_txt',
            python_callable = make_curl_request,
            op_args = [TASK_ARGUMENTS['url_campaigns_txt']]
        )
        ingest_transactions_txt = PythonOperator(
            task_id = 'ingest_transactions_txt',
            python_callable = make_curl_request,
            op_args = [TASK_ARGUMENTS['url_transactions_txt']]
        )

    start_datapipeline >> [api_local, api_global]

