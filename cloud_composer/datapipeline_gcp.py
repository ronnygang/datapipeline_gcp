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
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from datetime import timedelta, datetime
import logging
import requests
import random
import uuid

#API_GLOBAL
FECHA_FORMATO = datetime.now().strftime("%Y%m%d")
UUID_4DIG = str(uuid.uuid4().hex)[:4]
PROJECT_ID = 'ronny-dev-airflow'
REGION = 'us-central1'
CLUSTER_NAME = f'ephemeral-cluster-ronny-{FECHA_FORMATO}-{UUID_4DIG}'
PYSPARK_URI = 'gs://dev-ronny-datalake-raw/scripts/transformaciones_pyspark.py'
JAR_URI = 'gs://dev-ronny-datalake-raw/scripts/spark-bigquery-with-dependencies_2.12-0.30.0.jar'

#DATABASE_SQL
INSTANCE_NAME = 'dev-ronny-sql'
EXPORT_URI = f'gs://dev-ronny-datalake-raw/ingested/cloud_sql/stations_{FECHA_FORMATO}_{UUID_4DIG}.csv'
SQL_QUERY = "SELECT * FROM ronny_dev.stations"

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
    'campaigns_csv': 'gs://dev-ronny-datalake-raw/ingested/local/campaigns_*.csv',
    'transactions_csv': 'gs://dev-ronny-datalake-raw/ingested/local/transactions_*.csv',
    'campaigns_txt': 'gs://dev-ronny-datalake-raw/ingested/global/campaigns_*.txt',
    'transactions_txt': 'gs://dev-ronny-datalake-raw/ingested/global/transactions_*.txt',
    'users_csv' : 'gs://dev-ronny-datalake-raw/ingested/cloud_sql/users_*.csv',
    'loaded_local' : 'gs://dev-ronny-datalake-raw/loaded/local',
    'loaded_global' : 'gs://dev-ronny-datalake-raw/loaded/global',
    'loaded_cloud_sql' : 'gs://dev-ronny-datalake-raw/loaded/cloud_sql',
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 128},
    },
    "worker_config": {
        "num_instances": 3,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 128},
    }
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
    "jar_file_uris":[JAR_URI]
    }
}

EXPORT_BODY = {
    "exportContext": {
        "fileType": "csv",
        "uri": EXPORT_URI,
        "csvExportOptions":{
            "selectQuery": SQL_QUERY
        }
    }
}

QUERY_MASTER = """
    SELECT
        c.campaign_id,
        c.cost AS campaign_cost,
        t.transaction_id,
        t.income,
        t.country AS transaction_country,
        s.customer_id,
        s.product_id,
        s.quantity,
        s.price,
        s.category,
        t.date_time AS transaction_datetime
    FROM
       raw_layer.r_campaigns AS c
    JOIN
        raw_layer.r_transactions AS t 
        ON c.country = t.country 
        AND c.date_time = t.date_time
    JOIN
        raw_layer.r_sales AS s 
        ON t.transaction_id = s.transaction_id;
"""

QUERY_BUSINESS_PERFORMANCE_METRICS = """
    SELECT
        transaction_country AS country,
        DATE(transaction_datetime) AS date,
        COUNT(DISTINCT transaction_id) AS transaction_count,
        SUM(income) AS total_income,
        SUM(campaign_cost) AS total_campaign_cost,
        AVG(income) AS average_income,
        AVG(campaign_cost) AS average_campaign_cost,
        SUM(income - campaign_cost) AS total_profit,
        SUM(income) / COUNT(DISTINCT transaction_id) AS average_transaction_value
    FROM
        master_layer.m_data_model
    GROUP BY
        transaction_country,
        DATE(transaction_datetime);
"""

QUERY_BUSINESS_PRODUCT_PERFORMANCE = """
    SELECT
        product_id,
        transaction_country AS country,
        DATE(transaction_datetime) AS date,
        COUNT(DISTINCT transaction_id) AS transaction_count,
        SUM(quantity) AS total_quantity,
        SUM(income) AS total_income,
        AVG(income) AS average_income,
        SUM(income - campaign_cost) AS total_profit,
        SUM(income) / COUNT(DISTINCT transaction_id) AS average_transaction_value
    FROM
        master_layer.m_data_model
    GROUP BY
        product_id,
        transaction_country,
        DATE(transaction_datetime);
"""

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
    tags = ['datapipeline', 'gcp']

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
        


        with TaskGroup('load_raw_with_bq') as load_raw_with_bq:
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

            smart_cleaner_local = BashOperator(
                task_id = 'smart_cleaner_local',
                bash_command = """
                    gsutil mv {{ params.path_campaigns }} {{ params.target }} && 
                    gsutil mv {{ params.path_transactions }} {{ params.target }}         
                """,        
                params = {
                    'path_campaigns': PATHS['campaigns_csv'],
                    'path_transactions': PATHS['transactions_csv'],
                    'target': PATHS['loaded_local']
                }
            )

            [campaigns_ingested_sensor, transactions_ingested_sensor] >> starting_loading_to_raw 
            
            starting_loading_to_raw >> [load_campaign, load_transaction] >> smart_cleaner_local
        
        ingest_from_api_local >> load_raw_with_bq


    with TaskGroup('api_global') as api_global:
        with TaskGroup('ingest_from_api_global') as ingest_from_api_global:
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

            [ingest_campaigns_txt , ingest_transactions_txt]
        
        with TaskGroup('load_raw_with_spark') as load_raw_with_spark:

            campaigns_ingested_sensor = GCSObjectsWithPrefixExistenceSensor(
                    task_id='campaigns_ingested_sensor',
                    bucket='dev-ronny-datalake-raw',
                    prefix='ingested/txt/campaigns_{{ ds_nodash }}_',
                    google_cloud_conn_id='google_cloud_default',
                    timeout = 15
                )    

            transactions_ingested_sensor = GCSObjectsWithPrefixExistenceSensor(
                task_id='transactions_ingested_sensor',
                bucket='dev-ronny-datalake-raw',
                prefix='ingested/txt/transactions_{{ ds_nodash }}_',
                google_cloud_conn_id='google_cloud_default',
                timeout = 15
            )

            create_cluster = DataprocCreateClusterOperator(
                task_id="create_cluster",
                project_id=PROJECT_ID,
                cluster_config=CLUSTER_CONFIG,
                region=REGION,
                cluster_name=CLUSTER_NAME,
            )

            pyspark_task = DataprocSubmitJobOperator(
                task_id="pyspark_task",
                job=PYSPARK_JOB,
                region=REGION,
                project_id=PROJECT_ID
            )

            delete_cluster = DataprocDeleteClusterOperator(
                task_id="delete_cluster",
                project_id=PROJECT_ID,
                cluster_name=CLUSTER_NAME,
                region=REGION
            )

            smart_cleaner_global = BashOperator(
                task_id = 'smart_cleaner_global',
                bash_command = """
                    gsutil mv {{ params.path_campaigns }} {{ params.target }} && 
                    gsutil mv {{ params.path_transactions }} {{ params.target }}         
                """,        
                params = {
                    'path_campaigns': PATHS['campaigns_txt'],
                    'path_transactions': PATHS['transactions_txt'],
                    'target': PATHS['loaded_global']
                }
            )

            [campaigns_ingested_sensor, transactions_ingested_sensor] >> create_cluster >> pyspark_task

            pyspark_task >> delete_cluster >> smart_cleaner_global

        ingest_from_api_global >> load_raw_with_spark

    with TaskGroup('database_sql') as database_sql:
        with TaskGroup('ingest_from_database_sql') as ingest_from_database_sql:
            sales_export_task = CloudSqlInstanceExportOperator(
                task_id = 'sales_export_task',
                project_id = PROJECT_ID, 
                body = EXPORT_BODY, 
                instance = INSTANCE_NAME                
            )
            sales_export_task

        with TaskGroup('load_raw_with_bq') as load_raw_with_bq:
            sales_ingested_sensor = GCSObjectsWithPrefixExistenceSensor(
                task_id='users_ingested_sensor',
                bucket='dev-ronny-datalake-raw',
                prefix='ingested/cloud_sql/sales_{{ ds_nodash }}_',
                google_cloud_conn_id='google_cloud_default',
                timeout = 15
            )

            load_sales = GoogleCloudStorageToBigQueryOperator(
                task_id = "load_sales",
                bucket = 'dev-ronny-datalake-raw',
                source_objects = ['ingested/cloud_sql/sales_*.csv'],
                destination_project_dataset_table   ='raw_layer.r_sales',
                schema_fields=[
                    {'name': 'transaction_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'product_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'date_time', 'type': 'STRING', 'mode': 'NULLABLE'},
                ],
                create_disposition = 'CREATE_IF_NEEDED',
                write_disposition = 'WRITE_APPEND'
            )

            smart_cleaner_database_sql = BashOperator(
                task_id = 'smart_cleaner_database_sql',
                bash_command = """
                    gsutil mv {{ params.path_users }} {{ params.target }}        
                """,        
                params = {
                    'path_users': PATHS['users_csv'],
                    'target': PATHS['loaded_cloud_sql']
                }
            )

            sales_ingested_sensor >> load_sales >> smart_cleaner_database_sql
        
        ingest_from_database_sql >> load_raw_with_bq

    start_loading_master = DummyOperator(
        task_id = 'start_loading_master'
    )

    load_master = BigQueryOperator(
        task_id = "load_master",
        sql = QUERY_MASTER,
        destination_dataset_table = 'master_layer.m_performance_model',
        write_disposition = 'WRITE_APPEND',
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False,
        priority = 'BATCH'
    )

    start_loading_business = DummyOperator(
        task_id = 'start_loading_business'
    )

    load_business_performance_metrics = BigQueryOperator(
        task_id = "load_business_performance_metrics",
        sql = QUERY_BUSINESS_PERFORMANCE_METRICS,
        destination_dataset_table = 'business_layer.b_performance_metrics',
        write_disposition = 'WRITE_APPEND',
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False,
        priority = 'BATCH'
    )

    load_business_product_performance = BigQueryOperator(
        task_id = "load_business_product_performance",
        sql = QUERY_BUSINESS_PRODUCT_PERFORMANCE,
        destination_dataset_table = 'business_layer.product_performance',
        write_disposition = 'WRITE_APPEND',
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False,
        priority = 'BATCH'
    )

    end_datapipeline = DummyOperator(
        task_id = 'end_datapipeline'
    )


    
    start_datapipeline >> [api_local, api_global, database_sql] >> start_loading_master >> load_master

    load_master >> start_loading_business >> [load_business_performance_metrics, load_business_product_performance] >> end_datapipeline

