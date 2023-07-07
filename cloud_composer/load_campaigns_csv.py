from airflow import DAG
from airflow import models
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

QUERY = """
WITH tranx AS (
    SELECT
        *,
        SUBSTR(datetime, 1, 10) AS date,
        PARSE_TIME('%T', SUBSTR(datetime, 12)) AS time
    FROM
        ronny-dev-airflow.raw_layer.r_transactions
),
tranx_agg AS (
    SELECT
        country,
        SUM(CAST(income as FLOAT64)) as income,
        COUNT(id) as transaction_count,
        0.00 cost,
        0 campaign_count,
        date as date
    FROM
        tranx
    GROUP BY
        1,
        6
),
campaigns AS (
    SELECT
        *,
        SUBSTR(datetime, 1, 10) AS date,
        PARSE_TIME('%T', SUBSTR(datetime, 12)) AS time
    FROM
        ronny-dev-airflow.raw_layer.r_campaigns
),
campaigns_agg AS (
    SELECT
        pais as country,
        0.00 income,
        0 transaction_count,
        SUM(CAST(cost as FLOAT64)) as cost,
        COUNT(campaign_id) as campaign_count,
        date as date
    FROM
        campaigns
    GROUP BY
        1,
        6
),
unionall AS (
    SELECT
        *
    FROM
        tranx_agg
    UNION
    ALL
    SELECT
        *
    FROM
        campaigns_agg
),
unionall_agg AS (
    SELECT
        country,
        SUM(income) income,
        SUM(transaction_count) transaction_count,
        SUM(cost) cost,
        SUM(campaign_count) campaign_count,
        date
    FROM
        unionall
    GROUP BY
        1,
        6
)
SELECT
    country,
    transaction_count,
    income,
    cost,
    (income - cost) as revenue,
    campaign_count,
    date
FROM
    unionall_agg
"""

#PATH_CMP = 'gs://dev-ronny-datalake-raw/results/campanas_*.csv'
PATH_CMP = models.Variable.get('path_cmp')
#target = 'gs://dev-ronny-datalake-raw/loaded/'
TARGET = models.Variable.get('target')

DEFAULT_ARGS = {
    'owner': 'Ronny',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(

    'load_campaigns_csv',
    default_args=DEFAULT_ARGS,
    description='Data Pipeline with Cloud Function',
    catchup=False,
    start_date=datetime(2023, 7, 1),
    schedule_interval='@once',
    tags=['ingest', 'csv', 'bigquery']

) as dag:
    
    load_raw_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id = "load_raw_bigquery",
    bucket = 'dev-ronny-datalake-raw',
    source_objects = ['results/campaigns_*.csv'],    
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

    smart_cleaner = BashOperator(
        task_id = 'smart_cleaner',
        bash_command = """
            gsutil mv {{ params.path_cmp }} {{ params.target }}         
        """,        
        params = {
            'path_cmp': PATH_CMP,
            'target': TARGET
        }
    )

    load_raw_bigquery >> smart_cleaner