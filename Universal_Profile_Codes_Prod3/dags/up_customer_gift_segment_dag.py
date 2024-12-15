from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator

from scripts.universal_profile.up_customer_gift_segment.up_customer_gift_segment_sql import up_customer_gift_segment
from scripts.universal_profile.up_customer_gift_segment.bq_conf import QUERY_PARAMS


# defining default arguments
default_args = {
    'owner': 'UP',
    #'start_date': airflow.utils.dates.days_ago(1),
    'start_date': datetime(2021,12,19),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup':False
}

up_customer_gift_segment_sql = up_customer_gift_segment(QUERY_PARAMS)

# Schedule interval for weekly basis---- '0 15 * * 1'

# Naming the DAG and passing the default arguments from default_args
with DAG('up_customer_gift_segment', schedule_interval= '0 15 * * 1', default_args=default_args) as dag: 
    task_up_customer_gift_segment_sql = bigquery_operator.BigQueryOperator(
        task_id='up_customer_gift_segment_sql',
        sql=up_customer_gift_segment_sql,
        use_legacy_sql=False
    )

task_up_customer_gift_segment_sql