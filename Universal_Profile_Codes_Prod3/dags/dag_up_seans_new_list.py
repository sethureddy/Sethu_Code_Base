from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator

from scripts.universal_profile.up_seans_new_list import up_seans_new_list


# defining default arguments
default_args = {
    'owner': 'UP',
    'start_date': datetime(2022,3,31),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': True,
    'catchup': False
}

up_seans_new_list_sql = up_seans_new_list()

# Schedule interval- Everyday 9:00 PM IST

# Naming the DAG and passing the default arguments from default_args
with DAG('up_seans_new_list', schedule_interval= '30 15 1 * *', default_args=default_args) as dag: 
    task_up_seans_new_list_sql = bigquery_operator.BigQueryOperator(
        task_id='up_seans_new_list_sql',
        sql=up_seans_new_list_sql,
        use_legacy_sql=False
    )

task_up_seans_new_list_sql