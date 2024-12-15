from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.utils.dates import days_ago
from scripts.universal_profile.up_engagement_vip import up_engagement_vip
from scripts.universal_profile.up_jewelry_vip import up_jewelry_vip

# defining default arguments
default_args = {
    'owner': 'UP',
    'start_date': datetime(2022,2,13),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup': False
}

up_engagement_vip_sql = up_engagement_vip()
up_jewelry_vip_sql = up_jewelry_vip()
# Schedule interval- Everyday 9:00 PM IST

# Naming the DAG and passing the default arguments from default_args
with DAG('up_engagement_and_jewelry_vip', schedule_interval= '30 15 * * mon', default_args=default_args) as dag: 
    task_up_engagement_vip_sql = bigquery_operator.BigQueryOperator(
        task_id='up_engagement_vip_sql',
        sql=up_engagement_vip_sql,
        use_legacy_sql=False
    )
    task_up_jewelry_vip_sql = bigquery_operator.BigQueryOperator(
        task_id='up_jewelry_vip_sql',
        sql=up_jewelry_vip_sql,
        use_legacy_sql=False
    )

task_up_engagement_vip_sql>>task_up_jewelry_vip_sql