from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator

from scripts.universal_profile.up_customer_product import up_customer_product


# defining default arguments
default_args = {
    'owner': 'UP',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup': True
}

up_customer_product_sql = up_customer_product()

# Schedule interval- Everyday 9:00 PM IST

# Naming the DAG and passing the default arguments from default_args
with DAG('up_customer_product', schedule_interval= '30 02,12,17,22 * * *', default_args=default_args) as dag: 
    task_up_customer_product_sql = bigquery_operator.BigQueryOperator(
        task_id='up_customer_product_sql',
        sql=up_customer_product_sql,
        use_legacy_sql=False
    )

task_up_customer_product_sql