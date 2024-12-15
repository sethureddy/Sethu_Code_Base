from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator


from scripts.universal_profile.up_customer_seasonal_shopper.up_customer_seasonal_shopper_sql import up_customer_seasonal_shopper
from scripts.universal_profile.up_customer_seasonal_shopper.bq_conf import QUERY_PARAMS


# defining default arguments
default_args = {
    'owner': 'UP',
    'start_date': datetime(2021,12,19),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup':False
}

up_customer_seasonal_shopper_sql =  up_customer_seasonal_shopper(QUERY_PARAMS)

# Schedule interval for weekly basis each monday 

# Naming the DAG and passing the default arguments from default_args
with DAG('up_customer_seasonal_shopper', schedule_interval= '10 15 * * 1', default_args=default_args) as dag: 
    up_customer_seasonal_shopper_sql = bigquery_operator.BigQueryOperator(
        task_id='up_customer_seasonal_shopper_sql',
        sql=up_customer_seasonal_shopper_sql,
        use_legacy_sql=False
    )

up_customer_seasonal_shopper_sql