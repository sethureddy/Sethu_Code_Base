from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.utils import trigger_rule
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import bigquery_operator
from airflow.models import BaseOperator
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator

from scripts.universal_profile.bn_up_dashboard_codes import bn_up_dashboard_codes

# run_config = None
# YESTERDAY = datetime.now() - timedelta(days=1)

# defining default arguments
default_args = {
    'owner': 'UP',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

bn_up_dashboard_codes_sql = bn_up_dashboard_codes()

# Naming the DAG and passing the default arguments from default_args
with DAG('bn_up_dashboard_codes', schedule_interval='30 14 * * *', default_args=default_args) as dag:


    task_bn_up_dashbord_codes = bigquery_operator.BigQueryOperator(
        task_id='bn_up_dashboard_codes',
        sql=bn_up_dashboard_codes_sql,
        use_legacy_sql=False,
        # destination_dataset_table=bq_dataset_name1
    )