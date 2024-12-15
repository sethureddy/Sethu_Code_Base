from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.utils import trigger_rule
#from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.operators import bigquery_operator

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator

from airflow.operators.email_operator import EmailOperator


#importing the sql statements to process the stage_pii_process

from scripts.universal_profile.stage_pii_sql_stmts import stage_pii_sql_stmts

#defining default arguments
default_args = {
    'owner': 'UP',
    #'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': True,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

stage_pii_sql_stmts_sql=stage_pii_sql_stmts()

#Naming the DAG and passing the default arguments from default_args
with DAG('up_stage_pii_process', schedule_interval='0 9 * * *', default_args=default_args) as dag:

    #start_master_up_process = DummyOperator(task_id="start_master_up_process", dag=dag)
    # link = DummyOperator(task_id="link", dag=dag)
    
    task_explicit_process = bigquery_operator.BigQueryOperator(
        task_id='stage_pii_process',
        sql= stage_pii_sql_stmts_sql,
        #bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql=False,
        #destination_dataset_table=bq_dataset_name1
        )