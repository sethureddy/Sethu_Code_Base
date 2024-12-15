from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor, GoogleCloudStorageObjectUpdatedSensor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.models import Variable

# getting the dev credentials that are stored securely in the airflow server
o_project = Variable.get("project_id")
o_stg_dataset = Variable.get("stg_dataset")
o_warehouse_dataset = Variable.get("gcp_project_warehouse_dataset")

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
# Naming the DAG and passing the default arguments from default_args
with DAG('daily_currency_exchange_rate_prod_dag', schedule_interval='0 11 * * *', default_args=default_args) as dag:
    task1 = bash_operator.BashOperator(
        task_id='daily_currency_exchange_rate_TO_GBQ',
        # bigquery_conn_id = 'google_cloud_default',
        bash_command="source /home/airflow/gcs/dags/venvs/activate_cdw_ora_to_bq_venv.sh && python3 /home/airflow/gcs/dags/scripts/universal_profile/daily_currency_exchange_rate.py",
        dag=dag)

task2 = bigquery_operator.BigQueryOperator(
    task_id='delete_daily_currency_exchange_rate_stage_table',
    sql='''drop table if exists {0}.{1}.daily_currency_exchange_rate'''.format(o_project, o_stg_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)

task3 = bigquery_operator.BigQueryOperator(
    task_id='insert_into_daily_currency_exchange_rate_final_table',
    sql='''create table if not exists `{0}.{2}.daily_currency_exchange_rate`
               (	
                conversion_day DATE
                ,currency_code_from	STRING
                ,currency_code_to	STRING
                ,conversion_rate	NUMERIC
                ,last_oracle_change_date	DATETIME
                ,create_date_utc TIMESTAMP
                ,last_update_date_utc TIMESTAMP
               );	

                INSERT INTO
               `{0}.{2}.daily_currency_exchange_rate`
                (conversion_day
                ,currency_code_from	
                ,currency_code_to	
                ,conversion_rate 
                ,last_oracle_change_date 
                ,create_date_utc 
                ,last_update_date_utc
                )
               SELECT 
               cast(conversion_day as DATE) conversion_day
               ,cast(currency_code_from as STRING) currency_code_from
               ,cast(currency_code_to as STRING) currency_code_to
               ,cast(conversion_rate as NUMERIC) conversion_rate
               ,cast(last_oracle_change_date as DATETIME) last_oracle_change_date
               ,CURRENT_TIMESTAMP AS create_date_utc
               ,CURRENT_TIMESTAMP AS last_update_date_utc  
               from `{0}.{1}.daily_currency_exchange_rate`
               ;'''.format(o_project, o_stg_dataset, o_warehouse_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)
# Setting up the dependencies for the tasks
task2 >> task1 >> task3