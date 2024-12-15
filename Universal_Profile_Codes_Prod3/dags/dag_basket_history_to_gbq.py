from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.models import Variable

# getting the dev credentials that are stored securely in the airflow server
o_project = Variable.get("project_id")
o_stg_dataset = Variable.get("stg_dataset")
o_tgt_dataset = Variable.get("tgt_dataset")

# defining default arguments
default_args = {
    'owner': 'UP',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['undela.reddy@affineanalytics.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# Naming the DAG and passing the default arguments from default_args
with DAG('up_basket_history_to_gbq', catchup=False, schedule_interval='30 02,12,17,22 * * *', default_args=default_args) as dag:
    task_basket_history = bigquery_operator.BigQueryOperator(
        task_id='insert_into_basket_history',
        sql='''insert into bnile-cdw-prod.dssprod_o_warehouse.basket_history
        select * from `bnile-cdw-prod.nileprod_o_warehouse.basket` where

        PARSE_TIMESTAMP("%Y-%m-%d %H",substring(cast(data_capture_timestamp_utc as string),1,13)) 

        in (select PARSE_TIMESTAMP("%Y-%m-%d %H",(substring(cast((SELECT max(data_capture_timestamp_utc ) 

        FROM `bnile-cdw-prod.nileprod_o_warehouse.basket`) as string), 1,13))) )''',
        use_legacy_sql=False,
        dag=dag)

    task_basket_item_history = bigquery_operator.BigQueryOperator(
        task_id='insert_into_basket_item_history',
        sql='''insert into bnile-cdw-prod.dssprod_o_warehouse.basket_item_history
        select * from `bnile-cdw-prod.nileprod_o_warehouse.basket_item` where

        PARSE_TIMESTAMP("%Y-%m-%d %H",substring(cast(data_capture_timestamp_utc as string),1,13)) 

        in (select PARSE_TIMESTAMP("%Y-%m-%d %H",(substring(cast((SELECT max(data_capture_timestamp_utc ) 

        FROM `bnile-cdw-prod.nileprod_o_warehouse.basket_item`) as string), 1,13))) )''',
        use_legacy_sql=False,
        dag=dag)        
        

# Setting up the dependencies for the tasks

[task_basket_history,task_basket_item_history]