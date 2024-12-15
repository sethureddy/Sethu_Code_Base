from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.utils import trigger_rule
# from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor, GoogleCloudStorageObjectUpdatedSensor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator

from scripts.universal_profile.privacy_request.stage_2_prod import get_all_table_names_having_pii

from airflow.operators.email_operator import EmailOperator


# run_config = None
# YESTERDAY = datetime.now() - timedelta(days=1)

# defining default arguments
default_args = {
    'owner': 'UP',
    # 'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': False,
    'email_on_retry': False,
    'sla': timedelta(hours=1, minutes=10, seconds=5),
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

get_all_table_names_having_pii_sql = get_all_table_names_having_pii()


# Naming the DAG and passing the default arguments from default_args
with DAG('privacy_request_prod', schedule_interval=None, default_args=default_args) as dag: 
    task_get_all_table_names_having_pii = bigquery_operator.BigQueryOperator(
        task_id='get_all_table_names_having_pii',
        sql=get_all_table_names_having_pii_sql,
        use_legacy_sql=False
    )
    
    # task_create_pii_stage_table = bash_operator.BashOperator(
        # task_id='pii_stage_table_creation',
        # bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/pii_table_creation.py",
        # dag=dag
        # # destination_dataset_table=bq_dataset_name1
    # )

    task_identify_tables_having_data_prod = bash_operator.BashOperator(
        task_id='identify_tables_having_data_prod',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/identify_tables_having_data_prod.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
    
    task_newly_cretaed_tables_not_identified_in_prod = bash_operator.BashOperator(
        task_id='newly_created_tables_not_identified_in_prod',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/newly_created_tables_not_identified_in_prod.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
    
    task_identify_problematic_ip = bash_operator.BashOperator(
        task_id='identify_problematic_ip',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/identify_problematic_ip_address.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
    task_identify_problematic_guid = bash_operator.BashOperator(
        task_id='identify_problematic_guid',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/identify_problematic_guid.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
    task_update_query_1 = bash_operator.BashOperator(
        task_id='update_query_1',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/update_query_1.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
    task_update_query_2_1 = bash_operator.BashOperator(
        task_id='update_query_2_1',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/update_query_2_1.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
    task_update_query_2_2 = bash_operator.BashOperator(
        task_id='update_query_2_2',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/update_query_2_2.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
    task_update_query_3 = bash_operator.BashOperator(
        task_id='update_query_3',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/privacy_request/update_query_3.py",
        dag=dag
        # destination_dataset_table=bq_dataset_name1
    )
	
task_get_all_table_names_having_pii>>task_newly_cretaed_tables_not_identified_in_prod>>task_identify_tables_having_data_prod
task_identify_tables_having_data_prod>>[task_identify_problematic_guid,task_identify_problematic_ip] 
[task_identify_problematic_guid,task_identify_problematic_ip]>> task_update_query_1
task_update_query_1>>[ task_update_query_2_1,task_update_query_2_2]
[task_update_query_2_1,task_update_query_2_2]>> task_update_query_3


 