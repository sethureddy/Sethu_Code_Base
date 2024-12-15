from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.utils import trigger_rule
#from airflow.operators.sensors import ExternalTaskSensor
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

from scripts.universal_profile.explicit_process import explicit_process
from scripts.universal_profile.bn_customer_explicit_process import bn_customer_explicit_process
from airflow.operators.email_operator import EmailOperator

from scripts.universal_profile.pj_rfm import pj_rfm

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

explicit_process_sql=explicit_process()
bn_customer_explicit_process_sql=bn_customer_explicit_process()
pj_rfm_sql=pj_rfm()
# bn_sc_pj_attributes_sql=bn_sc_pj_attributes()
# bn_lc_pj_attributes_sql=bn_lc_pj_attributes()

#Naming the DAG and passing the default arguments from default_args
with DAG('bn_master_up_process', schedule_interval=None, default_args=default_args) as dag:

    #start_master_up_process = DummyOperator(task_id="start_master_up_process", dag=dag)
    # link = DummyOperator(task_id="link", dag=dag)
    
    task_explicit_process = bigquery_operator.BigQueryOperator(
        task_id='explicit_process',
        sql= explicit_process_sql,
        #bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql=False,
        #destination_dataset_table=bq_dataset_name1
        )

    task_bn_customer_explicit_process = bigquery_operator.BigQueryOperator(
        task_id='start_bn_customer_explicit_process',
        sql= bn_customer_explicit_process_sql,
        #bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql=False,
        #destination_dataset_table=bq_dataset_name1
        )
        
    task_pj_rfm_process = bigquery_operator.BigQueryOperator(
        task_id='start_bn_pj_rfm_process',
        sql= pj_rfm_sql,
        #bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql=False,
        #destination_dataset_table=bq_dataset_name1
        )


def trigger(context, dag_run_obj):
    dag_run_obj.payload = {'message': context['params']['message']}
    return dag_run_obj

start_short_consideration_propensity_model= TriggerDagRunOperator(
    dag=dag,
    task_id='sc_delete_exist_intermediate_sql_tables',
    trigger_dag_id="short_consideration_propensity_model",
    # python_callable=trigger,
    params={'message': 'Hello World'}
    )
start_long_conversion_propensity_model = TriggerDagRunOperator(
    dag=dag,
    task_id='lc_delete_exist_intermediate_tables',
    trigger_dag_id="long_conversion_propensity_model",
    # python_callable=trigger,
    params={'message': 'Hello World'}
    )
start_diamond_check = TriggerDagRunOperator(
    dag=dag,
    task_id='diamond_data_status_check_for_gbq',
    trigger_dag_id="diamond_data_status_check_dag",
    # python_callable=trigger,
    params={'message': 'Hello World'}
    )

task_email_on_success = EmailOperator(
    task_id="send_mail_on_success", 
    # to='bluenile@affineanalytics.com',
    to='rohit.varma@affine.ai',
    subject='UP ETL Alert:Success(bn_master_up_process)',
    html_content=''' 
    Hi,<br>
    
    <p>This email is to inform you that, master_up_process pipeline has been successfully completed.<br>
    <br>
    master_up_process Which has below subtasks, are also completed successfully<br>
    <br>
    1.up_explicit_process.<br>
    2.bn_customer_and_explicit process.<br>
    3.bn_pj_rfm_process.<br>
    <br>
    And also trigger's following DAG's externally.<br>
    <br>
    1.lc model, sc model,diamond check.
    
           <br>
           <br>
    -<br>
    UP Team.<p>''',
    dag=dag)

# start_master_up_process>>task_explicit_process
# task_explicit_process>>task_bn_customer_explicit_process    
# task_bn_customer_explicit_process>>task_pj_rfm_process
# task_bn_customer_explicit_process>>task_bn_sc_pj_attributes_process
# task_bn_customer_explicit_process>>task_bn_lc_pj_attributes_process
# task_pj_rfm_process>>link
# task_bn_sc_pj_attributes_process>>link
# task_bn_lc_pj_attributes_process>>link
# link>>start_short_consideration_propensity_model
# link>>start_long_conversion_propensity_model
# link>>start_recommendation_model_deploy
# link>>start_email_customer_profile_prod_dag

#start_master_up_process>>task_explicit_process
task_explicit_process>>task_bn_customer_explicit_process    
task_bn_customer_explicit_process>>task_pj_rfm_process
task_pj_rfm_process>>start_short_consideration_propensity_model
task_pj_rfm_process>>start_long_conversion_propensity_model
task_pj_rfm_process>>start_diamond_check
task_pj_rfm_process>>task_email_on_success

# task_pj_rfm_process>>start_recommendation_model_deploy
# task_pj_rfm_process>>start_recommendation_model_set_2
# task_pj_rfm_process>>start_recommendation_model_set_3
# task_pj_rfm_process>>start_cross_recommendation_model