from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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

#getting the dev credentials that are stored securely in the airflow server
o_project=Variable.get("project_id")
o_stg_dataset=Variable.get("stg_dataset")
o_tgt_dataset=Variable.get("tgt_dataset")


#run_config = None
#YESTERDAY = datetime.now() - timedelta(days=1)

#defining default arguments
default_args = {
    'owner': 'UP',
    #'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup':False
}
#Naming the DAG and passing the default arguments from default_args
with DAG('bn_guid_dim_cf_to_gbq', schedule_interval='17 10 * * *', default_args=default_args) as dag:
	
    task1 =  bash_operator.BashOperator(
		task_id = 'guid_dim_cf_TO_GBQ',
		#bigquery_conn_id = 'google_cloud_default',
		bash_command="source /home/airflow/gcs/dags/venvs/activate_cdw_ora_to_bq_venv.sh && python3 /home/airflow/gcs/dags/scripts/universal_profile/guid_dim_cf_pipeline.py",
		dag=dag) 
        
task2 =  bigquery_operator.BigQueryOperator(
		task_id = 'delete_guid_dim_cf_stage_table',
        sql='''drop table if exists {0}.{1}.guid_dim_cf'''.format(o_project, o_stg_dataset),
        use_legacy_sql=False,
        #write_disposition='WRITE_APPEND',
        #create_disposition='CREATE_IF_NEEDED',
		dag=dag)
        
task3 =  bigquery_operator.BigQueryOperator(
		task_id = 'insert_into_guid_dim_cf_final_table',
        sql='''create table if not exists `{0}.{2}.guid_dim_cf`
               (	
                 GUID_KEY	int64
				,GUID	String
                ,DATE_KEY	int64
                );
                	
               
               delete from  `{0}.{2}.guid_dim_cf` where date_key in (select distinct date_key from `{0}.{1}.guid_dim_cf`);
               
               INSERT INTO
               `{0}.{2}.guid_dim_cf` 
                          
               (
               select 
               cast(GUID_KEY as int64) GUID_KEY
			   ,cast(GUID as String) GUID
               ,cast(DATE_KEY as int64) DATE_KEY
                          
               from `{0}.{1}.guid_dim_cf`
               )'''.format(o_project, o_stg_dataset, o_tgt_dataset),
        use_legacy_sql=False,
        #write_disposition='WRITE_APPEND',
        #create_disposition='CREATE_IF_NEEDED',
		dag=dag)
# Setting up the dependencies for the tasks        
task2>>task1>>task3


       