from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.utils import trigger_rule
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.operators import bigquery_operator
from airflow.utils.decorators import apply_defaults
from airflow.operators import bash_operator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators import bigquery_operator
from scripts.universal_profile.duplicate_bnid_genearation.bnid_generation_preprocess import bnid_generation_preprocess
from scripts.universal_profile.duplicate_bnid_genearation.bnid_generation_postprocess import bnid_generation_postprocess


# defining default arguments
default_args = {
    'owner': 'UP',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': True,
    'sla': timedelta(hours=1, minutes=10, seconds=5),
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

bnid_generation_preprocess_sql = bnid_generation_preprocess()
bnid_generation_postprocess_sql = bnid_generation_postprocess()

# Naming the DAG and passing the default arguments from default_args
with DAG('duplicate_bnid_generation', schedule_interval=None, default_args=default_args) as dag:

    task_bnid_generation_preprocess = bigquery_operator.BigQueryOperator(
        task_id='bnid_generation_preprocess',
        sql=bnid_generation_preprocess_sql,
        use_legacy_sql=False
    )

    task_check_id_resolution_data = bash_operator.BashOperator(
        task_id='check_id_resolution_data',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/duplicate_bnid_genearation/id_resolution_data_check.py",
        dag=dag
    )
    
    task_merge_id_resolution_data_to_bnid_dedup_map = bigquery_operator.BigQueryOperator(
        task_id='merge_id_resolution_data_to_bnid_dedup_map',
        sql= """create or replace table bnile-cdw-prod.dag_migration_test.bnid_dedup_map_probablistic as 
                select * from bnile-cdw-prod.dag_migration_test.bnid_dedup_map;
                
                insert into bnile-cdw-prod.dag_migration_test.bnid_dedup_map_probablistic
                select primary_bnid		
                ,primary_first_event_date	as p_first_event_date
                ,secondary_bnid	as duplicate_bnid 
                ,secondary_first_event_date		as 	d_first_event_date
                ,Method as rule
                from bnile-cdw-prod.o_customer.id_resolution ;
                 
                create or replace table bnile-cdw-prod.dag_migration_test.bnid_dedup_map_probablistic as
                (select * from bnile-cdw-prod.dag_migration_test.bnid_dedup_map_probablistic
                where rule not in ('payment','email_base2')); """,
        use_legacy_sql=False
    )
    
    task_bnid_generation_id_merge = bash_operator.BashOperator(
        task_id='bnid_generation_id_merge',
        bash_command="python3 /home/airflow/gcs/dags/scripts/universal_profile/duplicate_bnid_genearation/bnid_generation_id_merge.py",
        dag=dag
    )
    
    task_bnid_generation_postprocess = bigquery_operator.BigQueryOperator(
        task_id='bnid_generation_postprocess',
        sql=bnid_generation_postprocess_sql,
        sla=timedelta(hours=1, minutes=10, seconds=5),
        use_legacy_sql=False
    )

task_bnid_generation_preprocess >> task_check_id_resolution_data >> task_merge_id_resolution_data_to_bnid_dedup_map >> task_bnid_generation_id_merge >> task_bnid_generation_postprocess


def trigger(context, dag_run_obj):
    dag_run_obj.payload = {'message': context['params']['message']}
    return dag_run_obj


start_bn_master_up_process = TriggerDagRunOperator(
    dag=dag,
    task_id='explicit_process',
    trigger_dag_id="duplicate_master_up_process",
    # python_callable=trigger,
    params={'message': 'Hello World'}
)

# unknown_users_trigger = TriggerDagRunOperator(
    # dag=dag,
    # task_id='task_check_and_drop_table_intermediate_unknown_users',
    # trigger_dag_id="unknown_super_users",
    # python_callable=trigger,
    # params={'message': 'Hello World'}
    # )
    
# task_email_on_success = EmailOperator(
    # task_id="send_mail_on_success", 
    # #to='bluenile@affineanalytics.com',
	# to='rohit.varma@affine.ai',
    # subject='UP ETL Alert:Success(bn_up_bnid_generation)',
    # html_content=''' 
    # Hi,<br>
    
    # <p>This email is to inform you that, bnid_generation pipeline has been successfully completed.<br>
           # <br>
    # -<br>
    # UP Team.<p>''',
    # dag=dag)

task_bnid_generation_postprocess >> start_bn_master_up_process
# task_bnid_generation_postprocess >> unknown_users_trigger
# task_bnid_generation_postprocess >> task_email_on_success