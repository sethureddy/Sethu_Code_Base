from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.utils import trigger_rule
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import BaseOperator
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator


# defining default arguments
default_args = {
    'owner': 'UP',
    # 'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': True,
    'sla': timedelta(hours=1, minutes=10, seconds=5),
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Naming the DAG and passing the default arguments from default_args
with DAG('diamond_data_status_check_dag', schedule_interval=None, default_args=default_args) as dag:

    task_diamond_data_status_check_for_gbq = bash_operator.BashOperator(
		task_id = 'diamond_data_status_check_for_gbq',
		dag=dag,
		bash_command = "python3 /home/airflow/gcs/dags/scripts/universal_profile/diamond_data_status_check_for_gbq.py"
    )
    
def trigger(context, dag_run_obj):
    dag_run_obj.payload = {'message': context['params']['message']}
    return dag_run_obj

start_recommendation_model_deploy = TriggerDagRunOperator(
    dag=dag,
    task_id='start_recommendation_set_1',
    trigger_dag_id="recommendation_model_deploy",
    # python_callable=trigger,
    params={'message': 'Hello World'}
    )
start_recommendation_model_set_2 = TriggerDagRunOperator(
    dag=dag,
    task_id='start_recommendation_set_2',
    trigger_dag_id="recommendation_model_set2",
    # python_callable=trigger,
    params={'message': 'Hello World'}
    )
start_recommendation_model_set_3 = TriggerDagRunOperator(
    dag=dag,
    task_id='start_recommendation_set_3',
    trigger_dag_id="recommendation_model_set3",
    # python_callable=trigger,
    params={'message': 'Hello World'}
    )
start_cross_recommendation_model = TriggerDagRunOperator(
    dag=dag,
    task_id='cross_reco_setup_conf_files',
    trigger_dag_id="cross_category_recommendation",
    # python_callable=trigger,
    params={'message': 'Hello World'}
    )
    
task_diamond_data_status_check_for_gbq >>[start_recommendation_model_deploy, start_recommendation_model_set_2, start_recommendation_model_set_3, start_cross_recommendation_model]
