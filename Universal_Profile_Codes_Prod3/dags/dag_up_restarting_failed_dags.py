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
with DAG('up_restarting_failed_dags', schedule_interval= '15 12 * * *', default_args=default_args) as dag:

    task_restarting_recom_model_1 = bash_operator.BashOperator(
		task_id = 'restarting_recom_model_1',
		dag=dag,
		bash_command = "python3 /home/airflow/gcs/dags/scripts/universal_profile/restarting_recom_model_1.py"
    )
    task_restarting_recom_model_2 = bash_operator.BashOperator(
		task_id = 'restarting_recom_model_2',
		dag=dag,
		bash_command = "python3 /home/airflow/gcs/dags/scripts/universal_profile/restarting_recom_model_2.py"
    )
    task_restarting_recom_model_3 = bash_operator.BashOperator(
		task_id = 'restarting_recom_model_3',
		dag=dag,
		bash_command = "python3 /home/airflow/gcs/dags/scripts/universal_profile/restarting_recom_model_3.py"
    )
    task_restarting_cross_category_model = bash_operator.BashOperator(
		task_id = 'restarting_cross_category_model',
		dag=dag,
		bash_command = "python3 /home/airflow/gcs/dags/scripts/universal_profile/restarting_cross_category_model.py"
    )

[task_restarting_recom_model_1,task_restarting_recom_model_2,task_restarting_recom_model_3,task_restarting_cross_category_model]