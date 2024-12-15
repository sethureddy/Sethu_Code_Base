from datetime import timedelta, datetime
import time
import airflow
#addes lines 4-7
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow import models

from airflow.operators import bash_operator
from airflow.operators import email_operator
from airflow.utils import trigger_rule

from airflow.contrib.operators import bigquery_operator

from scripts.universal_profile.bn_recommendations_json import bn_recommendations_json


# yesterday = datetime.datetime.combine(
#     datetime.datetime.today() - datetime.timedelta(0),
#     datetime.datetime.min.time())


default_dag_args = {
    'owner':'UP',
    'start_date': airflow.utils.dates.days_ago(0),
    'email': 'rohit.varma@affine.ai',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

params_query = {
    "project_name": "bnile-cdw-prod",
}

output_table_name1 = "o_customer.bn_recommendations_json"

sql_query1 = bn_recommendations_json(params=params_query)

with models.DAG(
        'bn_recommendations_json_prod_dag',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    task_check_and_drop_table_reco_json = bigquery_operator.BigQueryOperator(
        task_id='task_check_and_drop_table_reco_json',
        sql=f'''drop table if exists {output_table_name1}'''.format(output_table_name1),
        use_legacy_sql=False
    )


    task_bn_recommendations_json = bigquery_operator.BigQueryOperator(
        task_id='task_bn_recommendations_json',
        sql= sql_query1,
        use_legacy_sql=False,
        destination_dataset_table=output_table_name1
    )

    delay_python_task = PythonOperator(task_id="delay_python_task",
                                                       dag=dag,
                                                       python_callable=lambda: time.sleep(600))

    (
        delay_python_task >> task_check_and_drop_table_reco_json >> task_bn_recommendations_json
    )


# #added lines 77-87
# def run_this_func(*args, **kwargs):
#     print("Remotely received a message: {}".
#           format(kwargs['dag_run'].conf['message']))
#
# run_this = PythonOperator(
#     task_id='task_check_and_drop_table',
#     python_callable=run_this_func,
#     provide_context=True,
#     dag=dag,)