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

from scripts.universal_profile.product_feeds import basketaudienceview,lookup_BasketItemTable


# yesterday = datetime.datetime.combine(
#     datetime.datetime.today() - datetime.timedelta(0),
#     datetime.datetime.min.time())


default_dag_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'email': models.Variable.get('Marketing_DL','Affineetl_DL'),
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

params_query = {
    "project_name": "bnile-cdw-prod",
}

output_table_name1 = "o_mkt_audience.abandoned_baskets_audience"
output_table_name2 = "o_mkt_audience.lookup_basket_item_table"


sql_query1 = basketaudienceview(params=params_query)
sql_query2 = lookup_BasketItemTable(params=params_query)

with models.DAG(
        'product_feeds_view_prod_dag',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    task_check_and_drop_table = bigquery_operator.BigQueryOperator(
        task_id='task_check_and_drop_table',
        sql=f'''drop table if exists {output_table_name1};drop table if exists {output_table_name2}'''.format(output_table_name1,output_table_name2),
        use_legacy_sql=False
    )


    task_basketaudienceview = bigquery_operator.BigQueryOperator(
        task_id='task_basketaudienceview',
        sql= sql_query1,
        use_legacy_sql=False,
        destination_dataset_table=output_table_name1
    )

    task_lookup_BasketItemTable = bigquery_operator.BigQueryOperator(
        task_id='task_lookup_BasketItemTable',
        sql= sql_query2,
        use_legacy_sql=False,
        destination_dataset_table=output_table_name2
    )


    task_check_and_drop_table >> task_basketaudienceview
    task_check_and_drop_table >> task_lookup_BasketItemTable


#added lines 77-87
#def run_this_func(*args, **kwargs):
#    print("Remotely received a message: {}".
#          format(kwargs['dag_run'].conf['message']))
#
#run_this = PythonOperator(
#    task_id='task_check_and_drop_table',
#    python_callable=run_this_func,
#    provide_context=True,
#    dag=dag,)