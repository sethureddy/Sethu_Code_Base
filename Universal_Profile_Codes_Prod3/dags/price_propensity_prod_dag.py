from datetime import datetime, timedelta
import airflow
from airflow.models import Variable
from airflow import models

from airflow.operators import email_operator
from airflow.contrib.operators import bigquery_operator

from scripts.universal_profile.price_propensity import price_propensity

o_project = Variable.get("project_id")
o_stg_customer = Variable.get("stg_customer")

default_dag_args = {
    'owner':'UP',
    'start_date': datetime(2022,1,25),
    # 'start_date': airflow.utils.dates.days_ago(31),
    'email': 'rohit.varma@affine.ai',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project'),
    'catchup':False
}

params_query = {
    "project_name": o_project,
}

output_table_name1 = "o_customer.bn_customer_price_propensity"

sql_query1 = price_propensity(params=params_query)

with models.DAG(
        'price_propensity_prod_dag',
        schedule_interval='15 22 * * 1',
        default_args=default_dag_args) as dag:

    task_check_and_drop_table_price_propensity = bigquery_operator.BigQueryOperator(
        task_id='task_check_and_drop_table_price_propensity',
        sql=f'''drop table if exists {output_table_name1}'''.format(output_table_name1),
        use_legacy_sql=False
    )

    task_price_propensity = bigquery_operator.BigQueryOperator(
        task_id='task_price_propensity',
        sql= sql_query1,
        use_legacy_sql=False,
        destination_dataset_table=output_table_name1
    )

    (
         task_check_and_drop_table_price_propensity >> task_price_propensity
    )

