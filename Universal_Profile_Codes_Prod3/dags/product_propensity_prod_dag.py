from datetime import datetime, timedelta
import airflow
from airflow.models import Variable
from airflow import models

from airflow.operators import email_operator
from airflow.contrib.operators import bigquery_operator

from scripts.universal_profile.product_propensity import product_propensity

o_project = Variable.get("project_id")
o_stg_customer = Variable.get("stg_customer")

default_dag_args = {
    'owner':'UP',
    'start_date': datetime(2022,3,17),
    'email': models.Variable.get('email'),
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

output_table_name1 = "o_customer.bn_product_propensity"

sql_query1 = product_propensity(params=params_query)

with models.DAG(
        'product_propensity_prod_dag',
        schedule_interval='35 22 18 * *',
        default_args=default_dag_args) as dag:

    task_check_and_drop_table_product_propensity = bigquery_operator.BigQueryOperator(
        task_id='task_check_and_drop_table_product_propensity',
        sql=f'''drop table if exists {output_table_name1}'''.format(output_table_name1),
        use_legacy_sql=False
    )

    task_product_propensity = bigquery_operator.BigQueryOperator(
        task_id='task_product_propensity',
        sql= sql_query1,
        use_legacy_sql=False,
        destination_dataset_table=output_table_name1
    )

    (
         task_check_and_drop_table_product_propensity >> task_product_propensity
    )
