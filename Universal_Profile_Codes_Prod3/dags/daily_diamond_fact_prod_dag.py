from datetime import timedelta, datetime
import airflow
from airflow import DAG
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

# getting the dev credentials that are stored securely in the airflow server
o_project = Variable.get("project_id")
o_stg_dataset = Variable.get("stg_dataset")
o_diamond_dataset = Variable.get("diamond_dataset")

# run_config = None
# YESTERDAY = datetime.now() - timedelta(days=1)

# defining default arguments
default_args = {
    'owner': 'UP',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# Naming the DAG and passing the default arguments from default_args
with DAG('bn_daily_diamond_fact_to_gbq', schedule_interval='15 7 * * *', default_args=default_args) as dag:
    task1 = bash_operator.BashOperator(
        task_id='daily_diamond_fact_TO_GBQ',
        # bigquery_conn_id = 'google_cloud_default',
        bash_command="source /home/airflow/gcs/dags/venvs/activate_cdw_ora_to_bq_venv.sh && python3 /home/airflow/gcs/dags/scripts/universal_profile/daily_diamond_fact.py",
        dag=dag)

task2 = bigquery_operator.BigQueryOperator(
    task_id='delete_daily_diamond_fact_stage_table',
    sql='''drop table if exists `{0}.{1}.daily_diamond_fact`'''.format(o_project, o_stg_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)

task3 = bigquery_operator.BigQueryOperator(
    task_id='insert_into_daily_diamond_fact_final_table',
    sql='''create table if not exists `{0}.{2}.daily_diamond_fact` 
               (snapshot_date	datetime
                ,snapshot_date_key	int64
                ,sku string
                ,site   string
                ,first_available_date datetime
                ,sell string
                ,sell_default string
                ,is_three_stone_cntr_compatible numeric
                ,is_pendant_compatible numeric
                ,all_inclusive_price numeric
                ,price numeric
                ,transactional_currency_code string
                ,price_override_mode string
                ,usd_vendor_price numeric
                ,vendor_price_override_mode string
                ,usd_invoice_price numeric
                ,vendor_sku string
                ,memo_to_vendor_id numeric
                ,owned_by_vendor_id numeric
                ,vendor_on_memo string
                ,internal_vendor_notes string
                ,vendor_location_id numeric
                ,purchase_date datetime
                ,purchased_from_vendor_id numeric
                ,promised_basket_id numeric
                ,ships_in_days numeric
                ,supplied_usd_vendor_price numeric
                ,exclusion_reason string
                ,create_date_utc	timestamp
                ,last_update_date_utc	timestamp
               )
               partition by   range_bucket(snapshot_date_key,generate_array(20200101,20211231,02));	
               
               INSERT INTO
               `{0}.{2}.daily_diamond_fact` 

               (
               select *
               from(
               select
               DATETIME(current_timestamp,"America/Los_Angeles") as snapshot_date
               ,cast(replace(cast(date(current_timestamp) as string),'-','') as int64)-1 as snapshot_date_key
               ,cast(sku as  string) sku
                ,cast(site as  string) site
                ,cast(first_available_date as datetime) first_available_date
                ,cast(sell as string) sell
                ,cast(sell_default as string) sell_default
                ,cast(is_three_stone_cntr_compatible as numeric) is_three_stone_cntr_compatible
                ,cast(is_pendant_compatible as numeric) is_pendant_compatible
                ,cast(all_inclusive_price as numeric) all_inclusive_price
                ,cast(price as numeric) price
                ,cast(transactional_currency_code as string) transactional_currency_code
                ,cast(price_override_mode as string) price_override_mode
                ,cast(usd_vendor_price as numeric) usd_vendor_price
                ,cast(vendor_price_override_mode as string) vendor_price_override_mode
                ,cast(usd_invoice_price as numeric) usd_invoice_price
                ,cast(vendor_sku as string) vendor_sku
                ,cast(memo_to_vendor_id as numeric) memo_to_vendor_id
                ,cast(owned_by_vendor_id as numeric) owned_by_vendor_id
                ,cast(vendor_on_memo as string) vendor_on_memo
                ,cast(internal_vendor_notes as string) internal_vendor_notes
                ,cast(vendor_location_id as numeric) vendor_location_id
                ,cast(purchase_date as datetime) purchase_date
                ,cast(purchased_from_vendor_id as numeric) purchased_from_vendor_id
                ,cast(promised_basket_id as numeric) promised_basket_id
                ,cast(ships_in_days as numeric) ships_in_days
                ,cast(supplied_usd_vendor_price as numeric) supplied_usd_vendor_price
                ,cast(exclusion_reason as string) exclusion_reason
               ,current_timestamp create_date_utc
               ,current_timestamp last_update_date_utc
               from `{0}.{1}.daily_diamond_fact`))'''.format(
        o_project, o_stg_dataset, o_diamond_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)
# Setting up the dependencies for the tasks
task2 >> task1 >> task3