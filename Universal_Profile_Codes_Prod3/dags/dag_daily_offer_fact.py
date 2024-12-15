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
o_tgt_dataset = Variable.get("tgt_dataset")

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
with DAG('bn_daily_offer_fact_to_gbq', schedule_interval='0 11 * * *', default_args=default_args) as dag:
    task1 = bash_operator.BashOperator(
        task_id='daily_offer_fact_TO_GBQ',
        # bigquery_conn_id = 'google_cloud_default',
        bash_command="source /home/airflow/gcs/dags/venvs/activate_cdw_ora_to_bq_venv.sh && python3 /home/airflow/gcs/dags/scripts/universal_profile/daily_offer_fact_pipeline.py",
        dag=dag)

task2 = bigquery_operator.BigQueryOperator(
    task_id='delete_daily_offer_fact_stage_table',
    sql='''drop table if exists `{0}.{1}.stg_daily_offer_fact`'''.format(o_project, o_stg_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)

task3 = bigquery_operator.BigQueryOperator(
    task_id='insert_into_daily_offer_fact_final_table',
    sql='''create table if not exists `bnile-cdw-prod.o_product.daily_offer_fact` 
               (snapshot_date	datetime
               ,date_key	int64
               ,offer_id	int64
               ,site	STRING
               ,currency_code	STRING
               ,website_price	NUMERIC
               ,website_list_price	NUMERIC
               ,representative_sku	STRING
               ,ring_is_comfort_fit	NUMERIC
               ,min_stone_count	NUMERIC
               ,max_stone_count	NUMERIC
               ,max_chain_length_inches	NUMERIC
               ,min_chain_length_inches	NUMERIC
               ,average_customer_rating	NUMERIC
               ,sell_without_diamond	NUMERIC
               ,is_sale_item	NUMERIC
               ,product_current_status	STRING
               ,any_product_sellable	STRING
               ,eligible_to_search	NUMERIC
               ,is_all_backordered	NUMERIC
               ,is_any_backordered	NUMERIC
               ,is_limited_availability_item	NUMERIC
               ,is_engraveable	NUMERIC
               ,is_engrave_is_monogrammable	NUMERIC
               ,is_locally_sourced	NUMERIC
               ,max_ships_in_days	NUMERIC
               ,min_ships_in_days	NUMERIC
               ,min_price_with_contained	NUMERIC
               ,min_price	NUMERIC
               ,max_price	NUMERIC
               ,min_price_all_totalled	NUMERIC
               ,max_price_all_totalled	NUMERIC
               ,retail_comparison_price	NUMERIC
               ,retail_comparison_price_tot	NUMERIC
               ,create_date_utc	timestamp
               ,last_update_date_utc	timestamp
               )
               partition by   range_bucket(date_key,generate_array(20200101,20211231,02));	


               INSERT INTO
               `bnile-cdw-prod.o_product.daily_offer_fact` 

               (
               select *
               from(
               select
               DATETIME(current_timestamp,"America/Los_Angeles") as snapshot_date
               ,cast(replace(cast(DATE_SUB(date(current_timestamp), INTERVAL 1 DAY) AS string),'-','') as int64) as date_key
               ,cast(offer_id as int64) offer_id
               ,cast(site as STRING) site
               ,cast(currency_code as STRING) currency_code
               ,cast(website_price as NUMERIC) website_price
               ,cast(website_list_price as NUMERIC) website_list_price
               ,cast(representative_sku as STRING) representative_sku
               ,cast(ring_is_comfort_fit as NUMERIC) ring_is_comfort_fit
               ,cast(min_stone_count as NUMERIC) min_stone_count
               ,cast(max_stone_count as NUMERIC) max_stone_count
               ,cast(max_chain_length_inches as NUMERIC) max_chain_length_inches
               ,cast(min_chain_length_inches as NUMERIC) min_chain_length_inches
               ,cast(average_customer_rating as NUMERIC) average_customer_rating
               ,cast(sell_without_diamond as NUMERIC) sell_without_diamond
               ,cast(is_sale_item as NUMERIC) is_sale_item
               ,cast(product_current_status as STRING) product_current_status
               ,cast(any_product_sellable as STRING) any_product_sellable
               ,cast(eligible_to_search as NUMERIC) eligible_to_search
               ,cast(is_all_backordered as NUMERIC) is_all_backordered
               ,cast(is_any_backordered as NUMERIC) is_any_backordered
               ,cast(is_limited_availability_item as NUMERIC) is_limited_availability_item
               ,cast(is_engraveable as NUMERIC) is_engraveable
               ,cast(is_engrave_is_monogrammable as NUMERIC) is_engrave_is_monogrammable
               ,cast(is_locally_sourced as NUMERIC) is_locally_sourced
               ,cast(max_ships_in_days as NUMERIC) max_ships_in_days
               ,cast(min_ships_in_days as NUMERIC) min_ships_in_days
               ,cast(min_price_with_contained as NUMERIC) min_price_with_contained
               ,cast(min_price as NUMERIC) min_price
               ,cast(max_price as NUMERIC) max_price
               ,cast(min_price_all_totalled as NUMERIC) min_price_all_totalled
               ,cast(max_price_all_totalled as NUMERIC) max_price_all_totalled
               ,cast(retail_comparison_price as NUMERIC) retail_comparison_price
               ,cast(retail_comparison_price_tot as NUMERIC) retail_comparison_price_tot
               ,current_timestamp create_date_utc
               ,current_timestamp last_update_date_utc
               from `bnile-cdw-prod.up_stg_ora_tables.stg_daily_offer_fact`)aa where aa.date_key not in(select distinct date_key from `bnile-cdw-prod.o_product.daily_offer_fact`))'''.format(
        o_project, o_stg_dataset, o_tgt_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)
# Setting up the dependencies for the tasks
task2 >> task1 >> task3