import datetime
from airflow.utils.dates import days_ago
from airflow import models
from airflow.models import Variable
import time

from airflow.contrib.operators import bigquery_operator
from scripts.universal_profile.unknown_super_users import unknown_users_data_aggregation

# getting the dev credentials that are stored securely in the airflow server
o_project = Variable.get("project_id")
o_customer_dataset = Variable.get("gcp_project_customer_dataset")
o_stg_dataset = Variable.get("stg_dataset")
EXECUTION_DATE = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") #time.strftime("%Y%m%d")
output_table_name1 = "TBN_superusers_intermediate"

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'owner': 'UP',
    'start_date': yesterday,
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

params_query = {
    "o_project": o_project,
    "o_customer_dataset": o_customer_dataset,
    "o_stg_dataset": o_stg_dataset,
    "EXECUTION_DATE": int(EXECUTION_DATE),
    "output_table": output_table_name1
}

sql_query1 = unknown_users_data_aggregation(params=params_query)

with models.DAG(
        'unknown_super_users',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    task_unknown_users_data_aggregation = bigquery_operator.BigQueryOperator(
        task_id='unknown_users_data_aggregation',
        sql=sql_query1,
        use_legacy_sql=False,
    )

    task_check_and_drop_table_intermediate_unknown_users = bigquery_operator.BigQueryOperator(
        task_id='task_check_and_drop_table_intermediate_unknown_users',
        sql=f'''drop table if exists {o_project}.{o_stg_dataset}.{output_table_name1}'''.format(o_project,o_stg_dataset,output_table_name1),
        use_legacy_sql=False
    )

    merge_into_final_unknown_users_and_create_bnid = bigquery_operator.BigQueryOperator(
        task_id='merge_into_final_unknown_users_and_create_bnid',
        sql='''create table if not exists `{0}.{2}.bn_unknown_guid_attributes`
                   (
                    bnid STRING
                    ,guid STRING
                    ,guid_key NUMERIC
                    ,total_browse_count NUMERIC
                    ,days_appeared NUMERIC
                    ,months_elapsed_since_appeared NUMERIC
                    ,lc NUMERIC
                    ,sc NUMERIC
                    ,basket_flag NUMERIC
                    ,chat_count NUMERIC
                    ,review_count NUMERIC
                    ,detail_view_count NUMERIC
                    ,catalog_view_count NUMERIC
                    ,category_count NUMERIC
                    ,importance_score NUMERIC
                    ,propensity_phase STRING
                    ,create_date_utc TIMESTAMP
                    ,last_update_date_utc  TIMESTAMP
                    ,active_flag STRING
                   );

                MERGE INTO
               `{0}.{2}.bn_unknown_guid_attributes` tgt
               USING

               (
               select 
                cast(concat('TBN',GENERATE_UUID()) as STRING) as bnid
               ,cast(guid as STRING) guid
               ,cast(guid_key as INT64) guid_key
               ,cast(total_browse_count as INT64) total_browse_count
               ,cast(days_appeared as INT64) days_appeared
               ,cast(months_elapsed_since_appeared as INT64) months_elapsed_since_appeared
               ,cast(lc as INT64) lc
               ,cast(sc as INT64) sc
               ,cast(basket_flag as INT64) basket_flag
               ,cast(chat_count as INT64) chat_count
               ,cast(review_count as INT64) review_count
               ,cast(detail_view_count as INT64) detail_view_count
               ,cast(catalog_view_count as INT64) catalog_view_count
               ,cast(category_count as INT64) category_count
               ,cast(importance_score as INT64) importance_score
               ,cast(propensity_phase as STRING) propensity_phase
               ,CURRENT_TIMESTAMP AS create_date_utc
               ,CURRENT_TIMESTAMP AS last_update_date_utc
               ,'Y' as active_flag
               from `{0}.{1}.TBN_superusers_intermediate`
               )src

                ON
               (tgt.guid_key = src.guid_key)

                WHEN MATCHED  THEN
                UPDATE SET
                tgt.guid= src.guid
                ,tgt.guid_key= src.guid_key
                ,tgt.total_browse_count= src.total_browse_count
                ,tgt.days_appeared= src.days_appeared
                ,tgt.months_elapsed_since_appeared= src.months_elapsed_since_appeared
                ,tgt.lc= src.lc
                ,tgt.sc= src.sc
                ,tgt.basket_flag= src.basket_flag
                ,tgt.chat_count= src.chat_count
                ,tgt.review_count= src.review_count
                ,tgt.detail_view_count= src.detail_view_count
                ,tgt.catalog_view_count= src.catalog_view_count
                ,tgt.category_count= src.category_count
                ,tgt.importance_score= src.importance_score
                ,tgt.propensity_phase= src.propensity_phase
              --,tgt.create_date_utc=src.create_date_utc
                ,tgt.last_update_date_utc=src.last_update_date_utc
                ,tgt.active_flag=src.active_flag

                WHEN NOT MATCHED   THEN
                INSERT
                (
                bnid
                ,guid
                ,guid_key
                ,total_browse_count
                ,days_appeared
                ,months_elapsed_since_appeared
                ,lc
                ,sc
                ,basket_flag
                ,chat_count
                ,review_count
                ,detail_view_count
                ,catalog_view_count
                ,category_count
                ,importance_score
                ,propensity_phase
                ,create_date_utc
                ,last_update_date_utc
                ,active_flag
                )
                VALUES
                (
                src.bnid
                ,src.guid
                ,src.guid_key
                ,src.total_browse_count
                ,src.days_appeared
                ,src.months_elapsed_since_appeared
                ,src.lc
                ,src.sc
                ,src.basket_flag
                ,src.chat_count
                ,src.review_count
                ,src.detail_view_count
                ,src.catalog_view_count
                ,src.category_count
                ,src.importance_score
                ,src.propensity_phase
                ,src.create_date_utc
                ,src.last_update_date_utc
                ,src.active_flag
                )

                WHEN NOT MATCHED BY SOURCE
                THEN 
                UPDATE SET
                tgt.active_flag= 'N'
                '''.format(o_project, o_stg_dataset, o_customer_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

task_check_and_drop_table_intermediate_unknown_users >> task_unknown_users_data_aggregation >> merge_into_final_unknown_users_and_create_bnid