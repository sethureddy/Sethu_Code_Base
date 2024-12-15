import datetime
from airflow.utils.dates import days_ago
from airflow import models
import time
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators import bigquery_operator
from scripts.universal_profile.email_customer_profile import bn_dna_data,bn_cust_dna, email_customer_profile
from airflow.operators.email_operator import EmailOperator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(0),
    datetime.datetime.min.time())

default_dag_args = {
    'owner': 'UP',
    'start_date': yesterday,
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project'),
    'catchup':False
}

o_project = Variable.get("project_id")
o_customer_dataset = Variable.get("gcp_project_customer_dataset")
o_stg_dataset = Variable.get("stg_customer")

params_query = {
    "project_name": o_project
}


sql_query1 = bn_dna_data(params=params_query)
sql_query2 = bn_cust_dna(params=params_query)
sql_query3 = email_customer_profile(params=params_query)

with models.DAG(
        'email_customer_profile_prod_dag',
        schedule_interval= None,
        default_args=default_dag_args) as dag:


    task_bn_dna_data = bigquery_operator.BigQueryOperator(
        task_id='task_bn_dna_data',
        sql=sql_query1,
        use_legacy_sql=False,
    )

    task_bn_cust_dna = bigquery_operator.BigQueryOperator(
        task_id='task_bn_cust_dna',
        sql=sql_query2,
        use_legacy_sql=False,
    )

    task_email_customer_profile = bigquery_operator.BigQueryOperator(
        task_id='task_email_customer_profile',
        sql=sql_query3,
        use_legacy_sql=False,
    )

    delay_python_task = PythonOperator(task_id="delay_python_task",
                                                       dag=dag,
                                                       python_callable=lambda: time.sleep(600))

    # task_link1 = ExternalTaskSensor(
    #     task_id='start_email_customer_profile_dependency_1',
    #     external_dag_id='djtestlink1_dag',
    #     external_task_id='start_email_customer_profile_dependency_1',
    #     timeout=180)
    #
    # task_link2 = ExternalTaskSensor(
    #     task_id='start_email_customer_profile_dependency_2',
    #     external_dag_id='djtestlink2_dag',
    #     external_task_id='start_email_customer_profile_dependency_2',
    #     timeout=180)


    merge_into_final_email_customer_profile = bigquery_operator.BigQueryOperator(
        task_id='merge_into_final_email_customer_profile',
        sql='''create table if not exists `{0}.{2}.email_customer_profile`
                   (
                   email_address STRING
                    ,bnid STRING
                    ,up_email_address_key NUMERIC
                    ,email_address_test_key NUMERIC
                    ,bn_test_key NUMERIC
                    ,email_contactable_flag STRING
                    ,primary_site STRING
                    ,primary_hostname STRING
                    ,primary_language_code STRING
                    ,country_code STRING
                    ,phone STRING
                    ,primary_currency_code STRING
                    ,region STRING
                    ,queens_english_flag STRING
                    --Customer Store Proximity to dma zip additions----5/26/2021
                    ,primary_dma_zip STRING
                    ,primary_dma_source STRING
                    ,nearest_store_1_webroom_key NUMERIC
                    ,nearest_store_2_webroom_key NUMERIC
                    ,nearest_store_3_webroom_key NUMERIC
                    --------------------------------------------------
                    ,cust_dna STRING
                    ,subscribe_flag STRING
                    ,email_open_last_dt DATE
                    ,last_subscribe_date TIMESTAMP
                    ,browse_last_dt DATE
                    ,never_send_flag STRING
                    ,fraud_flag STRING
                    ,bad_address_flag STRING
                    ,undeliverable_date TIMESTAMP
                    ,sweepstakes_entry_last_dt TIMESTAMP
                    ,first_promo_subscribe_date TIMESTAMP
                    ,pending_order_flag STRING
                    ,order_cnt NUMERIC
                    ,order_last_dt DATE
                    ,birth_date DATE
                    ,wedding_date DATE
                    ,pj_lc_flag STRING
                    ,e_sc_most_viewed_offer_id_1 NUMERIC
                    ,e_lc_most_viewed_setting_OID_1 NUMERIC
                    ,e_lc_most_viewed_diamond_sku_1 STRING
                    ,customer_segment_exact STRING
                    ,create_date_utc TIMESTAMP
                    ,last_update_date_utc  TIMESTAMP
                   );

                MERGE INTO
               `{0}.{2}.email_customer_profile` tgt
               USING

               (
               select 
                cast(email_address as STRING)  email_address
                ,cast(bnid as STRING) bnid
                ,cast(up_email_address_key AS NUMERIC)  up_email_address_key
                ,cast(email_address_test_key as NUMERIC)  email_address_test_key
                ,cast(bn_test_key as NUMERIC)  bn_test_key
                ,cast(email_contactable_flag as STRING) email_contactable_flag
                ,cast(primary_site as STRING)primary_site
                ,cast(primary_hostname as STRING)primary_hostname
                ,cast(primary_language_code as STRING)primary_language_code
                ,cast(country_code as STRING)country_code
                ,cast(phone as STRING)phone
                ,cast(primary_currency_code as STRING)primary_currency_code
                ,cast(region as STRING)region
                ,cast(queens_english_flag as STRING)queens_english_flag
                ---------------------------------
                ,cast(primary_dma_zip as STRING)primary_dma_zip
                ,cast(primary_dma_source as STRING)primary_dma_source
                ,cast(nearest_store_1_webroom_key as NUMERIC)nearest_store_1_webroom_key
                ,cast(nearest_store_2_webroom_key as NUMERIC)nearest_store_2_webroom_key
                ,cast(nearest_store_3_webroom_key as NUMERIC)nearest_store_3_webroom_key
                ----------------------------------
                ,cast(cust_dna as STRING)cust_dna
                ,cast(subscribe_flag as STRING)subscribe_flag
                ,cast(email_open_last_dt as DATE)email_open_last_dt
                ,cast(last_subscribe_date as TIMESTAMP)last_subscribe_date
                ,cast(browse_last_dt as DATE)browse_last_dt
                ,cast(never_send_flag as STRING)never_send_flag
                ,cast(fraud_flag as STRING)fraud_flag
                ,cast(bad_address_flag as STRING)bad_address_flag
                ,cast(undeliverable_date as TIMESTAMP)undeliverable_date
                ,cast(sweepstakes_entry_last_dt as TIMESTAMP)sweepstakes_entry_last_dt
                ,cast(first_promo_subscribe_date as TIMESTAMP)first_promo_subscribe_date
                ,cast(pending_order_flag as STRING)pending_order_flag
                ,cast(order_cnt as NUMERIC)order_cnt
                ,cast(order_last_dt as DATE)order_last_dt
                ,cast(birth_date as DATE)birth_date
                ,cast(wedding_date as DATE)wedding_date
                ,cast(pj_lc_flag as STRING)pj_lc_flag
                ,cast(e_sc_most_viewed_offer_id_1 as NUMERIC)e_sc_most_viewed_offer_id_1
                ,cast(e_lc_most_viewed_setting_OID_1 as NUMERIC)e_lc_most_viewed_setting_OID_1
                ,cast(e_lc_most_viewed_diamond_sku_1 as STRING)e_lc_most_viewed_diamond_sku_1
                ,cast(customer_segment_exact as STRING)customer_segment_exact
                ,CURRENT_TIMESTAMP AS create_date_utc
                ,CURRENT_TIMESTAMP AS last_update_date_utc
               from `{0}.{1}.email_customer_profile_stg`
               )src

                ON
               (tgt.up_email_address_key = src.up_email_address_key)

                WHEN MATCHED  THEN
                UPDATE SET
                 tgt.email_address=src.email_address
                 ,tgt.bnid=src.bnid
                 ,tgt.up_email_address_key=src.up_email_address_key
                 ,tgt.email_address_test_key=src.email_address_test_key
                 ,tgt.bn_test_key=src.bn_test_key
                 ,tgt.email_contactable_flag=src.email_contactable_flag
                 ,tgt.primary_site=src.primary_site
                 ,tgt.primary_hostname=src.primary_hostname
                 ,tgt.primary_language_code=src.primary_language_code
                 ,tgt.country_code=src.country_code
                 ,tgt.phone=src.phone
                 ,tgt.primary_currency_code=src.primary_currency_code
                 ,tgt.region=src.region
                 ,tgt.queens_english_flag=src.queens_english_flag
                 ,tgt.primary_dma_zip =src.primary_dma_zip 
                 ,tgt.primary_dma_source =src.primary_dma_source 
                 ,tgt.nearest_store_1_webroom_key =src.nearest_store_1_webroom_key 
                 ,tgt.nearest_store_2_webroom_key =src.nearest_store_2_webroom_key 
                 ,tgt.nearest_store_3_webroom_key =src.nearest_store_3_webroom_key 
                 ,tgt.cust_dna=src.cust_dna
                 ,tgt.subscribe_flag=src.subscribe_flag
                 ,tgt.email_open_last_dt=src.email_open_last_dt
                 ,tgt.last_subscribe_date=src.last_subscribe_date
                 ,tgt.browse_last_dt=src.browse_last_dt
                 ,tgt.never_send_flag=src.never_send_flag
                 ,tgt.fraud_flag=src.fraud_flag
                 ,tgt.bad_address_flag=src.bad_address_flag
                 ,tgt.undeliverable_date=src.undeliverable_date
                 ,tgt.sweepstakes_entry_last_dt=src.sweepstakes_entry_last_dt
                 ,tgt.first_promo_subscribe_date=src.first_promo_subscribe_date
                 ,tgt.pending_order_flag=src.pending_order_flag
                 ,tgt.order_cnt=src.order_cnt
                 ,tgt.order_last_dt=src.order_last_dt
                 ,tgt.birth_date=src.birth_date
                 ,tgt.wedding_date=src.wedding_date
                 ,tgt.pj_lc_flag=src.pj_lc_flag
                 ,tgt.e_sc_most_viewed_offer_id_1=src.e_sc_most_viewed_offer_id_1
                 ,tgt.e_lc_most_viewed_setting_OID_1=src.e_lc_most_viewed_setting_OID_1
                 ,tgt.e_lc_most_viewed_diamond_sku_1=src.e_lc_most_viewed_diamond_sku_1
                 ,tgt.customer_segment_exact=src.customer_segment_exact
              -- ,tgt.create_date_utc=src.create_date_utc
                 ,tgt.last_update_date_utc=src.last_update_date_utc

                WHEN NOT MATCHED   THEN
                INSERT
                (
                email_address
                 ,bnid
                 ,up_email_address_key
                 ,email_address_test_key
                 ,bn_test_key
                 ,email_contactable_flag
                 ,primary_site
                 ,primary_hostname
                 ,primary_language_code
                 ,country_code
                 ,phone
                 ,primary_currency_code
                 ,region
                 ,queens_english_flag
                 ,primary_dma_zip 
                 ,primary_dma_source
                 ,nearest_store_1_webroom_key
                 ,nearest_store_2_webroom_key
                 ,nearest_store_3_webroom_key
                 ,cust_dna
                 ,subscribe_flag
                 ,email_open_last_dt
                 ,last_subscribe_date
                 ,browse_last_dt
                 ,never_send_flag
                 ,fraud_flag
                 ,bad_address_flag
                 ,undeliverable_date
                 ,sweepstakes_entry_last_dt
                 ,first_promo_subscribe_date
                 ,pending_order_flag
                 ,order_cnt
                 ,order_last_dt
                 ,birth_date
                 ,wedding_date
                 ,pj_lc_flag
                 ,e_sc_most_viewed_offer_id_1
                 ,e_lc_most_viewed_setting_OID_1
                 ,e_lc_most_viewed_diamond_sku_1
                 ,customer_segment_exact
                 ,create_date_utc
                 ,last_update_date_utc
                )
                VALUES
                (
                src.email_address
                 ,src.bnid
                 ,src.up_email_address_key
                 ,src.email_address_test_key
                 ,src.bn_test_key
                 ,src.email_contactable_flag
                 ,src.primary_site
                 ,src.primary_hostname
                 ,src.primary_language_code
                 ,src.country_code
                 ,src.phone
                 ,src.primary_currency_code
                 ,src.region
                 ,src.queens_english_flag
                 ,src.primary_dma_zip 
                 ,src.primary_dma_source
                 ,src.nearest_store_1_webroom_key
                 ,src.nearest_store_2_webroom_key
                 ,src.nearest_store_3_webroom_key
                 ,src.cust_dna
                 ,src.subscribe_flag
                 ,src.email_open_last_dt
                 ,src.last_subscribe_date
                 ,src.browse_last_dt
                 ,src.never_send_flag
                 ,src.fraud_flag
                 ,src.bad_address_flag
                 ,src.undeliverable_date
                 ,src.sweepstakes_entry_last_dt
                 ,src.first_promo_subscribe_date
                 ,src.pending_order_flag
                 ,src.order_cnt
                 ,src.order_last_dt
                 ,src.birth_date
                 ,src.wedding_date
                 ,src.pj_lc_flag
                 ,src.e_sc_most_viewed_offer_id_1
                 ,src.e_lc_most_viewed_setting_OID_1
                 ,src.e_lc_most_viewed_diamond_sku_1
                 ,src.customer_segment_exact
                 ,src.create_date_utc
                 ,src.last_update_date_utc
                )
                '''.format(o_project, o_stg_dataset, o_customer_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)
        
    task_email_on_success = EmailOperator(
    task_id="send_mail_on_success", 
    # to='bluenile@affineanalytics.com,marketingetl@bluenile.com,etl@bluenile.com',
    to='rohit.varma@affine.ai',
    subject="UP ETL Alert: Cloud Process Completed Successfully at {{ (macros.datetime.utcnow()- macros.timedelta(hours=7)).strftime('%d-%b-%Y %H:%M') }} PST",
    html_content=''' 
    Hi,<br>
    
    <p>This email is to inform you that the cloud process has successfully completed with the refresh of the pipeline named email_customer_profile on {{ (macros.datetime.utcnow()- macros.timedelta(hours=7)).strftime('%d-%b-%Y %H:%M') }} PST.<br>
           <br> 
	For any queries please send an email to bluenile@affineanalytics.com <br>   
    Thanks<br>
    UP Team<p>''',
    dag=dag)



    delay_python_task >> task_bn_dna_data >> task_bn_cust_dna >> task_email_customer_profile >> merge_into_final_email_customer_profile >> task_email_on_success