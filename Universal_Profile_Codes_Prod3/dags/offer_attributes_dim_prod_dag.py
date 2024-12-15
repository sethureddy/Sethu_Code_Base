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
o_product_dataset = Variable.get("product_dataset")

# run_config = None
# YESTERDAY = datetime.now() - timedelta(days=1)

# defining default arguments
default_args = {
    'owner': 'UP',
    # 'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# Naming the DAG and passing the default arguments from default_args
with DAG('offer_attributes_dim_prod_dag', schedule_interval='30 9 * * *', default_args=default_args) as dag:
    task1 = bash_operator.BashOperator(
        task_id='offer_attributes_dim_TO_GBQ',
        # bigquery_conn_id = 'google_cloud_default',
        bash_command="source /home/airflow/gcs/dags/venvs/activate_cdw_ora_to_bq_venv.sh && python3 /home/airflow/gcs/dags/scripts/universal_profile/offer_attributes_dim.py",
        dag=dag)

task2 = bigquery_operator.BigQueryOperator(
    task_id='delete_offer_dim__extended_stage_table',
    sql='''drop table if exists {0}.{1}.offer_attributes_dim'''.format(o_project, o_stg_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)

task3 = bigquery_operator.BigQueryOperator(
    task_id='insert_into_offer_attributes_dim_final_table',
    sql='''create table if not exists `{0}.{2}.offer_attributes_dim`
               (	
                offer_id INT64
                ,name	STRING
                ,description	STRING
                ,name_plaintext  STRING
                ,description_plaintext STRING
                ,short_name_plaintext STRING
                ,image_url  STRING
                ,target_gender	  STRING                     
                ,brand	STRING
                ,primary_metal_name	STRING
                ,primary_metal_color	STRING
                ,primary_metal_purity	STRING
                ,primary_stone_category	STRING
                ,primary_stone_type	     STRING           
                ,offer_display	STRING
                ,department_name	STRING
                ,category_name	STRING
                ,class_name	STRING
                ,sub_class_name	STRING
                ,merch_product_category STRING
                ,merch_category_rollup  STRING
                ,merch_sub_category_rollup  STRING
                ,feed_category STRING
                ,create_date_utc DATETIME
                ,last_update_date_utc DATETIME
                ,marketing_category STRING
                ,alternate_image_url STRING
                ,representative_sku STRING  
                
               );	

               
                MERGE INTO
               `{0}.{2}.offer_attributes_dim` tgt
               USING

               (
               select 
               cast(offer_id as INT64) offer_id
               ,cast(name as STRING) name
               ,cast(description as STRING) description
               ,cast(name_plaintext as STRING) name_plaintext
               ,cast(description_plaintext as STRING) description_plaintext
               ,cast(short_name_plaintext as STRING) short_name_plaintext                
               ,cast(image_url as STRING) image_url
               ,cast(target_gender as STRING) target_gender               
               ,cast(brand as STRING) brand
               ,cast(primary_metal_name as STRING) primary_metal_name
               ,cast(primary_metal_color as STRING) primary_metal_color
               ,cast(primary_metal_purity as STRING) primary_metal_purity
               ,cast(primary_stone_category as STRING) primary_stone_category
               ,cast(primary_stone_type as STRING) primary_stone_type
               ,cast(offer_display as STRING) offer_display
               ,cast(department_name as STRING) department_name
               ,cast(category_name as STRING) category_name
               ,cast(class_name as STRING) class_name
               ,cast(sub_class_name as STRING) sub_class_name
               ,cast(merch_product_category as STRING) merch_product_category
               ,cast(merch_category_rollup as STRING) merch_category_rollup
               ,cast(merch_sub_category_rollup as STRING) merch_sub_category_rollup
               ,cast(feed_category as STRING) feed_category
               ,DATETIME(CURRENT_TIMESTAMP) AS create_date_utc
               ,DATETIME(CURRENT_TIMESTAMP) AS last_update_date_utc  
               ,cast(marketing_category as STRING) marketing_category
               ,cast(alternate_image_url as STRING) alternate_image_url
               ,cast(representative_sku as STRING) representative_sku


               from `{0}.{1}.offer_attributes_dim`
               )src
               
                ON
               (tgt.offer_id = cast(src.offer_id as int64))
               
                WHEN MATCHED  THEN 
                UPDATE SET  
                tgt.offer_id=src.offer_id
                ,tgt.name=src.name	
                ,tgt.description=src.description	
                ,tgt.name_plaintext=src.name_plaintext 
                ,tgt.description_plaintext=src.description_plaintext 
                ,tgt.short_name_plaintext=src.short_name_plaintext 
                ,tgt.image_url=src.image_url  
                ,tgt.target_gender=src.target_gender	                               
                ,tgt.brand=src.brand	
                ,tgt.primary_metal_name=src.primary_metal_name	
                ,tgt.primary_metal_color=src.primary_metal_color	
                ,tgt.primary_metal_purity=src.primary_metal_purity	
                ,tgt.primary_stone_category=src.primary_stone_category	
                ,tgt.primary_stone_type=src.primary_stone_type	                
                ,tgt.offer_display=src.offer_display	
                ,tgt.department_name=src.department_name	
                ,tgt.category_name=src.category_name	
                ,tgt.class_name=src.class_name	
                ,tgt.sub_class_name=src.sub_class_name	
                ,tgt.merch_product_category=src.merch_product_category
                ,tgt.merch_category_rollup=src.merch_category_rollup 
                ,tgt.merch_sub_category_rollup=src.merch_sub_category_rollup 
                ,tgt.feed_category=src.feed_category 
            --,tgt.create_date_utc=src.create_date_utc
                ,tgt.last_update_date_utc=src.last_update_date_utc
                ,tgt.marketing_category=src.marketing_category
                ,tgt.alternate_image_url=src.alternate_image_url
                ,tgt.representative_sku=src.representative_sku


                WHEN NOT MATCHED   THEN
                INSERT
                (offer_id
                ,name	
                ,description	
                ,name_plaintext 
                ,description_plaintext 
                ,short_name_plaintext 
                ,image_url  
                ,target_gender	                               
                ,brand	
                ,primary_metal_name	
                ,primary_metal_color	
                ,primary_metal_purity	
                ,primary_stone_category	
                ,primary_stone_type	                
                ,offer_display	
                ,department_name	
                ,category_name	
                ,class_name	
                ,sub_class_name	
                ,merch_product_category
                ,merch_category_rollup 
                ,merch_sub_category_rollup 
                ,feed_category 
                ,create_date_utc
                ,last_update_date_utc
                ,marketing_category
                ,alternate_image_url
                ,representative_sku
                )
                VALUES
                (
                src.offer_id
                ,src.name	
                ,src.description	
                ,src.name_plaintext 
                ,src.description_plaintext 
                ,src.short_name_plaintext 
                ,src.image_url  
                ,src.target_gender	                               
                ,src.brand	
                ,src.primary_metal_name	
                ,src.primary_metal_color	
                ,src.primary_metal_purity	
                ,src.primary_stone_category	
                ,src.primary_stone_type	                
                ,src.offer_display	
                ,src.department_name	
                ,src.category_name	
                ,src.class_name	
                ,src.sub_class_name	
                ,src.merch_product_category
                ,src.merch_category_rollup 
                ,src.merch_sub_category_rollup 
                ,src.feed_category 
                ,src.create_date_utc
                ,src.last_update_date_utc
                ,src.marketing_category
                ,src.alternate_image_url
                ,src.representative_sku
                )
               ;'''.format(o_project, o_stg_dataset, o_product_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)
# Setting up the dependencies for the tasks
task2 >> task1 >> task3

