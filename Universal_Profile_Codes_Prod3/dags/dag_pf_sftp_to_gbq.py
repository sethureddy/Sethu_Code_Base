from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor, GoogleCloudStorageObjectUpdatedSensor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.models import Variable
from airflow.utils import trigger_rule

# getting the dev credentials that are stored securely in the airflow server
o_project = Variable.get("project_id")
# o_stg_dataset=Variable.get("stg_dataset")
# o_tgt_dataset=Variable.get("tgt_dataset")
o_dataset = 'product_feeds'

default_args = {
    'owner': 'UP',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('bn_pf_sftp_to_bq', schedule_interval='15 8 * * *', default_args=default_args) as dag:
    sftp_to_gcs = bash_operator.BashOperator(
        task_id='pf_all_sftp_to_gcs',
        # bigquery_conn_id='google_cloud_default',
        bash_command="source /home/airflow/gcs/dags/venvs/activate_cdw_ora_to_bq_venv.sh && python3 /home/airflow/gcs/dags/scripts/universal_profile/pf_sftp_to_gcs.py",
        dag=dag)

    pf_us_byo_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="pf_us_byo_gcs_to_bq_raw",
        bucket='us-central1-prod-3-4076075b-bucket',
        destination_project_dataset_table="product_feeds.stg_src_us_bn_byo",
        source_objects=['data/ProductFeeds/ProductFeeds/USA/inv_bluenile_byo.txt'],
        # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
        # schema_object='Dssprod/browsing-history/schema.json',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        autodetect=True,
        source_format='CSV',
        #field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    pf_us_diamonds_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="pf_us_diamonds_gcs_to_bq_raw",
        bucket='us-central1-prod-3-4076075b-bucket',
        destination_project_dataset_table="product_feeds.stg_src_us_bn_diamonds",
        source_objects=['data/ProductFeeds/ProductFeeds/USA/inv_bluenile_diamonds.txt'],
        # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
        # schema_object='Dssprod/browsing-history/schema.json',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        autodetect=True,
        source_format='CSV',
        #field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    pf_us_product_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="pf_us_product_gcs_to_bq_raw",
        bucket='us-central1-prod-3-4076075b-bucket',
        destination_project_dataset_table="product_feeds.stg_src_us_bn_product",
        source_objects=['data/ProductFeeds/ProductFeeds/USA/inv_bluenile.txt'],
        # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
        # schema_object='Dssprod/browsing-history/schema.json',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        #quote_character="",
        #encoding='UTF-8',
        #allow_quoted_newlines=True,
        autodetect=True,
        source_format='CSV',
        #field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    pf_us_setting_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="pf_us_setting_gcs_to_bq_raw",
        bucket='us-central1-prod-3-4076075b-bucket',
        destination_project_dataset_table="product_feeds.stg_src_us_bn_setting",
        source_objects=['data/ProductFeeds/ProductFeeds/USA/inv_bluenile_setting.txt'],
        # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
        # schema_object='Dssprod/browsing-history/schema.json',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        autodetect=True,
        source_format='CSV',
        #field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    pf_us_byo_stage_to_final = bigquery_operator.BigQueryOperator(
        task_id='pf_us_byo_stage_to_final',
        sql='''CREATE OR REPLACE TABLE
               `{0}.{1}.stg_src_us_bn_byo` AS (
               SELECT
                   *,
                   current_date AS created_date
               FROM
                   `{0}.{1}.stg_src_us_bn_byo`);

               CREATE TABLE IF NOT EXISTS
               `{0}.{1}.src_us_bn_byo` AS(
               SELECT
                   *
               FROM
                   `{0}.{1}.stg_src_us_bn_byo`);
               INSERT INTO
               `{0}.{1}.src_us_bn_byo`
               SELECT
               *
               FROM
               `{0}.{1}.stg_src_us_bn_byo`
               WHERE
               created_date NOT IN(
               SELECT
                   DISTINCT created_date
               FROM
                   `{0}.{1}.src_us_bn_byo`);'''.format(o_project, o_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    pf_us_diamonds_stage_to_final = bigquery_operator.BigQueryOperator(
        task_id='pf_us_diamonds_stage_to_final',
        sql='''CREATE OR REPLACE TABLE
               `{0}.{1}.stg_src_us_bn_diamonds` AS (
               SELECT
                   *,
                   current_date AS created_date
               FROM
                   `{0}.{1}.stg_src_us_bn_diamonds`);

               CREATE TABLE IF NOT EXISTS
               `{0}.{1}.src_us_bn_diamonds` AS(
               SELECT
                   *
               FROM
                   `{0}.{1}.stg_src_us_bn_diamonds`);
               INSERT INTO
               `{0}.{1}.src_us_bn_diamonds`
               SELECT
               *
               FROM
               `{0}.{1}.stg_src_us_bn_diamonds`
               WHERE
               created_date NOT IN(
               SELECT
                   DISTINCT created_date
               FROM
                   `{0}.{1}.src_us_bn_diamonds`);'''.format(o_project, o_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    pf_us_product_stage_to_final = bigquery_operator.BigQueryOperator(
        task_id='pf_us_product_stage_to_final',
        sql='''CREATE OR REPLACE TABLE
               `{0}.{1}.stg_src_us_bn_product` AS (
               SELECT
                   *,
                   current_date AS created_date
               FROM
                   `{0}.{1}.stg_src_us_bn_product`);

               CREATE TABLE IF NOT EXISTS
               `{0}.{1}.src_us_bn_product` AS(
               SELECT
                   *
               FROM
                   `{0}.{1}.stg_src_us_bn_product`);
               INSERT INTO
               `{0}.{1}.src_us_bn_product`
               SELECT
               *
               FROM
               `{0}.{1}.stg_src_us_bn_product`
               WHERE
               created_date NOT IN(
               SELECT
                   DISTINCT created_date
               FROM
                   `{0}.{1}.src_us_bn_product`);'''.format(o_project, o_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    pf_us_setting_stage_to_final = bigquery_operator.BigQueryOperator(
        task_id='pf_us_setting_stage_to_final',
        sql='''CREATE OR REPLACE TABLE
               `{0}.{1}.stg_src_us_bn_setting` AS (
               SELECT
                   *,
                   current_date AS created_date
               FROM
                   `{0}.{1}.stg_src_us_bn_setting`);

               CREATE TABLE IF NOT EXISTS
               `{0}.{1}.src_us_bn_setting` AS(
               SELECT
                   *
               FROM
                   `{0}.{1}.stg_src_us_bn_setting`);
               INSERT INTO
               `{0}.{1}.src_us_bn_setting`
               SELECT
               *
               FROM
               `{0}.{1}.stg_src_us_bn_setting`
               WHERE
               created_date NOT IN(
               SELECT
                   DISTINCT created_date
               FROM
                   `{0}.{1}.src_us_bn_setting`);'''.format(o_project, o_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    pf_uk_byo_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="pf_uk_byo_gcs_to_bq_raw",
        bucket='us-central1-prod-3-4076075b-bucket',
        destination_project_dataset_table="product_feeds.stg_src_uk_bn_byo",
        source_objects=['data/ProductFeeds/ProductFeeds/GBR/inv_bluenile_byo.txt'],
        # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
        # schema_object='Dssprod/browsing-history/schema.json',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        autodetect=True,
        source_format='CSV',
        #field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    pf_uk_diamonds_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="pf_uk_diamonds_gcs_to_bq_raw",
        bucket='us-central1-prod-3-4076075b-bucket',
        destination_project_dataset_table="product_feeds.stg_src_uk_bn_diamonds",
        source_objects=['data/ProductFeeds/ProductFeeds/GBR/inv_bluenile_diamonds.txt'],
        # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
        # schema_object='Dssprod/browsing-history/schema.json',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        autodetect=True,
        source_format='CSV',
        #field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    pf_uk_product_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="pf_uk_product_gcs_to_bq_raw",
        bucket='us-central1-prod-3-4076075b-bucket',
        destination_project_dataset_table="product_feeds.stg_src_uk_bn_product",
        source_objects=['data/ProductFeeds/ProductFeeds/GBR/inv_bluenile.txt'],
        # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
        # schema_object='Dssprod/browsing-history/schema.json',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        #quote_character="",
        #encoding='UTF-8',
        #allow_quoted_newlines=True,
        autodetect=True,
        source_format='CSV',
        #field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    # pf_uk_setting_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    # task_id="pf_uk_setting_gcs_to_bq_raw",
    # bucket='us-central1-prod-3-4076075b-bucket',
    # destination_project_dataset_table="product_feeds.stg_src_uk_bn_setting",
    # source_objects=['data/ProductFeeds/ProductFeeds/GBR/inv_bluenile_setting.txt'],
    # schema_object='/home/airflow/gcs/dags/scripts/address_dim.json',
    # schema_object='Dssprod/browsing-history/schema.json',
    # bigquery_conn_id = 'google_cloud_default',
    # google_cloud_storage_conn_id='google_cloud_default',
    # autodetect=True,
    # source_format='CSV',
    # #field_delimiter=',',
    # skip_leading_rows=1,
    # create_disposition='CREATE_IF_NEEDED',
    # write_disposition='WRITE_TRUNCATE',
    # dag=dag)

    pf_uk_byo_stage_to_final = bigquery_operator.BigQueryOperator(
        task_id='pf_uk_byo_stage_to_final',
        sql='''CREATE OR REPLACE TABLE
               `{0}.{1}.stg_src_uk_bn_byo` AS (
               SELECT
                   *,
                   current_date AS created_date
               FROM
                   `{0}.{1}.stg_src_uk_bn_byo`);

               CREATE TABLE IF NOT EXISTS
               `{0}.{1}.src_uk_bn_byo` AS(
               SELECT
                   *
               FROM
                   `{0}.{1}.stg_src_uk_bn_byo`);
               INSERT INTO
               `{0}.{1}.src_uk_bn_byo`
               SELECT
               *
               FROM
               `{0}.{1}.stg_src_uk_bn_byo`
               WHERE
               created_date NOT IN(
               SELECT
                   DISTINCT created_date
               FROM
                   `{0}.{1}.src_uk_bn_byo`);'''.format(o_project, o_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    pf_uk_diamonds_stage_to_final = bigquery_operator.BigQueryOperator(
        task_id='pf_uk_diamonds_stage_to_final',
        sql='''CREATE OR REPLACE TABLE
               `{0}.{1}.stg_src_uk_bn_diamonds` AS (
               SELECT
                   *,
                   current_date AS created_date
               FROM
                   `{0}.{1}.stg_src_uk_bn_diamonds`);

               CREATE TABLE IF NOT EXISTS
               `{0}.{1}.src_uk_bn_diamonds` AS(
               SELECT
                   *
               FROM
                   `{0}.{1}.stg_src_uk_bn_diamonds`);
               INSERT INTO
               `{0}.{1}.src_uk_bn_diamonds`
               SELECT
               *
               FROM
               `{0}.{1}.stg_src_uk_bn_diamonds`
               WHERE
               created_date NOT IN(
               SELECT
                   DISTINCT created_date
               FROM
                   `{0}.{1}.src_uk_bn_diamonds`);'''.format(o_project, o_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    pf_uk_product_stage_to_final = bigquery_operator.BigQueryOperator(
        task_id='pf_uk_product_stage_to_final',
        sql='''CREATE OR REPLACE TABLE
               `{0}.{1}.stg_src_uk_bn_product` AS (
               SELECT
                   *,
                   current_date AS created_date
               FROM
                   `{0}.{1}.stg_src_uk_bn_product`);

               CREATE TABLE IF NOT EXISTS
               `{0}.{1}.src_uk_bn_product` AS(
               SELECT
                   *
               FROM
                   `{0}.{1}.stg_src_uk_bn_product`);
               INSERT INTO
               `{0}.{1}.src_uk_bn_product`
               SELECT
               *
               FROM
               `{0}.{1}.stg_src_uk_bn_product`
               WHERE
               created_date NOT IN(
               SELECT
                   DISTINCT created_date
               FROM
                   `{0}.{1}.src_uk_bn_product`);'''.format(o_project, o_dataset),
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    # pf_uk_setting_stage_to_final =  bigquery_operator.BigQueryOperator(
    # task_id = 'pf_uk_setting_stage_to_final',
    # sql='''CREATE OR REPLACE TABLE
    # `{0}.{1}.stg_src_uk_bn_setting` AS (
    # SELECT
    # *,
    # current_date AS created_date
    # FROM
    # `{0}.{1}.stg_src_uk_bn_setting`);

    # CREATE TABLE IF NOT EXISTS
    # `{0}.{1}.src_uk_bn_setting` AS(
    # SELECT
    # *
    # FROM
    # `{0}.{1}.stg_src_uk_bn_setting`);
    # INSERT INTO
    # `{0}.{1}.src_uk_bn_setting`
    # SELECT
    # *
    # FROM
    # `{0}.{1}.stg_src_uk_bn_setting`
    # WHERE
    # created_date NOT IN(
    # SELECT
    # DISTINCT created_date
    # FROM
    # `{0}.{1}.src_uk_bn_setting`);'''.format(o_project, o_dataset),
    # use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    # dag=dag)

    byo_offer_images_dim_load = bigquery_operator.BigQueryOperator(
        task_id='byo_offer_images_dim_load',
        sql='''create table if not exists `bnile-cdw-prod.o_product.byo_offer_images_dim`
               (              
               logical_id               STRING
               ,offer_id STRING
               ,diamond_shape  STRING
               ,ct_wt     STRING
               ,image_link           STRING
               ,create_date_utc DATETIME
               ,last_update_date_utc        DATETIME
              );


               MERGE INTO
              `bnile-cdw-prod.o_product.byo_offer_images_dim` tgt
               USING

               (
                  SELECT
                  logical_id,
                  offer_id,
                  diamond_shape,
                  ct_wt,
                  image_link,
                  DATETIME(current_timestamp) AS create_date_utc,
                  DATETIME(current_timestamp) AS last_update_date_utc
                  FROM (
                  SELECT
                      CONCAT(CONCAT(offer_id,'-'),shape) logical_id,
                      offer_id,
                      shape diamond_shape,
                      image_link,
                      ct_wt,
                      ROW_NUMBER() OVER (PARTITION BY offer_id, shape ORDER BY CASE WHEN ct_wt LIKE '%1.%' THEN '999' ELSE ct_wt END DESC) rn -- do this to get 1 ct images when available
                  FROM ( SELECT SUBSTR(id,0, STRPOS(id,'-')-1) AS Offer_id, CASE
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Cushion') > 0 THEN 'CU'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Emerald') > 0 THEN 'EC'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Asscher') > 0 THEN 'AS'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Radiant') > 0 THEN 'RA'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Princess') > 0 THEN 'PR'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Oval') > 0 THEN 'OV'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Pear') > 0 THEN 'PS'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Heart') > 0 THEN 'HS'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Marquise') > 0 THEN 'MQ'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Round') > 0 THEN 'RD'
                      ELSE
                      ''
                      END
                      AS Shape,
                      REGEXP_EXTRACT(title, r"[- . 0-9]+[ ct.]+$") ct_wt,
                      title,
                      description,
                      image_link
                      FROM
                      `bnile-cdw-prod.product_feeds.src_us_bn_byo`
                      UNION ALL
                      SELECT
                      SUBSTR(id,0, STRPOS(id,'-')-1) AS Offer_id,
                      CASE
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Cushion') > 0 THEN 'CU'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Emerald') > 0 THEN 'EC'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Asscher') > 0 THEN 'AS'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Radiant') > 0 THEN 'RA'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Princess') > 0 THEN 'PR'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Oval') > 0 THEN 'OV'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Pear') > 0 THEN 'PS'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Heart') > 0 THEN 'HS'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Marquise') > 0 THEN 'MQ'
                          WHEN STRPOS(SUBSTR(title,LENGTH(title) - 25, 25), 'Round') > 0 THEN 'RD'
                      ELSE
                      ''
                      END
                      AS Shape,
                      REGEXP_EXTRACT(title, r"[- . 0-9]+[ ct.]+$") ct_wt,
                      title,
                      description,
                      image_link
                      FROM
                      `bnile-cdw-prod.product_feeds.src_uk_bn_byo` ) )
                  WHERE
                  rn = 1
                          )src

                          ON
              (tgt.logical_id = src.logical_id)

              WHEN MATCHED  THEN 
                  UPDATE SET  
             --tgt.logical_id=src.logical_id
             tgt.offer_id=src.offer_id
             ,tgt.diamond_shape=src.diamond_shape
             ,tgt.ct_wt=src.ct_wt
             ,tgt.image_link=src.image_link
             --tgt.create_date_utc=src.create_date_utc
             ,tgt.last_update_date_utc=src.last_update_date_utc



              WHEN NOT MATCHED   THEN
              INSERT
              (
              logical_id
              ,offer_id
              ,diamond_shape
              ,ct_wt
              ,image_link
              ,create_date_utc
              ,last_update_date_utc


              )

              VALUES(
              logical_id
              ,src.offer_id
              ,src.diamond_shape
              ,src.ct_wt
              ,src.image_link
              ,src.create_date_utc
              ,src.last_update_date_utc
              )''',
        use_legacy_sql=False,
        # write_disposition='WRITE_APPEND',
        # create_disposition='CREATE_IF_NEEDED',
        dag=dag)

    dependency_link = DummyOperator(task_id="dependency_link", dag=dag)

    # dependency_link =  bash_operator.BashOperator(
    # task_id = 'dependency_link',
    # bash_command="echo succes",
    # dag=dag)

sftp_to_gcs >> [pf_us_byo_gcs_to_bq, pf_us_diamonds_gcs_to_bq, pf_us_product_to_bq, pf_us_setting_gcs_to_bq,
                pf_uk_byo_gcs_to_bq, pf_uk_diamonds_gcs_to_bq, pf_uk_product_to_bq] >> dependency_link >> [
    pf_us_byo_stage_to_final, pf_us_diamonds_stage_to_final, pf_us_product_stage_to_final, pf_us_setting_stage_to_final,
    pf_uk_byo_stage_to_final, pf_uk_diamonds_stage_to_final, pf_uk_product_stage_to_final] >> byo_offer_images_dim_load