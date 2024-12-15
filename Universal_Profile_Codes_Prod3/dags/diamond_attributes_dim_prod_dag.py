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
with DAG('diamond_attributes_dim_prod_dag', schedule_interval='30 9 * * *', default_args=default_args) as dag:
    task1 = bash_operator.BashOperator(
        task_id='diamond_attributes_dim_TO_GBQ',
        # bigquery_conn_id = 'google_cloud_default',
        bash_command="source /home/airflow/gcs/dags/venvs/activate_cdw_ora_to_bq_venv.sh && python3 /home/airflow/gcs/dags/scripts/universal_profile/diamond_attributes_dim.py",
        dag=dag)

task2 = bigquery_operator.BigQueryOperator(
    task_id='delete_diamond_dim_extended_stage_table',
    sql='''drop table if exists {0}.{1}.diamond_attributes_dim'''.format(o_project, o_stg_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)

task3 = bigquery_operator.BigQueryOperator(
    task_id='insert_into_diamond_attributes_dim_final_table',
    sql='''create table if not exists `{0}.{2}.diamond_attributes_dim`
               (
                sku		            STRING
                ,shape	 	        STRING
                ,carat	 	        NUMERIC
                ,cut	 	        STRING
                ,color	 	        STRING
                ,clarity	 	    STRING
                ,symmetry	 	    STRING
                ,polish	 	        STRING
                ,fluorescence	    STRING
                ,brand	 	        STRING
                ,depth_percent	 	NUMERIC
                ,table_percent	 	NUMERIC
                ,length	 	        NUMERIC
                ,width	 	        NUMERIC
                ,height	 	        NUMERIC
                ,girdle_size_min	STRING
                ,girdle_size_max	STRING
                ,girdle_facet	 	STRING
                ,culet	 	        STRING
                ,crown	 	        NUMERIC
                ,pavillion	 	    NUMERIC
                ,milky	 	        STRING
                ,eye_clean	 	    STRING
                ,crown_angle	 	NUMERIC
                ,crown_height	 	NUMERIC
                ,pavillion_angle	NUMERIC
                ,pavillion_depth	NUMERIC
                ,star_length	 	NUMERIC
                ,inscription	 	STRING
                ,lower_half	 	    NUMERIC
                ,rough_origin	 	STRING
                ,country_of_origin	STRING
                ,country_of_mfg	 	STRING
                ,is_canada_mark	 	NUMERIC
                ,is_rio_tinto	 	NUMERIC
                ,promised_basket_id	NUMERIC
                ,ships_in_days	 	NUMERIC
                ,oracle_apps_status	 	STRING
                ,display	 	STRING
                ,status	 	STRING
                ,owned_by_vendor_id	 	NUMERIC
                ,purchased_from_vendor_id	 	NUMERIC
                ,memo_to_vendor_id	 	NUMERIC
                ,purchase_date	DATETIME
                ,vendor_sku	 	STRING
                ,vendor_on_memo	 	STRING
                ,vendor_location_id	 	NUMERIC
                ,imp_log_id	 	NUMERIC
                ,pending_import_log_id NUMERIC
                ,comment_text	 	STRING
                ,internal_vendor_notes	 	STRING
                ,fancy_color_code STRING
                ,fancy_color_description	 	STRING
                ,fancy_base_color	 	STRING
                ,fancy_color_origin	 	STRING
                ,fancy_color_intensity	 	STRING
                ,fancy_color_overtone1	 	STRING
                ,fancy_color_overtone2	 	STRING
                ,fancy_color_overtone3	 	STRING
                ,fancy_color_distribution	 	STRING
                ,merch_sku	 	NUMERIC
                ,merch_category_code	 	STRING
                ,back_of_rap_percent	 	NUMERIC
                ,merch_pricing_quartile	 	NUMERIC
                ,is_marked_delivered	 	STRING
                ,cert_id	 	STRING
                ,cert_lab	 	STRING
                ,cert_file	 	STRING
                ,cert_file_zoom	 	STRING
                ,cert_file_zoom_2	 	STRING
                ,cert_cut	 	STRING
                ,cert_file_raw	 	STRING
                ,cert_id_2	 	STRING
                ,cert_lab_2	 	STRING
                ,cert_file_2	 	STRING
                ,cert_file_raw_2 STRING
                ,sku_create_date DATETIME
                ,alternate_image_url STRING
                ,image_url STRING
                ,create_date_utc TIMESTAMP
                ,last_update_date_utc  TIMESTAMP
               );


                MERGE INTO
               `{0}.{2}.diamond_attributes_dim` tgt
               USING

               (
               select
                CAST(sku		  AS   STRING) sku
                ,CAST(a.shape	 	AS        STRING) shape
                ,CAST(carat	 	  AS      NUMERIC) carat
                ,CAST(cut	 	   AS     STRING) cut
                ,CAST(color	 	   AS     STRING) color
                ,CAST(clarity	 AS	    STRING) clarity
                ,CAST(symmetry  AS	 	    STRING) symmetry
                ,CAST(polish	 	    AS    STRING) polish
                ,CAST(fluorescence	AS    STRING)	 fluorescence
                ,CAST(brand	 	 AS       STRING) brand
                ,CAST(depth_percent	 AS	NUMERIC) depth_percent
                ,CAST(table_percent	 AS	NUMERIC) table_percent
                ,CAST(length	 	AS        NUMERIC) length
                ,CAST(width	 	    AS    NUMERIC) width
                ,CAST(height	 	      AS  NUMERIC) height
                ,CAST(girdle_size_min   AS	STRING)	 girdle_size_min
                ,CAST(girdle_size_max	AS STRING) girdle_size_max
                ,CAST(girdle_facet	 AS	STRING) girdle_facet
                ,CAST(culet	 	AS        STRING) culet
                ,CAST(crown	 	AS        NUMERIC) crown
                ,CAST(pavillion	AS 	    NUMERIC) pavillion
                ,CAST(milky	 	AS        STRING) milky
                ,CAST(eye_clean	 AS	    STRING) eye_clean
                ,CAST(crown_angle	 AS	NUMERIC) crown_angle
                ,CAST(crown_height	AS 	NUMERIC) crown_height
                ,CAST(pavillion_angle   AS	  NUMERIC) pavillion_angle
                ,CAST(pavillion_depth   AS	NUMERIC) pavillion_depth
                ,CAST(star_length	AS 	NUMERIC) star_length
                ,CAST(inscription	 AS	STRING) inscription
                ,CAST(lower_half	 AS	    NUMERIC) lower_half
                ,CAST(rough_origin	   AS	STRING) rough_origin
                ,CAST(country_of_origin	   AS  STRING) country_of_origin
                ,CAST(country_of_mfg	 AS	STRING) country_of_mfg
                ,CAST(is_canada_mark	 AS	NUMERIC) is_canada_mark
                ,CAST(is_rio_tinto	 AS	NUMERIC) is_rio_tinto
                ,CAST(promised_basket_id AS	NUMERIC)	promised_basket_id
                ,CAST(ships_in_days	 AS	NUMERIC) ships_in_days
                ,CAST(oracle_apps_status	 AS	STRING) oracle_apps_status
                ,CAST(display	 AS	STRING) display
                ,CAST(status	 AS	STRING) status
                ,CAST(owned_by_vendor_id	 AS	NUMERIC) owned_by_vendor_id
                ,CAST(purchased_from_vendor_id	 AS	NUMERIC) purchased_from_vendor_id
                ,CAST(memo_to_vendor_id	AS 	NUMERIC) memo_to_vendor_id
                ,CAST(purchase_date AS	DATETIME) purchase_date
                ,CAST(vendor_sku	 AS	STRING) vendor_sku
                ,CAST(vendor_on_memo	 AS	STRING) vendor_on_memo
                ,CAST(vendor_location_id	AS 	NUMERIC) vendor_location_id
                ,CAST(imp_log_id	 AS	NUMERIC) imp_log_id
                ,CAST(pending_import_log_id    AS    NUMERIC)	 pending_import_log_id
                ,CAST(comment_text	AS 	STRING) comment_text
                ,CAST(internal_vendor_notes	 	AS STRING) internal_vendor_notes
                ,CAST(fancy_color_code  AS   STRING) fancy_color_code
                ,CAST(fancy_color_description	 AS	STRING) fancy_color_description
                ,CAST(fancy_base_color	 AS	STRING) fancy_base_color
                ,CAST(fancy_color_origin	 AS	STRING) fancy_color_origin
                ,CAST(fancy_color_intensity	 AS	STRING) fancy_color_intensity
                ,CAST(fancy_color_overtone1	 AS	STRING) fancy_color_overtone1
                ,CAST(fancy_color_overtone2	   AS 	STRING) fancy_color_overtone2
                ,CAST(fancy_color_overtone3	 AS 	STRING)  fancy_color_overtone3
                ,CAST(fancy_color_distribution	AS 	STRING) fancy_color_distribution
                ,CAST(merch_sku	 AS	NUMERIC) merch_sku
                ,CAST(merch_category_code	 	AS STRING) merch_category_code
                ,CAST(back_of_rap_percent	 AS	NUMERIC) back_of_rap_percent
                ,CAST(merch_pricing_quartile	 AS	NUMERIC) merch_pricing_quartile
                ,CAST(is_marked_delivered	 AS	STRING) is_marked_delivered
                ,CAST(cert_id	 AS	STRING) cert_id
                ,CAST(cert_lab	AS 	STRING) cert_lab
                ,CAST(cert_file	AS 	STRING) cert_file
                ,CAST(cert_file_zoom	 AS	STRING) cert_file_zoom
                ,CAST(cert_file_zoom_2	 AS	STRING) cert_file_zoom_2
                ,CAST(cert_cut	AS 	STRING) cert_cut
                ,CAST(cert_file_raw	 AS	STRING) cert_file_raw
                ,CAST(cert_id_2	 AS	STRING) cert_id_2
                ,CAST(cert_lab_2	 AS	STRING) cert_lab_2
                ,CAST(cert_file_2	 AS	STRING) cert_file_2
                ,CAST(cert_file_raw_2       AS     STRING) cert_file_raw_2
                ,CAST(sku_create_date      AS    DATETIME) sku_create_date
                ,CAST(alternate_image_url       AS      STRING) alternate_image_url
                ,CAST(b.image_link      AS     STRING) image_url
               ,CURRENT_TIMESTAMP AS create_date_utc
               ,CURRENT_TIMESTAMP AS last_update_date_utc

               from `{0}.{1}.diamond_attributes_dim` a
                left join
                (SELECT DISTINCT SUBSTR(image_link,0, STRPOS(image_link,'wid=')-1)  as image_link,
                        CASE WHEN STRPOS(DESCRIPTION, 'Cushion') > 0 THEN 'CU'
                                  WHEN STRPOS(DESCRIPTION, 'Emerald') > 0 THEN 'EC'
                                  WHEN STRPOS(DESCRIPTION, 'Asscher') > 0 THEN 'AS'
                                  WHEN STRPOS(DESCRIPTION, 'Radiant') > 0 THEN 'RA'
                                  WHEN STRPOS(DESCRIPTION, 'Princess') > 0 THEN 'PR'
                                  WHEN STRPOS(DESCRIPTION, 'Oval') > 0 THEN 'OV'
                                  WHEN STRPOS(DESCRIPTION, 'Pear') > 0 THEN 'PS'
                                  WHEN STRPOS(DESCRIPTION, 'Heart') > 0 THEN 'HS'
                                  WHEN STRPOS(DESCRIPTION, 'Marquise') > 0 THEN 'MQ'
                                  WHEN STRPOS(DESCRIPTION, 'Round') > 0 THEN 'RD'
                                  ELSE '' END AS SHAPE
                        from `{0}.product_feeds.src_us_bn_diamonds` 
                        WHERE STRPOS(image_link,'wid=') > 0
                        ) b         
                on a.shape=b.shape

               )src

                ON
               (tgt.SKU = src.SKU)

                WHEN MATCHED  THEN
                UPDATE SET
                tgt.sku= src.sku
                ,tgt.shape=src.shape
                ,tgt.carat=src.carat
                ,tgt.cut=src.cut
                ,tgt.color=src.color
                ,tgt.clarity=src.clarity
                ,tgt.symmetry=src.symmetry
                ,tgt.polish=src.polish
                ,tgt.fluorescence=src.fluorescence
                ,tgt.brand=src.brand
                ,tgt.depth_percent=src.depth_percent
                ,tgt.table_percent=src.table_percent
                ,tgt.length=src.length
                ,tgt.width=src.width
                ,tgt.height=src.height
                ,tgt.girdle_size_min=src.girdle_size_min
                ,tgt.girdle_size_max=src.girdle_size_max
                ,tgt.girdle_facet=src.girdle_facet
                ,tgt.culet=src.culet
                ,tgt.crown=src.crown
                ,tgt.pavillion=src.pavillion
                ,tgt.milky=src.milky
                ,tgt.eye_clean=src.eye_clean
                ,tgt.crown_angle=src.crown_angle
                ,tgt.crown_height=src.crown_height
                ,tgt.pavillion_angle=src.pavillion_angle
                ,tgt.pavillion_depth=src.pavillion_depth
                ,tgt.star_length=src.star_length
                ,tgt.inscription=src.inscription
                ,tgt.lower_half=src.lower_half
                ,tgt.rough_origin=src.rough_origin
                ,tgt.country_of_origin=src.country_of_origin
                ,tgt.country_of_mfg=src.country_of_mfg
                ,tgt.is_canada_mark=src.is_canada_mark
                ,tgt.is_rio_tinto=src.is_rio_tinto
                ,tgt.promised_basket_id=src.promised_basket_id
                ,tgt.ships_in_days=src.ships_in_days
                ,tgt.oracle_apps_status=src.oracle_apps_status
                ,tgt.display=src.display
                ,tgt.status=src.status
                ,tgt.owned_by_vendor_id=src.owned_by_vendor_id
                ,tgt.purchased_from_vendor_id=src.purchased_from_vendor_id
                ,tgt.memo_to_vendor_id=src.memo_to_vendor_id
                ,tgt.purchase_date=src.purchase_date
                ,tgt.vendor_sku=src.vendor_sku
                ,tgt.vendor_on_memo=src.vendor_on_memo
                ,tgt.vendor_location_id=src.vendor_location_id
                ,tgt.imp_log_id=src.imp_log_id
                ,tgt.pending_import_log_id=src.pending_import_log_id
                ,tgt.comment_text=src.comment_text
                ,tgt.internal_vendor_notes=src.internal_vendor_notes
                ,tgt.fancy_color_code=src.fancy_color_code
                ,tgt.fancy_color_description=src.fancy_color_description
                ,tgt.fancy_base_color=src.fancy_base_color
                ,tgt.fancy_color_origin=src.fancy_color_origin
                ,tgt.fancy_color_intensity=src.fancy_color_intensity
                ,tgt.fancy_color_overtone1=src.fancy_color_overtone1
                ,tgt.fancy_color_overtone2=src.fancy_color_overtone2
                ,tgt.fancy_color_overtone3=src.fancy_color_overtone3
                ,tgt.fancy_color_distribution=src.fancy_color_distribution
                ,tgt.merch_sku=src.merch_sku
                ,tgt.merch_category_code=src.merch_category_code
                ,tgt.back_of_rap_percent=src.back_of_rap_percent
                ,tgt.merch_pricing_quartile=src.merch_pricing_quartile
                ,tgt.is_marked_delivered=src.is_marked_delivered
                ,tgt.cert_id=src.cert_id
                ,tgt.cert_lab=src.cert_lab
                ,tgt.cert_file=src.cert_file
                ,tgt.cert_file_zoom=src.cert_file_zoom
                ,tgt.cert_file_zoom_2=src.cert_file_zoom_2
                ,tgt.cert_cut=src.cert_cut
                ,tgt.cert_file_raw=src.cert_file_raw
                ,tgt.cert_id_2=src.cert_id_2
                ,tgt.cert_lab_2=src.cert_lab_2
                ,tgt.cert_file_2=src.cert_file_2
                ,tgt.cert_file_raw_2=src.cert_file_raw_2
                ,tgt.sku_create_date=src.sku_create_date
                ,tgt.alternate_image_url=src.alternate_image_url
                ,tgt.image_url=src.image_url
              --,tgt.create_date_utc=src.create_date_utc
                ,tgt.last_update_date_utc=src.last_update_date_utc


                WHEN NOT MATCHED   THEN
                INSERT
                (
                sku
                ,shape
                ,carat
                ,cut
                ,color
                ,clarity
                ,symmetry
                ,polish
                ,fluorescence
                ,brand
                ,depth_percent
                ,table_percent
                ,length
                ,width
                ,height
                ,girdle_size_min
                ,girdle_size_max
                ,girdle_facet
                ,culet
                ,crown
                ,pavillion
                ,milky
                ,eye_clean
                ,crown_angle
                ,crown_height
                ,pavillion_angle
                ,pavillion_depth
                ,star_length
                ,inscription
                ,lower_half
                ,rough_origin
                ,country_of_origin
                ,country_of_mfg
                ,is_canada_mark
                ,is_rio_tinto
                ,promised_basket_id
                ,ships_in_days
                ,oracle_apps_status
                ,display
                ,status
                ,owned_by_vendor_id
                ,purchased_from_vendor_id
                ,memo_to_vendor_id
                ,purchase_date
                ,vendor_sku
                ,vendor_on_memo
                ,vendor_location_id
                ,imp_log_id
                ,pending_import_log_id
                ,comment_text
                ,internal_vendor_notes
                ,fancy_color_code
                ,fancy_color_description
                ,fancy_base_color
                ,fancy_color_origin
                ,fancy_color_intensity
                ,fancy_color_overtone1
                ,fancy_color_overtone2
                ,fancy_color_overtone3
                ,fancy_color_distribution
                ,merch_sku
                ,merch_category_code
                ,back_of_rap_percent
                ,merch_pricing_quartile
                ,is_marked_delivered
                ,cert_id
                ,cert_lab
                ,cert_file
                ,cert_file_zoom
                ,cert_file_zoom_2
                ,cert_cut
                ,cert_file_raw
                ,cert_id_2
                ,cert_lab_2
                ,cert_file_2
                ,cert_file_raw_2
                ,sku_create_date
                ,alternate_image_url
                ,image_url
                ,create_date_utc
                ,last_update_date_utc

                )
                VALUES
                (
                src.sku
                ,src.shape
                ,src.carat
                ,src.cut
                ,src.color
                ,src.clarity
                ,src.symmetry
                ,src.polish
                ,src.fluorescence
                ,src.brand
                ,src.depth_percent
                ,src.table_percent
                ,src.length
                ,src.width
                ,src.height
                ,src.girdle_size_min
                ,src.girdle_size_max
                ,src.girdle_facet
                ,src.culet
                ,src.crown
                ,src.pavillion
                ,src.milky
                ,src.eye_clean
                ,src.crown_angle
                ,src.crown_height
                ,src.pavillion_angle
                ,src.pavillion_depth
                ,src.star_length
                ,src.inscription
                ,src.lower_half
                ,src.rough_origin
                ,src.country_of_origin
                ,src.country_of_mfg
                ,src.is_canada_mark
                ,src.is_rio_tinto
                ,src.promised_basket_id
                ,src.ships_in_days
                ,src.oracle_apps_status
                ,src.display
                ,src.status
                ,src.owned_by_vendor_id
                ,src.purchased_from_vendor_id
                ,src.memo_to_vendor_id
                ,src.purchase_date
                ,src.vendor_sku
                ,src.vendor_on_memo
                ,src.vendor_location_id
                ,src.imp_log_id
                ,src.pending_import_log_id
                ,src.comment_text
                ,src.internal_vendor_notes
                ,src.fancy_color_code
                ,src.fancy_color_description
                ,src.fancy_base_color
                ,src.fancy_color_origin
                ,src.fancy_color_intensity
                ,src.fancy_color_overtone1
                ,src.fancy_color_overtone2
                ,src.fancy_color_overtone3
                ,src.fancy_color_distribution
                ,src.merch_sku
                ,src.merch_category_code
                ,src.back_of_rap_percent
                ,src.merch_pricing_quartile
                ,src.is_marked_delivered
                ,src.cert_id
                ,src.cert_lab
                ,src.cert_file
                ,src.cert_file_zoom
                ,src.cert_file_zoom_2
                ,src.cert_cut
                ,src.cert_file_raw
                ,src.cert_id_2
                ,src.cert_lab_2
                ,src.cert_file_2
                ,src.cert_file_raw_2
                ,src.sku_create_date
                ,src.alternate_image_url
                ,src.image_url
                ,src.create_date_utc
                ,src.last_update_date_utc
                )'''.format(o_project, o_stg_dataset, o_diamond_dataset),
    use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # create_disposition='CREATE_IF_NEEDED',
    dag=dag)
# Setting up the dependencies for the tasks
task2 >> task1 >> task3