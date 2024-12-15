import datetime
import time
import json

from airflow import models
from airflow.operators import (
    bash_operator,
    python_operator,
    dummy_operator,
    email_operator,
)
from airflow.contrib.operators import bigquery_to_gcs, bigquery_operator, gcs_to_bq
from airflow.utils import trigger_rule
from airflow.operators.email_operator import EmailOperator

# Dependency files and functions
from scripts.universal_profile.cross_category_recommendation.model_task_preprocess import (
    task_preprocess,
    recommendation_task_preprocess
)

# from datetime import timedelta
# yesterday = datetime.datetime.now() - timedelta(1)
# EXECUTION_DATE=datetime.datetime.strftime(yesterday, '%Y%m%d')

# ENV Variables
EMAIL = models.Variable.get("email")
PROJECT_ID = models.Variable.get("gcp_project")
EXECUTION_DATE = time.strftime("%Y%m%d")
GCS_BUCKET = models.Variable.get("gcs_bucket")
GCS_DIR_PATH = "data/cross_category_recommendation"
VM_CREATION_CODE_PATH = "/home/airflow/gcs/dags/scripts/universal_profile/cross_category_recommendation/vm_api"
VM_INSTANCE_ZONE = "us-central1-a"
VM_INSTANCE_NAME = "cross-catogory-recommendation-scoring-" + EXECUTION_DATE
VM_INSTANCE_TYPE = "n1-highmem-64"
MODEL_NAME = ["jewellery", "setting", "diamond", "lastpurchase"]
RECOMMENDATION_MODEL_NAME = "cross_category_recommendation"

# Load conf from GCS to BigQuery
FINAL_TABLE_OUTPUT = "o_customer.bn_crosscategory_recommendation"
FINAL_INTERMEDIATE_TABLE = (
        "o_customer.bn_crosscategory_recommendation_" + EXECUTION_DATE
)
FINAL_RECOMMENDATION_CSV = (
        GCS_DIR_PATH + "/output/" + EXECUTION_DATE + "/bn_crosscategory_recommendation_" + EXECUTION_DATE + ".csv"
)

# Configs

(
    JEWELLERY_SQL_QUERY,
    DIAMOND_SQL_QUERY,
    SETTING_SQL_QUERY,
    LASTPURCHASE_SQL_QUERY,
    JEWELLERY_SQL_CSV_INPUT_FILE,
    DIAMOND_SQL_CSV_INPUT_FILE,
    SETTING_SQL_CSV_INPUT_FILE,
    LASTPURCHASE_SQL_CSV_INPUT_FILE,
    JEWELLERY_SQL_OUTPUT,
    DIAMOND_SQL_OUTPUT,
    SETTING_SQL_OUTPUT,
    LASTPURCHASE_SQL_OUTPUT,
    LOCAL_CONF_PATH,
    GCS_CONF_PATH,
    SCORING_OUTPUT,
) = task_preprocess(
    GCS_BUCKET, GCS_DIR_PATH, MODEL_NAME, EXECUTION_DATE
)

"""
# Diamond configs
(
    DIAMOND_SQL_QUERY,
    DIAMOND_SQL_CSV_INPUT_FILE,
    DIAMOND_SQL_OUTPUT,
    DIAMOND_LOCAL_CONF_PATH,
    GCS_CONF_PATH,
    DIAMOND_SCORING_OUTPUT,
) = diamond_task_preprocess(
    GCS_BUCKET, GCS_DIR_PATH, DIAMOND_MODEL_NAME, EXECUTION_DATE
)

# Setting configs
(
    SETTING_SQL_QUERY,
    SETTING_SQL_CSV_INPUT_FILE,
    SETTING_SQL_OUTPUT,
    SETTING_LOCAL_CONF_PATH,
    GCS_CONF_PATH,
    SETTING_SCORING_OUTPUT,
) = setting_task_preprocess(
    GCS_BUCKET, GCS_DIR_PATH, SETTING_MODEL_NAME, EXECUTION_DATE
)

# Jewellery configs
(
    JEWELLERY_SQL_QUERY,
    JEWELLERY_SQL_CSV_INPUT_FILE,
    JEWELLERY_SQL_OUTPUT,
    JEWELLERY_LOCAL_CONF_PATH,
    GCS_CONF_PATH,
    JEWELLERY_SCORING_OUTPUT,
) = jewellery_task_preprocess(
    GCS_BUCKET, GCS_DIR_PATH, JEWELLERY_MODEL_NAME, EXECUTION_DATE
)
"""
# Recommendation configs
RECOMMENDATION_LOCAL_CONF_PATH, GCS_CONF_PATH = recommendation_task_preprocess(
    SCORING_OUTPUT,
    GCS_BUCKET,
    GCS_DIR_PATH,
    MODEL_NAME,
    RECOMMENDATION_MODEL_NAME,
    EXECUTION_DATE,
)

# Dag configuration
DAG_START_DATE = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)
DEFAULT_DAG_ARGS = {
    'owner':'UP',
    "start_date": DAG_START_DATE,
    'email': ['rohit.varma@affine.ai'],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": PROJECT_ID,
}

# Dag tasks
with models.DAG(
        "cross_category_recommendation",
        schedule_interval=None,
        default_args=DEFAULT_DAG_ARGS,
) as dag:
	#task_start = dummy_operator.DummyOperator(task_id="start_cross_recommendation_set_2", dag=dag)

	# Load local conf files to GCS
	task_setup_conf_files = bash_operator.BashOperator(
		task_id="cross_reco_setup_conf_files",
		bash_command="sudo gsutil cp {score_file_path} {recommendation_score_file_path} {gcs_conf_path}".format(
			score_file_path=LOCAL_CONF_PATH,
			recommendation_score_file_path=RECOMMENDATION_LOCAL_CONF_PATH,
			gcs_conf_path=GCS_CONF_PATH,
		),
	)

	# Remove local conf files
	task_rm_local_conf_files = bash_operator.BashOperator(
		task_id="cross_reco_remove_conf_files",
		bash_command="sudo rm {score_file_path} {recommendation_file_name}".format(
			score_file_path=LOCAL_CONF_PATH,
			recommendation_file_name=RECOMMENDATION_LOCAL_CONF_PATH,
		),
	)

	task_check_delete_intermediate_sql_tables = bigquery_operator.BigQueryOperator(
		task_id="cross_reco_check_delete_intermediate_tables",
		sql="""drop table if exists {0}; drop table if exists {1}; drop table if exists {2}; drop table if exists {3}; drop table if exists {4} """.format(
			DIAMOND_SQL_OUTPUT,
			SETTING_SQL_OUTPUT,
			JEWELLERY_SQL_OUTPUT,
			LASTPURCHASE_SQL_OUTPUT,
			FINAL_INTERMEDIATE_TABLE,
		),
		use_legacy_sql=False,
	)

	# Diamond BigQuery Execution
	task_diamond_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="cross_reco_diamond_bq_exec",
		sql=DIAMOND_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=DIAMOND_SQL_OUTPUT,
	)

	# Setting BigQuery Execution
	task_setting_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="cross_reco_setting_bq_exec",
		sql=SETTING_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=SETTING_SQL_OUTPUT,
	)

	# Jewellery BigQuery Execution
	task_jewellery_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="cross_reco_jewellery_bq_exec",
		sql=JEWELLERY_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=JEWELLERY_SQL_OUTPUT,
	)

	# lastpurchase BigQuery Execution
	task_lastpurchase_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="cross_reco_lastpurchase_bq_exec",
		sql=LASTPURCHASE_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=LASTPURCHASE_SQL_OUTPUT,
	)

	# Diamond BigQuery to GCS
	task_diamond_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="cross_reco_diamond_export_query_data_gcs",
		source_project_dataset_table=DIAMOND_SQL_OUTPUT,
		destination_cloud_storage_uris=DIAMOND_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	# Setting BigQuery to GCS
	task_setting_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="cross_reco_setting_export_query_data_gcs",
		source_project_dataset_table=SETTING_SQL_OUTPUT,
		destination_cloud_storage_uris=SETTING_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	# Jewellery BigQuery to GCS
	task_jewellery_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="cross_reco_jewellery_export_query_data_gcs",
		source_project_dataset_table=JEWELLERY_SQL_OUTPUT,
		destination_cloud_storage_uris=JEWELLERY_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	# Jewellery BigQuery to GCS
	task_lastpurchase_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="cross_reco_lastpurchase_export_query_data_gcs",
		source_project_dataset_table=LASTPURCHASE_SQL_OUTPUT,
		destination_cloud_storage_uris=LASTPURCHASE_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	#Create VM and Prepare the instance and run model scoring
	task_vm_creation_run_scoring = bash_operator.BashOperator(
		task_id="cross_reco_vm_creation_run_scoring",
		bash_command="""python {VM_CREATION_CODE_PATH}/create_instance.py {PROJECT_ID} {GCS_BUCKET} {EXECUTION_DATE} {VM_INSTANCE_TYPE} {GCS_DIR_PATH} --zone {VM_INSTANCE_ZONE} --name {VM_INSTANCE_NAME}""".format(
			VM_CREATION_CODE_PATH=VM_CREATION_CODE_PATH,
			PROJECT_ID=PROJECT_ID,
			GCS_BUCKET=GCS_BUCKET,
			EXECUTION_DATE=EXECUTION_DATE,
			VM_INSTANCE_TYPE=VM_INSTANCE_TYPE,
			GCS_DIR_PATH=GCS_DIR_PATH,
			VM_INSTANCE_ZONE=VM_INSTANCE_ZONE,
			VM_INSTANCE_NAME=VM_INSTANCE_NAME,
		),
	)

	#Delete VM
	task_vm_deletion = bash_operator.BashOperator(
		task_id="cross_reco_vm_deletion",
		bash_command="""python {VM_CREATION_CODE_PATH}/create_instance.py {PROJECT_ID} {GCS_BUCKET} {EXECUTION_DATE} {VM_INSTANCE_TYPE} {GCS_DIR_PATH} --zone {VM_INSTANCE_ZONE} --name {VM_INSTANCE_NAME} --delete-instance yes""".format(
			VM_CREATION_CODE_PATH=VM_CREATION_CODE_PATH,
			PROJECT_ID=PROJECT_ID,
			GCS_BUCKET=GCS_BUCKET,
			EXECUTION_DATE=EXECUTION_DATE,
			VM_INSTANCE_TYPE=VM_INSTANCE_TYPE,
			GCS_DIR_PATH=GCS_DIR_PATH,
			VM_INSTANCE_ZONE=VM_INSTANCE_ZONE,
			VM_INSTANCE_NAME=VM_INSTANCE_NAME,
		),
	)

	task_append_final_csv_output_to_table = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
		task_id="cross_reco_append_final_csv_output_to_inter_table",
		destination_project_dataset_table=FINAL_INTERMEDIATE_TABLE,
		bucket=GCS_BUCKET,
		source_objects=[FINAL_RECOMMENDATION_CSV],
		autodetect=True,
		source_format="CSV",
		skip_leading_rows=1,
		create_disposition="CREATE_IF_NEEDED",
		write_disposition="WRITE_TRUNCATE",
	)

	task_insert_from_intermediate_to_final = bigquery_operator.BigQueryOperator(
		task_id="cross_reco_insert_from_intermediate_to_final",
		sql="""delete from `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}` where run_date IN (
			select distinct RUN_DATE from `{PROJECT_ID}.{FINAL_INTERMEDIATE_TABLE}` );
			insert into `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}`
		SELECT
		cast(BNID as STRING) BNID
		,cast(RECO_ID as STRING) RECO_ID
		,cast(RECO_RANK as INT64) RECO_RANK
		,cast(BYO_OPTIONS as STRING) BYO_OPTIONS
		,cast(PRODUCT_CLASS_NAME as STRING) PRODUCT_CLASS_NAME
		,cast(MERCH_PRODUCT_CATEGORY as STRING) MERCH_PRODUCT_CATEGORY
		,cast(RUN_DATE as DATE) RUN_DATE
		,cast(TOTAL_PRICE as FLOAT64) TOTAL_PRICE
		,cast(SETTING_OFFER_ID as INT64) SETTING_OFFER_ID
		,cast(SETTING_PRICE as FLOAT64) SETTING_PRICE
		,cast(ITEM_SKU as STRING) ITEM_SKU
		,cast(ITEM_OFFER_ID as INT64) ITEM_OFFER_ID
		,cast(ITEM_PRICE as FLOAT64) ITEM_PRICE
		,cast(CHAIN_OFFER_ID as INT64) CHAIN_OFFER_ID
		,cast(CHAIN_PRICE as FLOAT64) CHAIN_PRICE
		,cast(PRIMARY_DIAMOND_SHAPE as STRING) PRIMARY_DIAMOND_SHAPE
		,cast(DIAMOND_SKU_1 as STRING) DIAMOND_SKU_1
		,cast(DIAMOND_1_PRICE as FLOAT64) DIAMOND_1_PRICE
		,cast(DIAMOND_SKU_2 as STRING) DIAMOND_SKU_2
		,cast(DIAMOND_2_PRICE as FLOAT64) DIAMOND_2_PRICE
		,cast(DIAMOND_SKU_3 as STRING) DIAMOND_SKU_3
		,cast(DIAMOND_3_PRICE as FLOAT64) DIAMOND_3_PRICE
		,cast(DIAMOND_SKU_4 as STRING) DIAMOND_SKU_4
		,cast(DIAMOND_4_PRICE as FLOAT64) DIAMOND_4_PRICE
		,cast(DIAMOND_SKU_5 as STRING) DIAMOND_SKU_5
		,cast(DIAMOND_5_PRICE as FLOAT64) DIAMOND_5_PRICE
		,cast(MATCHING_PRODUCTS as INT64) MATCHING_PRODUCTS
		FROM
		`{PROJECT_ID}.{FINAL_INTERMEDIATE_TABLE}`;""".format(
			PROJECT_ID=PROJECT_ID,
			FINAL_TABLE_OUTPUT=FINAL_TABLE_OUTPUT,
			FINAL_INTERMEDIATE_TABLE=FINAL_INTERMEDIATE_TABLE,
		),
		use_legacy_sql=False,
	)

	task_email_on_success = EmailOperator(
		task_id="send_mail_on_success",
		# to='undela.reddy@bluenile.com,Vaidehi.Thaker@bluenile.com',
        to='rohit.varma@affine.ai',
		subject='UP ETL Alert:Success(cross_category_recommendation)',
		html_content='''
		Hi,<br>
		
		<p>This email is to inform you that, crosscategory recommendation pipeline has been successfully completed.<br>
			<br>
		-<br>
		UP Team.<p>''',
		dag=dag
	)

	# task_end = dummy_operator.DummyOperator(task_id="end_recommendation_set_2", dag=dag)
	# task_start >> task_check_delete_intermediate_sql_tables >> task_setup_conf_files >> task_rm_local_conf_files
task_setup_conf_files >> task_rm_local_conf_files
task_rm_local_conf_files >> task_diamond_bq_exec >> task_diamond_export_query_data_gcs >> task_vm_creation_run_scoring
task_rm_local_conf_files >> task_setting_bq_exec >> task_setting_export_query_data_gcs >> task_vm_creation_run_scoring
task_rm_local_conf_files >> task_jewellery_bq_exec >> task_jewellery_export_query_data_gcs >> task_vm_creation_run_scoring
task_rm_local_conf_files >> task_lastpurchase_bq_exec >> task_lastpurchase_export_query_data_gcs >> task_vm_creation_run_scoring
task_vm_creation_run_scoring >> task_vm_deletion >> task_append_final_csv_output_to_table >> task_insert_from_intermediate_to_final
task_insert_from_intermediate_to_final >> task_email_on_success
task_insert_from_intermediate_to_final >> task_check_delete_intermediate_sql_tables