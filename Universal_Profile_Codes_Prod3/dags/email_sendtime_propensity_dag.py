import datetime
import time
import json
from datetime import timedelta
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
from datetime import timedelta
# Dependency files and functions
from scripts.universal_profile.email_sendtime_propensity.model_task_preprocess import (
    task_preprocess
)

# ENV Variables
EMAIL = models.Variable.get("email")
PROJECT_ID = models.Variable.get("gcp_project")
EXECUTION_DATE = time.strftime("%Y%m%d")
GCS_BUCKET = models.Variable.get("gcs_bucket")
GCS_DIR_PATH = "data/email_sendtime_propensity"
VM_CREATION_CODE_PATH = "/home/airflow/gcs/dags/scripts/universal_profile/email_sendtime_propensity/vm_api"
VM_INSTANCE_ZONE = "us-central1-a"
VM_INSTANCE_NAME = "email-sendtime-propensity-" + EXECUTION_DATE
VM_INSTANCE_TYPE = "n2-highmem-128"
MODEL_NAME = ["openfact", "clickfact", "lastopen", "subscribed"]
RECOMMENDATION_MODEL_NAME = "email_sendtime_propensity"

# Load conf from GCS to BigQuery
# INTERMEDIATE_TABLE = "up_stg_customer.bn_email_sendtime_propensity" 
FINAL_TABLE_OUTPUT = "o_customer.bn_email_sendtime_propensity"
FINAL_INTERMEDIATE_TABLE = (
        "up_stg_customer.bn_email_sendtime_propensity_" + EXECUTION_DATE
)
FINAL_RECOMMENDATION_CSV = (
        GCS_DIR_PATH + "/output/" + EXECUTION_DATE + "/predicted_emailslot" + ".csv"
)

# Configs

(
    ACTIVITY_SQL_QUERY,
    OPENCOUNT_SQL_QUERY,
    LASTOPEN_SQL_QUERY,
    SUBSCRIPTION_SQL_QUERY,
    ACTIVITY_SQL_CSV_INPUT_FILE,
    OPENCOUNT_SQL_CSV_INPUT_FILE,
    LASTOPEN_SQL_CSV_INPUT_FILE,
    SUBSCRIPTION_SQL_CSV_INPUT_FILE,
    ACTIVITY_SQL_OUTPUT,
    OPENCOUNT_SQL_OUTPUT,
    LASTOPEN_SQL_OUTPUT,
    SUBSCRIPTION_SQL_OUTPUT,
    LOCAL_CONF_PATH,
    GCS_CONF_PATH,
    SCORING_OUTPUT,
) = task_preprocess(
    GCS_BUCKET, GCS_DIR_PATH, MODEL_NAME, EXECUTION_DATE
)


# Dag configuration
DAG_START_DATE = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)
DEFAULT_DAG_ARGS = {
    "owner" : "UP",
    "start_date": datetime.datetime(2022,1,4),
    'email': ['rohit.varma@affine.ai'],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": PROJECT_ID,
}

# Dag tasks
with models.DAG(
        "email_sendtime_propensity",
        schedule_interval= "30 5 1 * *",
        default_args=DEFAULT_DAG_ARGS,
        dagrun_timeout = timedelta(minutes=600),
) as dag:

	# Load local conf files to GCS
	task_setup_conf_files = bash_operator.BashOperator(
		task_id="email_sendtime_setup_conf_files",
		bash_command="sudo gsutil cp {score_file_path} {gcs_conf_path}".format(
			score_file_path=LOCAL_CONF_PATH,
			#recommendation_score_file_path=RECOMMENDATION_LOCAL_CONF_PATH,
			gcs_conf_path=GCS_CONF_PATH,
		),
	)

	# Remove local conf files
	task_rm_local_conf_files = bash_operator.BashOperator(
		task_id="email_sendtime_remove_conf_files",
		bash_command="sudo rm {score_file_path}".format(
			score_file_path=LOCAL_CONF_PATH
			#recommendation_file_name=RECOMMENDATION_LOCAL_CONF_PATH,
		),
	)

	task_check_delete_intermediate_sql_tables = bigquery_operator.BigQueryOperator(
		task_id="email_sendtime_check_delete_intermediate_tables",
		sql="""drop table if exists {0}; drop table if exists {1}; drop table if exists {2}; drop table if exists {3}; drop table if exists {4} """.format(
			ACTIVITY_SQL_OUTPUT,
			OPENCOUNT_SQL_OUTPUT,
			LASTOPEN_SQL_OUTPUT,
			SUBSCRIPTION_SQL_OUTPUT,
			FINAL_INTERMEDIATE_TABLE,
		),
		use_legacy_sql=False,
	)

	# Diamond BigQuery Execution
	task_activity_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="email_sendtime_activity_bq_exe",
		sql=ACTIVITY_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=ACTIVITY_SQL_OUTPUT,
	)

	# Setting BigQuery Execution
	task_opencount_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="email_sendtime_opencount_bq_exec",
		sql=OPENCOUNT_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=OPENCOUNT_SQL_OUTPUT,
	)

	# Jewellery BigQuery Execution
	task_lastopen_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="email_sendtime_lastopen_bq_exec",
		sql=LASTOPEN_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=LASTOPEN_SQL_OUTPUT,
	)

	# lastpurchase BigQuery Execution
	task_subscription_bq_exec = bigquery_operator.BigQueryOperator(
		task_id="email_sendtime_subscription_bq_exec",
		sql=SUBSCRIPTION_SQL_QUERY,
		use_legacy_sql=False,
		destination_dataset_table=SUBSCRIPTION_SQL_OUTPUT,
	)

	# Diamond BigQuery to GCS
	task_activity_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="email_sendtime_activity_export_query_data_gcs",
		source_project_dataset_table=ACTIVITY_SQL_OUTPUT,
		destination_cloud_storage_uris=ACTIVITY_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	# Setting BigQuery to GCS
	task_opencount_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="email_sendtime_opencount_export_query_data_gcs",
		source_project_dataset_table=OPENCOUNT_SQL_OUTPUT,
		destination_cloud_storage_uris=OPENCOUNT_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	# Jewellery BigQuery to GCS
	task_lastopen_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="email_sendtime_lastopen_export_query_data_gcs",
		source_project_dataset_table=LASTOPEN_SQL_OUTPUT,
		destination_cloud_storage_uris=LASTOPEN_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	# Jewellery BigQuery to GCS
	task_subscription_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id="email_sendtime_subscription_export_query_data_gcs",
		source_project_dataset_table=SUBSCRIPTION_SQL_OUTPUT,
		destination_cloud_storage_uris=SUBSCRIPTION_SQL_CSV_INPUT_FILE,
		export_format="CSV",
	)

	#Create VM and Prepare the instance and run model scoring
	task_vm_creation_run_scoring = bash_operator.BashOperator(
		task_id="email_sendtime_vm_creation_run_scoring",
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
		task_id="email_sendtime_vm_deletion",
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

	task_append_final_csv_output_to_intermediate_table = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
		task_id="email_sendtime_append_final_csv_output",
		destination_project_dataset_table=FINAL_INTERMEDIATE_TABLE,
		bucket=GCS_BUCKET,
		source_objects=[FINAL_RECOMMENDATION_CSV],
		autodetect=True,
		source_format="CSV",
		skip_leading_rows=1,
		create_disposition="CREATE_IF_NEEDED",
		write_disposition="WRITE_APPEND",
	)

	task_insert_from_intermediate_to_final = bigquery_operator.BigQueryOperator(
		task_id="email_sendtime_insert_from_intermediate_to_final",
		sql="""insert into bnile-cdw-prod.{FINAL_TABLE_OUTPUT} 
            (SELECT bnid, year, month_name, day_of_week, preferred_time_slot, cast(preferred_send_time as String) 
            FROM `bnile-cdw-prod.{FINAL_INTERMEDIATE_TABLE}`);""".format(
			FINAL_TABLE_OUTPUT=FINAL_TABLE_OUTPUT,
			FINAL_INTERMEDIATE_TABLE=FINAL_INTERMEDIATE_TABLE,
		),
		use_legacy_sql=False,
	)

	task_email_on_success = EmailOperator(
		task_id="send_mail_on_success",
		to='rohit.varma@affine.ai',
		subject='UP ETL Alert:Success(email_sendtime_propensity)',
		html_content='''
		Hi,<br>
		
		<p>This email is to inform you that, email send propensity pipeline has been successfully completed.<br>
			<br>
		-<br>
		UP Team.<p>''',
		dag=dag
	)


task_setup_conf_files >> task_rm_local_conf_files
task_rm_local_conf_files >> task_activity_bq_exec >> task_activity_export_query_data_gcs >> task_vm_creation_run_scoring
task_rm_local_conf_files >> task_opencount_bq_exec >> task_opencount_export_query_data_gcs >> task_vm_creation_run_scoring
task_rm_local_conf_files >> task_lastopen_bq_exec >> task_lastopen_export_query_data_gcs >> task_vm_creation_run_scoring
task_rm_local_conf_files >> task_subscription_bq_exec >> task_subscription_export_query_data_gcs >> task_vm_creation_run_scoring
task_vm_creation_run_scoring >> task_vm_deletion >>  task_append_final_csv_output_to_intermediate_table
task_append_final_csv_output_to_intermediate_table>> task_insert_from_intermediate_to_final >> task_check_delete_intermediate_sql_tables
task_check_delete_intermediate_sql_tables >> task_email_on_success