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



# ENV Variables
EMAIL = models.Variable.get("email")
PROJECT_ID = models.Variable.get("gcp_project")
EXECUTION_DATE = time.strftime("%Y%m%d")
GCS_BUCKET = models.Variable.get("gcs_bucket")
GCS_DIR_PATH = "data/review_data_model"
VM_CREATION_CODE_PATH = "/home/airflow/gcs/dags/scripts/universal_profile/reviews_pii_model/vm_api"
VM_INSTANCE_ZONE = "us-central1-a"
VM_INSTANCE_NAME = "review-data-model-" + EXECUTION_DATE
VM_INSTANCE_TYPE = "n1-highmem-32"
MODEL_NAME = ["jewellery", "setting", "diamond", "lastpurchase"]
RECOMMENDATION_MODEL_NAME = "reviews_pii_model"

# Load conf from GCS to BigQuery
FINAL_TABLE_OUTPUT = "o_customer.reviews_pii"
FINAL_INTERMEDIATE_TABLE = (
        "o_customer.reviews_pii_" + EXECUTION_DATE
)
FINAL_RECOMMENDATION_CSV = (
        GCS_DIR_PATH + "/output/" + "reviews_pii_" + EXECUTION_DATE + ".csv"
)

# Configs



# Dag configuration
DAG_START_DATE = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)
DEFAULT_DAG_ARGS = {
    "owner":'UP',
    "start_date": DAG_START_DATE,
    'email': ['rohit.varma@affine.ai'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": PROJECT_ID,
}

# Dag tasks
with models.DAG(
        "review_data_model",
        schedule_interval=None,
        default_args=DEFAULT_DAG_ARGS,
) as dag:

	task_check_delete_intermediate_sql_tables = bigquery_operator.BigQueryOperator(
		task_id="review_data_check_delete_intermediate_tables",
		sql="""drop table if exists {0}; """.format(
			FINAL_INTERMEDIATE_TABLE
		),
		use_legacy_sql=False,
	)

	#Create VM and Prepare the instance and run model scoring
	task_vm_creation_run_scoring = bash_operator.BashOperator(
		task_id="review_data_vm_creation_run_scoring",
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
		task_id="review_data_model_vm_deletion",
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
		task_id="review_data_model_append_final_csv_output_to_inter_table",
		destination_project_dataset_table=FINAL_INTERMEDIATE_TABLE,
		bucket=GCS_BUCKET,
		source_objects=[FINAL_RECOMMENDATION_CSV],
		#quote_character="",
		#encoding='UTF-8',
		autodetect=True,
		source_format="CSV",
		skip_leading_rows=1,
		allow_quoted_newlines=True,
		create_disposition="CREATE_IF_NEEDED",
		write_disposition="WRITE_TRUNCATE",
	)

	task_insert_from_intermediate_to_final = bigquery_operator.BigQueryOperator(
		task_id="review_data_insert_from_intermediate_to_final",
		sql="""delete from `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}` where run_date IN (
			select distinct RUN_DATE from `{PROJECT_ID}.{FINAL_INTERMEDIATE_TABLE}` );
			insert into `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}`
		SELECT
		cast(offer_key as int64) offer_key
		,cast(offer_id as STRING) offer_id
		,cast(review_date_key as int64) review_date_key
		,cast(review_date as datetime) review_date
		,cast(review_title as STRING) review_title
		,cast(review_content as STRING) review_content
		,cast(review_score as int64) review_score
		,cast(published as STRING) published
		,cast(offer_url as STRING) offer_url
		,cast(product_image_url as STRING) product_image_url
		,cast(reviewer_display_name as STRING) reviewer_display_name
		,cast(reviewer_email_address as STRING) reviewer_email_address
		,cast(reviewer_country as STRING) reviewer_country
		,cast(reviewer_ip_address as STRING) reviewer_ip_address
		,cast(form_recommendation as STRING) form_recommendation
		,cast(questionaire_recommendation as STRING) questionaire_recommendation
		,cast(questionnaire_why_bluenile as STRING) questionnaire_why_bluenile
		,cast(yotpo_id as int64) yotpo_id
		,cast(user_type as STRING) user_type
		,cast(appkey as STRING) appkey
		,cast(published_image_url as STRING) published_image_url
		,cast(unpublished_image_url as STRING) unpublished_image_url
		,cast(published_video_url as STRING) published_video_url
		,cast(unpublished_video_url as STRING) unpublished_video_url
		,cast(RUN_DATE as DATE) RUN_DATE
		,cast(gender as STRING) gender
		,cast(marital_status as STRING) marital_status
		,cast(anniversary_month as STRING) anniversary_month
		,cast(wedding_month as STRING) wedding_month
		,cast(wedding_year as int64) wedding_year
		,cast(RELATIONS_BIRTHDAY_FLAGS as STRING) RELATIONS_BIRTHDAY_FLAGS
		,cast(RELATIONS_BIRTHDAY_MONTH as STRING) RELATIONS_BIRTHDAY_MONTH
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
		to='rohit.varma@affine.ai',
		subject='UP ETL Alert:Success(review_data_model)',
		html_content='''
		Hi,<br>
		
		<p>This email is to inform you that, review_data_model pipeline has been successfully completed.<br>
			<br>
		-<br>
		UP Team.<p>''',
		dag=dag
	)

	# task_end = dummy_operator.DummyOperator(task_id="end_recommendation_set_2", dag=dag)
	# task_start >> task_check_delete_intermediate_sql_tables >> task_setup_conf_files >> task_rm_local_conf_files
task_vm_creation_run_scoring >> task_vm_deletion >> task_append_final_csv_output_to_table >> task_insert_from_intermediate_to_final >> task_email_on_success
task_insert_from_intermediate_to_final >> task_check_delete_intermediate_sql_tables