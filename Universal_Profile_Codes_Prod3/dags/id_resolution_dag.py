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
from scripts.universal_profile.id_resolution.bnq_conf import PARAMS_QUERY
from scripts.universal_profile.id_resolution.model_task_preprocess import id_resolution_task_preprocess


# ENV Variables
EMAIL = models.Variable.get("email")
PROJECT_ID = models.Variable.get("gcp_project")
EXECUTION_DATE = time.strftime("%Y%m%d")
GCS_BUCKET = models.Variable.get("gcs_bucket")
GCS_DIR_PATH = "data/id_resolution"
VM_CREATION_CODE_PATH = "/home/airflow/gcs/dags/scripts/universal_profile/id_resolution/vm_api"
VM_INSTANCE_ZONE = "us-central1-a"
VM_INSTANCE_NAME = "id-resolution-" + EXECUTION_DATE
VM_INSTANCE_TYPE = "n2-highmem-128" 
# VM_INSTANCE_TYPE = "n2d-standard-128"
# VM_INSTANCE_TYPE = "c2-standard-60"
id_resolution_model_name = ['address','email_base1','email_base2', 'payment', 'phone_base1', 'phone_base2','id_resolution']


# LOad conf from GCS to BigQuery
FINAL_INTERMEDIATE_TABLE = "bnile-cdw-prod.up_stg_customer.id_resolution_" +EXECUTION_DATE
FINAL_TABLE_OUTPUT = "bnile-cdw-prod.o_customer.id_resolution"
FINAL_RECOMMENDATION_CSV = (
    GCS_DIR_PATH + "/output/" + EXECUTION_DATE + "/id_resolution.csv"
)

(
    ADDRESS_SQL,
    EMAIL_BASE1_SQL,
    EMAIL_BASE2_SQL,
    PAYMENT_SQL,
    PHONE_BASE1_SQL,
    PHONE_BASE2_SQL,
    
    ID_RESOLUTION_ADDRESS_SQL_CSV_INPUT_FILE,
    ID_RESOLUTION_EMAIIL_BASE1_SQL_CSV_INPUT_FILE,
    ID_RESOLUTION_EMAIL_BASE2_SQL_CSV_INPUT_FILE,
    ID_RESOLUTION_PAYMENT_SQL_CSV_INPUT_FILE,
    ID_RESOLUTION_PHONE_BASE1_SQL_CSV_INPUT_FILE,
    ID_RESOLUTION_PHONE_BASE2_SQL_CSV_INPUT_FILE,
    
    ADDRESS_SQL_OUTPUT,
    EMAIL_BASE1_SQL_OUTPUT,
    EMAIL_BASE2_SQL_OUTPUT,
    PAYMENT_SQL_OUTPUT,
    PHONE_BASE1_SQL_OUTPUT,
    PHONE_BASE2_SQL_OUTPUT,
    
    ID_RESOLUTION_LOCAL_CONF_PATH,
    GCS_CONF_PATH,
    ID_RESOLUTION_SCORING_OUTPUT,
) = id_resolution_task_preprocess(
    GCS_BUCKET, GCS_DIR_PATH, id_resolution_model_name, EXECUTION_DATE
)

# Dag configuration
DAG_START_DATE = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)
DEFAULT_DAG_ARGS = {
    "owner": "UP",
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
    "id_resolution_dag",
    schedule_interval=None,
    default_args=DEFAULT_DAG_ARGS,
) as dag:

    task_start = dummy_operator.DummyOperator(task_id="start_id_resolution", dag=dag)

    # Load local conf files to GCS
    task_setup_conf_files = bash_operator.BashOperator(
        task_id="setup_conf_files",
        bash_command="sudo gsutil cp {id_resolution_score_file_path} {gcs_conf_path}".format(
            id_resolution_score_file_path=ID_RESOLUTION_LOCAL_CONF_PATH,
            gcs_conf_path=GCS_CONF_PATH,
        ),
    )

    # Remove local conf files
    task_rm_local_conf_files = bash_operator.BashOperator(
        task_id="rm_local_conf_files",
        bash_command="sudo rm {id_resolution_score_file_path}".format(
            id_resolution_score_file_path=ID_RESOLUTION_LOCAL_CONF_PATH
        ),
    )

    # address BigQuery Execution
    task_address_id_resolution_table = bigquery_operator.BigQueryOperator(
        task_id="address_id_resolution_table",
        sql=ADDRESS_SQL,
        use_legacy_sql=False,
        destination_dataset_table=ADDRESS_SQL_OUTPUT,
    )
    
    # email_base1 BigQuery Execution
    task_email_base1_id_resolution_table = bigquery_operator.BigQueryOperator(
        task_id="email_base1_id_resolution_table",
        sql=EMAIL_BASE1_SQL,
        use_legacy_sql=False,
        destination_dataset_table=EMAIL_BASE1_SQL_OUTPUT,
    )
    
    # email_base2 BigQuery Execution
    task_email_base2_id_resolution_table = bigquery_operator.BigQueryOperator(
        task_id="email_base2_id_resolution_table",
        sql=EMAIL_BASE2_SQL,
        use_legacy_sql=False,
        destination_dataset_table=EMAIL_BASE2_SQL_OUTPUT,
    )
    
    # payment BigQuery Execution
    task_payment_id_resolution_table = bigquery_operator.BigQueryOperator(
        task_id="payment_id_resolution_table",
        sql=PAYMENT_SQL,
        use_legacy_sql=False,
        destination_dataset_table=PAYMENT_SQL_OUTPUT,
    )
    
    # phone_base1 BigQuery Execution
    task_phone_base1_id_resolution_table = bigquery_operator.BigQueryOperator(
        task_id="phone_base1_id_resolution_table",
        sql=PHONE_BASE1_SQL,
        use_legacy_sql=False,
        destination_dataset_table=PHONE_BASE1_SQL_OUTPUT,
    )
    
    # phone_base2 BigQuery Execution
    task_phone_base2_id_resolution_table = bigquery_operator.BigQueryOperator(
        task_id="phone_base2_id_resolution_table",
        sql=PHONE_BASE2_SQL,
        use_legacy_sql=False,
        destination_dataset_table=PHONE_BASE2_SQL_OUTPUT,
    )
    
    # address data to gcs
    task_address_id_resolution_table_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="address_id_resolution_table_data_gcs",
        source_project_dataset_table=ADDRESS_SQL_OUTPUT,
        destination_cloud_storage_uris=ID_RESOLUTION_ADDRESS_SQL_CSV_INPUT_FILE,
        export_format="CSV",
    )
    
    # email_base1 data to gcs
    task_email_base1_id_resolution_table_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="email_base1_id_resolution_table_data_gcs",
        source_project_dataset_table=EMAIL_BASE1_SQL_OUTPUT,
        destination_cloud_storage_uris=ID_RESOLUTION_EMAIIL_BASE1_SQL_CSV_INPUT_FILE,
        export_format="CSV",
    )
    
    # email_base2 data to gcs
    task_email_base2_id_resolution_table_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="email_base2_id_resolution_table_data_gcs",
        source_project_dataset_table=EMAIL_BASE2_SQL_OUTPUT,
        destination_cloud_storage_uris=ID_RESOLUTION_EMAIL_BASE2_SQL_CSV_INPUT_FILE,
        export_format="CSV",
    )
    
    # payment data to gcs
    task_payment_id_resolution_table_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="payment_id_resolution_table_data_gcs",
        source_project_dataset_table=PAYMENT_SQL_OUTPUT,
        destination_cloud_storage_uris=ID_RESOLUTION_PAYMENT_SQL_CSV_INPUT_FILE,
        export_format="CSV",
    )
    
    # phone_base1 data to gcs
    task_phone_base1_id_resolution_table_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="phone_base1_id_resolution_table_data_gcs",
        source_project_dataset_table=PHONE_BASE1_SQL_OUTPUT,
        destination_cloud_storage_uris=ID_RESOLUTION_PHONE_BASE1_SQL_CSV_INPUT_FILE,
        export_format="CSV",
    )
    
    # phone_base2 data to gcs
    task_phone_base2_id_resolution_table_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="phone_base2_id_resolution_table_data_gcs",
        source_project_dataset_table=PHONE_BASE2_SQL_OUTPUT,
        destination_cloud_storage_uris=ID_RESOLUTION_PHONE_BASE2_SQL_CSV_INPUT_FILE,
        export_format="CSV",
    )

    # Create VM and Prepare the instance and run model scoring
    task_vm_creation_run_scoring = bash_operator.BashOperator(
        task_id="vm_creation_run_scoring",
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

    # Delete VM
    task_vm_deletion = bash_operator.BashOperator(
        task_id="vm_deletion",
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
        task_id="append_final_csv_output_to_intermediate_table",
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
		task_id="insert_from_intermediate_to_final",
		sql="""create or replace table {FINAL_TABLE_OUTPUT} as
            select * from {FINAL_INTERMEDIATE_TABLE}""".format(
			FINAL_TABLE_OUTPUT=FINAL_TABLE_OUTPUT,
			FINAL_INTERMEDIATE_TABLE=FINAL_INTERMEDIATE_TABLE,
		),
		use_legacy_sql=False,
	)

    # Delete intermediate tables
    task_delete_intermediate_sql_tables = bigquery_operator.BigQueryOperator(
        task_id="delete_intermediate_tables",
        sql="""drop table if exists {0}; drop table if exists {1}; drop table if exists {2};
        drop table if exists {3}; drop table if exists {4}; drop table if exists {5}; drop table if exists {6};
        """.format(
            ADDRESS_SQL_OUTPUT,
            EMAIL_BASE1_SQL_OUTPUT,
            EMAIL_BASE2_SQL_OUTPUT,
            PAYMENT_SQL_OUTPUT,
            PHONE_BASE1_SQL_OUTPUT,
            PHONE_BASE2_SQL_OUTPUT,
            FINAL_INTERMEDIATE_TABLE
        ),
        use_legacy_sql=False,
    )
    
    task_email_on_success = EmailOperator(
    task_id="send_mail_on_success", 
    to='rohit.varma@affine.ai',
    subject='UP ETL Alert:Success(id_resolution)',
    html_content=''' 
    Hi,<br>
    
    <p>This email is to inform you that, recommendation_set_1 pipeline has been successfully completed.<br>
           <br>
    -<br>
    UP Team.<p>''',
    dag=dag)

    task_end = dummy_operator.DummyOperator(task_id="end_id_resolution", dag=dag)

    task_start >> task_setup_conf_files >> task_rm_local_conf_files

    task_rm_local_conf_files >> task_address_id_resolution_table >> task_address_id_resolution_table_data_gcs >> task_vm_creation_run_scoring
    task_rm_local_conf_files >> task_email_base1_id_resolution_table >> task_email_base1_id_resolution_table_data_gcs >> task_vm_creation_run_scoring
    task_rm_local_conf_files >> task_email_base2_id_resolution_table >> task_email_base2_id_resolution_table_data_gcs >> task_vm_creation_run_scoring
    task_rm_local_conf_files >> task_payment_id_resolution_table >> task_payment_id_resolution_table_data_gcs >> task_vm_creation_run_scoring
    task_rm_local_conf_files >> task_phone_base1_id_resolution_table >> task_phone_base1_id_resolution_table_data_gcs >> task_vm_creation_run_scoring
    task_rm_local_conf_files >> task_phone_base2_id_resolution_table >> task_phone_base2_id_resolution_table_data_gcs >> task_vm_creation_run_scoring

    task_vm_creation_run_scoring >> task_vm_deletion >> task_append_final_csv_output_to_intermediate_table >> task_insert_from_intermediate_to_final
    task_insert_from_intermediate_to_final >>  task_delete_intermediate_sql_tables >> task_end
    task_delete_intermediate_sql_tables >> task_email_on_success
    