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
from scripts.universal_profile.campaign_ranking.bq_conf import PARAMS_QUERY
from scripts.universal_profile.campaign_ranking.model_task_preprocess import campaign_ranking_task_preprocess


# ENV Variables
EMAIL = models.Variable.get("email")
PROJECT_ID = models.Variable.get("gcp_project")
EXECUTION_DATE = time.strftime("%Y%m%d")
GCS_BUCKET = models.Variable.get("gcs_bucket")
GCS_DIR_PATH = "data/campaign_ranking"
VM_CREATION_CODE_PATH = "/home/airflow/gcs/dags/scripts/universal_profile/campaign_ranking/vm_api"
VM_INSTANCE_ZONE = "us-central1-a"
VM_INSTANCE_NAME = "campaign-ranking-" + EXECUTION_DATE
VM_INSTANCE_TYPE = "n1-standard-16"
campaign_ranking_model_name = "campaign_ranking"

# LOad conf from GCS to BigQuery
FINAL_INTERMEDIATE_TABLE = "bnile-cdw-prod.up_stg_customer.trigger_campaign_ranking_" +EXECUTION_DATE
FINAL_TABLE_OUTPUT = "bnile-cdw-prod.o_customer.trigger_campaign_ranking"
FINAL_RECOMMENDATION_CSV = (
    GCS_DIR_PATH + "/output/" + EXECUTION_DATE + "/campaign_ranking.csv"
)

(
    CAMPAIGN_RANKING_SQL_QUERY,
    CAMPAIGN_RANKING_SQL_CSV_INPUT_FILE,
    CAMPAIGN_RANKING_SQL_OUTPUT,
    CAMPAIGN_RANKING_LOCAL_CONF_PATH,
    GCS_CONF_PATH,
    CAMPAIGN_RANKING_SCORING_OUTPUT,
) = campaign_ranking_task_preprocess(
    GCS_BUCKET, GCS_DIR_PATH, campaign_ranking_model_name, EXECUTION_DATE
)

# Dag configuration
DAG_START_DATE = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)
DEFAULT_DAG_ARGS = {
    "owner": "UP",
    "start_date": datetime.datetime(2022,3,2),
    'email': ['rohit.varma@affine.ai'],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": PROJECT_ID,
}

# Dag tasks
with models.DAG(
    "campaign_ranking_dag",
    schedule_interval='30 14 3 * *',
    default_args=DEFAULT_DAG_ARGS,
) as dag:

    task_start = dummy_operator.DummyOperator(task_id="start_recommendation_set_1", dag=dag)

    # Load local conf files to GCS
    task_setup_conf_files = bash_operator.BashOperator(
        task_id="setup_conf_files",
        bash_command="sudo gsutil cp {campaign_ranking_score_file_path} {gcs_conf_path}".format(
            campaign_ranking_score_file_path=CAMPAIGN_RANKING_LOCAL_CONF_PATH,
            gcs_conf_path=GCS_CONF_PATH,
        ),
    )

    # Remove local conf files
    task_rm_local_conf_files = bash_operator.BashOperator(
        task_id="rm_local_conf_files",
        bash_command="sudo rm {campaign_ranking_score_file_path}".format(
            campaign_ranking_score_file_path=CAMPAIGN_RANKING_LOCAL_CONF_PATH
        ),
    )

    # Diamond BigQuery Execution
    task_base_trigger_campaign_ranking_table = bigquery_operator.BigQueryOperator(
        task_id="base_trigger_campaign_ranking_table",
        sql=CAMPAIGN_RANKING_SQL_QUERY,
        use_legacy_sql=False,
        destination_dataset_table=CAMPAIGN_RANKING_SQL_OUTPUT,
    )

    # Diamond BigQuery to GCS
    task_base_trigger_campaign_table_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="base_trigger_campaign_table_data_gcs",
        source_project_dataset_table=CAMPAIGN_RANKING_SQL_OUTPUT,
        destination_cloud_storage_uris=CAMPAIGN_RANKING_SQL_CSV_INPUT_FILE,
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
    
    task_update_column_value = bigquery_operator.BigQueryOperator(
		task_id="update_column_value",
		sql=""" update {FINAL_INTERMEDIATE_TABLE}
                set revenue=0.0
                where revenue is null;

                update {FINAL_INTERMEDIATE_TABLE}
                set margin=0.0
                where margin is null;

                update {FINAL_INTERMEDIATE_TABLE}
                set sends=0
                where sends is null;

                update {FINAL_INTERMEDIATE_TABLE}
                set clicks=0
                where clicks is null;

                update {FINAL_INTERMEDIATE_TABLE}
                set click_ratio=0.0
                where click_ratio is null;""".format(
			FINAL_INTERMEDIATE_TABLE=FINAL_INTERMEDIATE_TABLE,
		),
		use_legacy_sql=False,
	)
    
    task_insert_from_intermediate_to_final = bigquery_operator.BigQueryOperator(
		task_id="insert_from_intermediate_to_final",
		sql="""insert into {FINAL_TABLE_OUTPUT} 
            (SELECT placement_key			
            ,placement_name			
            ,campaign_id			
            ,year			
            ,month			
            ,sends			
            ,clicks			
            ,click_ratio			
            ,revenue			
            ,margin			
            ,topsis_score_response			
            ,topsis_score_revenue			
            ,topsis_score_overall			
            ,response_rank			
            ,revenue_rank			
            ,overall_rank			
            ,refresh_date		
            ,ranking_month		
            FROM {FINAL_INTERMEDIATE_TABLE});""".format(
			FINAL_TABLE_OUTPUT=FINAL_TABLE_OUTPUT,
			FINAL_INTERMEDIATE_TABLE=FINAL_INTERMEDIATE_TABLE,
		),
		use_legacy_sql=False,
	)

    # Delete intermediate tables
    task_delete_intermediate_sql_tables = bigquery_operator.BigQueryOperator(
        task_id="delete_intermediate_tables",
        sql="""drop table if exists {0}; drop table if exists {1};""".format(
            CAMPAIGN_RANKING_SQL_OUTPUT,
            FINAL_INTERMEDIATE_TABLE
        ),
        use_legacy_sql=False,
    )
    
    task_email_on_success = EmailOperator(
    task_id="send_mail_on_success", 
    to='rohit.varma@affine.ai',
    subject='UP ETL Alert:Success(trigger_campaign_ranking)',
    html_content=''' 
    Hi,<br>
    
    <p>This email is to inform you that, recommendation_set_1 pipeline has been successfully completed.<br>
           <br>
    -<br>
    UP Team.<p>''',
    dag=dag)

    task_end = dummy_operator.DummyOperator(task_id="end_recommendation_set_1", dag=dag)

    task_start >> task_setup_conf_files >> task_rm_local_conf_files

    task_rm_local_conf_files >> task_base_trigger_campaign_ranking_table >> task_base_trigger_campaign_table_data_gcs >> task_vm_creation_run_scoring

    task_vm_creation_run_scoring >> task_vm_deletion >> task_append_final_csv_output_to_intermediate_table >> task_update_column_value >> task_insert_from_intermediate_to_final
    task_insert_from_intermediate_to_final >>  task_delete_intermediate_sql_tables >> task_end
    task_delete_intermediate_sql_tables >> task_email_on_success
    