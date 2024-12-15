from datetime import datetime, timedelta
import time
import json
import airflow
from airflow import models
from airflow.operators import (
    bash_operator,
    python_operator,
    dummy_operator,
    email_operator,
)
from airflow.contrib.operators import bigquery_to_gcs, bigquery_operator, gcs_to_bq
from airflow.utils import trigger_rule
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Dependency files and functions
from scripts.universal_profile.item_item_similarity_codes.model_task_preprocess import (
    setting_task_preprocess,
    jewellery_task_preprocess,
    # recommendation_task_preprocess,
)

# ENV Variables
# EMAIL = models.Variable.get("email")
PROJECT_ID = models.Variable.get("gcp_project")
EXECUTION_DATE = time.strftime("%Y%m%d")
exec_date = int(EXECUTION_DATE)
GCS_BUCKET = models.Variable.get("gcs_bucket")
GCS_DIR_PATH = "data/item_item_similarity_codes"
VM_CREATION_CODE_PATH = "/home/airflow/gcs/dags/scripts/universal_profile/item_item_similarity_codes/vm_api"
VM_INSTANCE_ZONE = "us-central1-a"
VM_INSTANCE_NAME = "ds-model-item-item-deploy-automation-" + EXECUTION_DATE
VM_INSTANCE_TYPE = "n1-standard-16"
SETTING_MODEL_NAME = "settings"
JEWELLERY_MODEL_NAME = "jewellery"
# RECOMMENDATION_MODEL_NAME = "recommendation"


# LOad conf from GCS to BigQuery
FINAL_TABLE_OUTPUT = "o_customer.recommendation_offer_offer_similarity"
FINAL_INTERMEDIATE_TABLE_1 = (
    "up_stg_customer.jewellery_intermediate_offer_offer_final" + EXECUTION_DATE
)
FINAL_INTERMEDIATE_TABLE_2 = (
    "up_stg_customer.settings_intermediate_offer_offer_final" + EXECUTION_DATE
)
FINAL_RECOMMENDATION_CSV_1 = (
    GCS_DIR_PATH + "/output/jewellery_" + EXECUTION_DATE + "/jewellery_output.csv"
)
FINAL_RECOMMENDATION_CSV_2 = (
    GCS_DIR_PATH + "/output/settings_" + EXECUTION_DATE + "/settings_output.csv"
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

# # Recommendation configs
# RECOMMENDATION_LOCAL_CONF_PATH, GCS_CONF_PATH = recommendation_task_preprocess(
#     DIAMOND_SCORING_OUTPUT,
#     SETTING_SCORING_OUTPUT,
#     JEWELLERY_SCORING_OUTPUT,
#     GCS_BUCKET,
#     GCS_DIR_PATH,
#     RECOMMENDATION_MODEL_NAME,
#     EXECUTION_DATE,
# )

# # Dag configuration
# DAG_START_DATE = datetime.datetime.combine(
#     datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
# )
DEFAULT_DAG_ARGS = {
    'start_date': datetime(2022,3,24),
    "email": 'rohit.varma@affine.ai',
    "owner": 'UP',
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": PROJECT_ID,
}

# Dag tasks
with models.DAG(
    "item-item-similarity",
    schedule_interval='45 19 25 * *',
    default_args=DEFAULT_DAG_ARGS,
) as dag:

    task_start = dummy_operator.DummyOperator(task_id="start_item_item_similarity", dag=dag)

    # Load local conf files to GCS
    task_setup_conf_files = bash_operator.BashOperator(
        task_id="setup_conf_files",
        bash_command="sudo gsutil cp {setting_score_file_path} {jewellery_score_file_path} {gcs_conf_path}".format(
            setting_score_file_path=SETTING_LOCAL_CONF_PATH,
            jewellery_score_file_path=JEWELLERY_LOCAL_CONF_PATH,
            # recommendation_score_file_path=RECOMMENDATION_LOCAL_CONF_PATH,
            gcs_conf_path=GCS_CONF_PATH,
        ),
    )

    # Remove local conf files
    task_rm_local_conf_files = bash_operator.BashOperator(
        task_id="rm_local_conf_files",
        bash_command="sudo rm {setting_file_name} {jewellery_file_name}".format(
            setting_file_name=SETTING_LOCAL_CONF_PATH,
            jewellery_file_name=JEWELLERY_LOCAL_CONF_PATH,
            # recommendation_file_name=RECOMMENDATION_LOCAL_CONF_PATH,
        ),
    )

    # # Diamond BigQuery Execution
    # task_diamond_bq_exec_3 = bigquery_operator.BigQueryOperator(
    #     task_id="diamond_bq_exec_3",
    #     sql=DIAMOND_SQL_QUERY,
    #     use_legacy_sql=False,
    #     destination_dataset_table=DIAMOND_SQL_OUTPUT,
    # )

    # Setting BigQuery Execution
    task_setting_bq_exec = bigquery_operator.BigQueryOperator(
        task_id="setting_bq_exec",
        sql=SETTING_SQL_QUERY,
        use_legacy_sql=False,
        destination_dataset_table=SETTING_SQL_OUTPUT,
    )

    # Jewellery BigQuery Execution
    task_jewellery_bq_exec = bigquery_operator.BigQueryOperator(
        task_id="jewellery_bq_exec",
        sql=JEWELLERY_SQL_QUERY,
        use_legacy_sql=False,
        destination_dataset_table=JEWELLERY_SQL_OUTPUT,
    )

    # # Diamond BigQuery to GCS
    # task_diamond_export_query_data_gcs_3 = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    #     task_id="diamond_export_query_data_gcs_3",
    #     source_project_dataset_table=DIAMOND_SQL_OUTPUT,
    #     destination_cloud_storage_uris=DIAMOND_SQL_CSV_INPUT_FILE,
    #     export_format="CSV",
    # )

    # Setting BigQuery to GCS
    task_setting_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="setting_export_query_data_gcs",
        source_project_dataset_table=SETTING_SQL_OUTPUT,
        destination_cloud_storage_uris=SETTING_SQL_CSV_INPUT_FILE,
        export_format="CSV",
    )

    # Jewellery BigQuery to GCS
    task_jewellery_export_query_data_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="jewellery_export_query_data_gcs",
        source_project_dataset_table=JEWELLERY_SQL_OUTPUT,
        destination_cloud_storage_uris=JEWELLERY_SQL_CSV_INPUT_FILE,
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

    task_append_final_csv_output_to_table_1 = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="append_final_csv_output_to_table_1",
        destination_project_dataset_table=FINAL_INTERMEDIATE_TABLE_1,
        bucket=GCS_BUCKET,
        source_objects=[FINAL_RECOMMENDATION_CSV_1],
        autodetect=True,
        source_format="CSV",
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    task_append_final_csv_output_to_table_2 = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="append_final_csv_output_to_table_2",
        destination_project_dataset_table=FINAL_INTERMEDIATE_TABLE_2,
        bucket=GCS_BUCKET,
        source_objects=[FINAL_RECOMMENDATION_CSV_2],
        autodetect=True,
        source_format="CSV",
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    task_insert_from_intermediate_to_final = bigquery_operator.BigQueryOperator(
        task_id="insert_from_intermediate_to_final",
        sql=f'''delete from `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}` where 1=1;
        insert into `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}`
        SELECT
        Seed_OID
        ,OID_rank1
        ,OID_rank2
        ,OID_rank3
        ,OID_rank4
        ,OID_rank5
        ,OID_rank6
        ,OID_rank7
        ,OID_rank8
        ,OID_rank9
        ,OID_rank10
        ,OID_type
        ,null
        FROM
        `{PROJECT_ID}.{FINAL_INTERMEDIATE_TABLE_1}`;
        insert into `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}`
        SELECT
        Seed_OID
        ,OID_rank1
        ,OID_rank2
        ,OID_rank3
        ,OID_rank4
        ,OID_rank5
        ,OID_rank6
        ,OID_rank7
        ,OID_rank8
        ,OID_rank9
        ,OID_rank10
        ,OID_type
        ,null
        FROM
        `{PROJECT_ID}.{FINAL_INTERMEDIATE_TABLE_2}`;
        update `{PROJECT_ID}.{FINAL_TABLE_OUTPUT}` set Model_run_date = {exec_date} where 1=1'''.format(
            PROJECT_ID,
            FINAL_TABLE_OUTPUT,
            FINAL_INTERMEDIATE_TABLE_1,
            FINAL_INTERMEDIATE_TABLE_2,
            exec_date,
        ),
        use_legacy_sql=False,
    )

    # Delete intermediate tables
    task_delete_intermediate_sql_tables = bigquery_operator.BigQueryOperator(
        task_id="delete_intermediate_tables",
        sql="""drop table if exists {0}; drop table if exists {1}; drop table if exists {2}; drop table if exists {3}""".format(
            SETTING_SQL_OUTPUT,
            JEWELLERY_SQL_OUTPUT,
            FINAL_INTERMEDIATE_TABLE_1,
            FINAL_INTERMEDIATE_TABLE_2,
        ),
        use_legacy_sql=False,
    )



    task_end = dummy_operator.DummyOperator(task_id="end_item_item_similarity", dag=dag)


    task_start >> task_setup_conf_files >> task_rm_local_conf_files

    task_rm_local_conf_files >> task_setting_bq_exec >> task_setting_export_query_data_gcs >> task_vm_creation_run_scoring
    task_rm_local_conf_files >> task_jewellery_bq_exec >> task_jewellery_export_query_data_gcs >> task_vm_creation_run_scoring

    task_vm_creation_run_scoring >> task_vm_deletion
    task_vm_deletion >> task_append_final_csv_output_to_table_1 >> task_insert_from_intermediate_to_final
    task_vm_deletion >> task_append_final_csv_output_to_table_2 >> task_insert_from_intermediate_to_final
    task_insert_from_intermediate_to_final >> task_delete_intermediate_sql_tables >> task_end

