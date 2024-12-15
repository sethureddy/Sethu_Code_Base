import datetime
import time

from airflow import models

from airflow.operators import (
    bash_operator,
    python_operator,
    dummy_operator,
    email_operator,
)
from airflow.utils import trigger_rule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.email_operator import EmailOperator

from scripts.universal_profile.bn_lc_pj_attributes import bn_lc_pj_attributes
bn_lc_pj_attributes_sql=bn_lc_pj_attributes()

from scripts.universal_profile.long_conversion_pyfiles.model_task_preprocess import (
    get_lc_queries,
    get_lc_scoring_params,
    get_final_table_insert_query,
)

GCS_BUCKET = models.Variable.get("gcs_bucket")
PROJECT_ID = models.Variable.get("gcp_project")
DATASET_NAME = "o_customer"
EXECUTION_DATE = time.strftime("%Y%m%d")
# ENV Variables
GCS_DIR_PATH = "data/long_conversion_data"
VM_CREATION_CODE_PATH = "/home/airflow/gcs/dags/scripts/universal_profile/long_conversion_pyfiles/vm_api"
VM_INSTANCE_ZONE = "us-central1-a"
VM_INSTANCE_NAME = "long-propensity-scoring-" + EXECUTION_DATE
VM_INSTANCE_TYPE = "n1-standard-4"

OUTPUT_CSV_PATH = "gs://{gcs_bucket}/data/long_conversion_data/input/lc_data_for_aggregation{execution_date}.csv".format(
    gcs_bucket=GCS_BUCKET, execution_date=EXECUTION_DATE
)

# BigQuery tables info
BNID_INTERMEDIATE_TABLE_LOC = (
    "up_tgt_ora_tables" + ".bq_long_conversion_sql_intermediate_" + EXECUTION_DATE
)
BNID_LC, BNID_LC_DELETE = get_lc_queries(EXECUTION_DATE, BNID_INTERMEDIATE_TABLE_LOC)

LC_LOCAL_CONF_PATH, GCS_CONF_PATH = get_lc_scoring_params(
    EXECUTION_DATE, OUTPUT_CSV_PATH, GCS_BUCKET, GCS_DIR_PATH
)

# GCS to BigQuery Conf
BNID_INTERMEDIATE = "o_customer.lc_bnid_level_intermediate_" + EXECUTION_DATE
BNID_INTERMEDIATE_WITHOUT_DATASET = "lc_bnid_level_intermediate_" + EXECUTION_DATE
FINAL_BNID_TABLE = "bn_lc_propensity"
DATA_PATH = "data/long_conversion_data/output/"
FINAL_BNID_CSV = DATA_PATH + "long_conversion_bnid_level_" + EXECUTION_DATE + ".csv"

BNID_JOURNEY_INTERMEDIATE = (
    "o_customer.lc_bnid_journey_level_intermediate_" + EXECUTION_DATE
)
BNID_JOURNEY_INTERMEDIATE_WITHOUT_DATASET = (
    "lc_bnid_journey_level_intermediate_" + EXECUTION_DATE
)
FINAL_BNID_JOURNEY_TABLE = "bn_lc_category_propensity"
FINAL_BNID_JOURNEY_CSV = (
    DATA_PATH + "long_conversion_bnid_journey_level_" + EXECUTION_DATE + ".csv"
)

FINAL_INSERT_QUERY = get_final_table_insert_query(
    PROJECT_ID,
    DATASET_NAME,
    FINAL_BNID_TABLE,
    BNID_INTERMEDIATE_WITHOUT_DATASET,
    FINAL_BNID_JOURNEY_TABLE,
    BNID_JOURNEY_INTERMEDIATE_WITHOUT_DATASET,
)


# DAG Conf
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

default_dag_args = {
    'owner': 'UP',
    "start_date": yesterday,
    'email': ['rohit.varma@affine.ai'],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": models.Variable.get("gcp_project"),
}

with models.DAG(
    "long_conversion_propensity_model",
    schedule_interval=None,
    default_args=default_dag_args,
) as dag:

    #task_start = dummy_operator.DummyOperator(task_id="start_lc_model", dag=dag)

    task_delete_exist_intermediate_sql_tables = BigQueryOperator(
        task_id="lc_delete_exist_intermediate_tables",
        sql="""drop table if exists {0}; drop table if exists {1}; drop table if exists {2}; """.format(
            BNID_INTERMEDIATE_TABLE_LOC, BNID_INTERMEDIATE, BNID_JOURNEY_INTERMEDIATE
        ),
        use_legacy_sql=False,
    )

    # Load local conf files to GCS
    task_setup_conf_files = bash_operator.BashOperator(
        task_id="lc_setup_conf_files",
        bash_command="sudo gsutil cp {lc_score_file_path} {gcs_conf_path}".format(
            lc_score_file_path=LC_LOCAL_CONF_PATH, gcs_conf_path=GCS_CONF_PATH,
        ),
    )

    # Remove local conf files
    task_rm_local_conf_files = bash_operator.BashOperator(
        task_id="lc_rm_local_conf_files",
        bash_command="sudo rm {lc_score_file_path}".format(
            lc_score_file_path=LC_LOCAL_CONF_PATH,
        ),
    )

    task_bnid_daylevel_conversion = BigQueryOperator(
        task_id="lc_bnid_daylevel_conversion",
        sql=BNID_LC,
        use_legacy_sql=False,
        destination_dataset_table=BNID_INTERMEDIATE_TABLE_LOC,
    )

    task_bnid_lc_delete_duplicate_records = BigQueryOperator(
        task_id="lc_bnid_delete_duplicate_records",
        sql=BNID_LC_DELETE,
        use_legacy_sql=False,
    )

    task_export_queried_data_to_gcs = BigQueryToCloudStorageOperator(
        task_id="lc_export_queried_data_to_gcs",
        source_project_dataset_table=BNID_INTERMEDIATE_TABLE_LOC,
        destination_cloud_storage_uris=[OUTPUT_CSV_PATH],
        export_format="CSV",
    )

    # Create VM and Prepare the instance and run model scoring
    task_vm_creation_run_scoring = bash_operator.BashOperator(
        task_id="lc_vm_creation_run_scoring",
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
        task_id="lc_vm_deletion",
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

    task_append_bnid_data_to_intermediate = GoogleCloudStorageToBigQueryOperator(
        task_id="lc_append_bnid_data_to_intermediate",
        destination_project_dataset_table=BNID_INTERMEDIATE,
        bucket=GCS_BUCKET,
        source_objects=[FINAL_BNID_CSV],
        autodetect=True,
        source_format="CSV",
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    task_append_bnid_journey_data_to_intermediate = GoogleCloudStorageToBigQueryOperator(
        task_id="lc_append_bnid_journey_data_to_intermediate",
        destination_project_dataset_table=BNID_JOURNEY_INTERMEDIATE,
        bucket=GCS_BUCKET,
        source_objects=[FINAL_BNID_JOURNEY_CSV],
        autodetect=True,
        source_format="CSV",
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    task_insert_final = BigQueryOperator(
        task_id="lc_insert_data_into_final",
        sql=FINAL_INSERT_QUERY,
        use_legacy_sql=False,
    )

    task_bn_lc_pj_attributes_process = BigQueryOperator(
        task_id='start_bn_lc_pj_attributes_process',
        sql= bn_lc_pj_attributes_sql,
        #bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql=False,
        #destination_dataset_table=bq_dataset_name1
        )
        
    task_email_on_success_lc_model = EmailOperator(
    task_id="send_mail_on_success_lc_model", 
    # to='bluenile@affineanalytics.com',
    to='rohit.varma@affine.ai',
    
    subject='UP ETL Alert:Success(long_conversion_propensity_model)',
    html_content=''' 
    Hi,<br>
    
    <p>This email is to inform you that, long_conversion_propensity_model(LC Model) pipeline has been successfully completed.<br>
           <br>
    -<br>
    UP Team.<p>''',
    dag=dag)
    
    task_email_on_success_bn_lc_pj_atrributes = EmailOperator(
    task_id="send_mail_on_success_bn_lc_pj_atrributes", 
    # to='bluenile@affineanalytics.com',
    to='rohit.varma@affine.ai',
    subject='UP ETL Alert:Success(bn_lc_pj_attributes_process)',
    html_content=''' 
    Hi,<br>
    
    <p>This email is to inform you that, bn_lc_pj_attributes_process pipeline has been successfully completed.<br>
           <br>
    -<br>
    UP Team.<p>''',
    dag=dag)

    #task_end = dummy_operator.DummyOperator(task_id="end_lc_model", dag=dag)

    # DAG links
    task_delete_exist_intermediate_sql_tables >> task_setup_conf_files >> task_rm_local_conf_files >> task_bnid_daylevel_conversion >> task_bnid_lc_delete_duplicate_records >> task_export_queried_data_to_gcs >> task_vm_creation_run_scoring >> task_vm_deletion

    task_vm_deletion >> task_append_bnid_data_to_intermediate >> task_insert_final
    task_vm_deletion >> task_append_bnid_journey_data_to_intermediate >> task_insert_final

    task_insert_final >> task_bn_lc_pj_attributes_process
    
    task_insert_final >> task_email_on_success_lc_model
    
    task_bn_lc_pj_attributes_process >> task_email_on_success_bn_lc_pj_atrributes