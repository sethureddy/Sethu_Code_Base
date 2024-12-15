import json

from .bq_conf import PARAMS_QUERY, PARAMS_QUERY_DELETE
from .bnid_daylevel_conversion_lc import bnid_daylevel_conversion
from .bnid_lc_delete_duplicate_records import bnid_lc_delete_duplicate_records
from .lc_models_insert_into_final_tables import lc_models_insert_into_final_tables


def get_lc_queries(execution_date, master_intermediate):
    PARAMS_QUERY["delta_date"] = int(execution_date)
    PARAMS_QUERY_DELETE["table_name"] = master_intermediate

    BNID_LC = bnid_daylevel_conversion(params=PARAMS_QUERY)
    BNID_LC_DELETE = bnid_lc_delete_duplicate_records(params=PARAMS_QUERY_DELETE)

    return BNID_LC, BNID_LC_DELETE


def get_lc_scoring_params(execution_date, OUTPUT_CSV_PATH, GCS_BUCKET, GCS_DIR_PATH):
    params_lc_scoring = {
        "input_file": OUTPUT_CSV_PATH,
        "output_path": "gs://{gcs_bucket}/data/long_conversion_data/output/".format(
            gcs_bucket=GCS_BUCKET
        ),
        "model_path": "gs://{gcs_bucket}/data/long_conversion_data/LC_model.pkl".format(
            gcs_bucket=GCS_BUCKET
        ),
        "scoring_date": execution_date,
        "bucket_name": GCS_BUCKET,
        "model_bucket": "data/long_conversion_data/LC_model.pkl",
        "model_local": "LC_model.pkl",
    }

    """
    Prepares conf files of models to load to GCS
    """
    model_conf_data = json.dumps(params_lc_scoring)
    file_path = "/home/airflow/gcs/lc_params.json"
    conf_file = open(file_path, "w")
    conf_file.write(model_conf_data)
    conf_file.flush()
    conf_file.close()
    gcs_path = "gs://{gcs_bucket}/{directory_path}/conf/".format(
        gcs_bucket=GCS_BUCKET, directory_path=GCS_DIR_PATH
    )

    return file_path, gcs_path


def get_final_table_insert_query(
    PROJECT_NAME,
    DATASET_NAME,
    BNID_TABLE,
    BNID_INTERMEDIATE,
    JOURNEY_TABLE,
    JOURNEY_INTERMEDIATE,
):
    params = {
        "project_name": PROJECT_NAME,
        "dataset_name": DATASET_NAME,
        "final_table_bnid_level": BNID_TABLE,
        "intermediate_table_bnid_level": BNID_INTERMEDIATE,
        "final_table_journey_level": JOURNEY_TABLE,
        "intermediate_table_journey_level": JOURNEY_INTERMEDIATE,
    }
    query = lc_models_insert_into_final_tables(params)

    return query
