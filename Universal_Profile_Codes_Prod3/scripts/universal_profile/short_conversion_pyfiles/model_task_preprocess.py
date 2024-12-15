import json

from .bq_conf import PARAMS_QUERY_PARENT, PARAMS_QUERY_CHILD
from .bnid_daylevel_conversion import bnid_daylevel_conversion
from .bnid_daylevel_conversion_output import bnid_daylevel_conversion_output
from .sc_models_insert_into_final_tables import sc_models_insert_into_final_tables


def get_sc_queries(execution_date, master_intermediate):
    PARAMS_QUERY_PARENT["delta_date"] = int(execution_date)
    PARAMS_QUERY_CHILD["table_name"] = master_intermediate
    BNID_PARENT = bnid_daylevel_conversion(params=PARAMS_QUERY_PARENT)
    BNID_CHILD = bnid_daylevel_conversion_output(params=PARAMS_QUERY_CHILD)
    return BNID_PARENT, BNID_CHILD


def get_sc_scoring_params(EXECUTION_DATE, OUTPUT_CSV_PATH, GCS_BUCKET, GCS_DIR_PATH):
    params_sc_scoring = {
        "input_file": OUTPUT_CSV_PATH,
        "output_path": "gs://{gcs_bucket}/data/short_conversion_data/output/".format(
            gcs_bucket=GCS_BUCKET
        ),
        "model_path": "gs://{gcs_bucket}/data/short_conversion_data/SC_model.pkl".format(
            gcs_bucket=GCS_BUCKET
        ),
        "scoring_date": EXECUTION_DATE,
        "bucket_name": GCS_BUCKET,
        "model_bucket": "data/short_conversion_data/SC_model.pkl",
        "model_local": "SC_model.pkl",
    }

    """
    Prepares conf files of models to load to GCS
    """
    model_conf_data = json.dumps(params_sc_scoring)
    file_path = "/home/airflow/gcs/sc_params.json"
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
    query = sc_models_insert_into_final_tables(params)

    return query
