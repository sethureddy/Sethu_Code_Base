import json
from .settings import setting_item_item_similarity
from .jewellery import item_item_similarity_jewellery
from .utils import prepare_conf_files, prepare_scoring_params
from .bq_conf import QUERY_PARAMS


def setting_task_preprocess(gcs_bucket, directory_path, model_name, execution_date):
    # Setting preprocess steps
    QUERY_PARAMS["delta_date"] = int(execution_date)
    setting_sql = setting_item_item_similarity.setting_item_interaction(QUERY_PARAMS)
    setting_scoring_parmas = prepare_scoring_params(
        gcs_bucket, directory_path, model_name, execution_date
    )
    setting_conf_data = json.dumps(setting_scoring_parmas)
    setting_sql_output = QUERY_PARAMS[
        "dataset_name"
    ] + ".{model}_setting_item_item_intermediate_table_{date}".format(
        model=model_name, date=execution_date
    )
    local_conf_file_path, gcs_conf_path = prepare_conf_files(
        model_name, setting_conf_data, gcs_bucket, directory_path
    )

    return (
        setting_sql,
        setting_scoring_parmas["input_file"],
        setting_sql_output,
        local_conf_file_path,
        gcs_conf_path,
        setting_scoring_parmas["output_path"],
    )

def jewellery_task_preprocess(gcs_bucket, directory_path, model_name, execution_date):
    # Jewellery preprocess steps
    QUERY_PARAMS["delta_date"] = int(execution_date)
    jewellery_sql = item_item_similarity_jewellery.jewellery_item_interaction(QUERY_PARAMS)
    jewellery_scoring_parmas = prepare_scoring_params(
        gcs_bucket, directory_path, model_name, execution_date
    )
    jewellery_conf_data = json.dumps(jewellery_scoring_parmas)
    jewellery_sql_output = QUERY_PARAMS[
        "dataset_name"
    ] + ".{model}_jewellery_item_item_intermediate_table_{date}".format(
        model=model_name, date=execution_date
    )
    local_conf_file_path, gcs_conf_path = prepare_conf_files(
        model_name, jewellery_conf_data, gcs_bucket, directory_path
    )

    return (
        jewellery_sql,
        jewellery_scoring_parmas["input_file"],
        jewellery_sql_output,
        local_conf_file_path,
        gcs_conf_path,
        jewellery_scoring_parmas["output_path"]
    )

# def recommendation_task_preprocess(setting_output_path, jewellery_output_path, gcs_bucket, directory_path, model_name, execution_date):
#     # Recommendation Conf file prep
#     recommendation_scoring_parmas = prepare_scoring_params(
#         gcs_bucket, directory_path, model_name, execution_date
#     )
#     recommendation_scoring_parmas["setting_file"] = setting_output_path
#     recommendation_scoring_parmas["jewellery_file"] = jewellery_output_path
#     recommendation_scoring_parmas.pop("input_file")
#
#
#     recommendation_conf_data = json.dumps(recommendation_scoring_parmas)
#     local_conf_file_path, gcs_conf_path = prepare_conf_files(
#         model_name, recommendation_conf_data, gcs_bucket, directory_path
#     )
#
#     return (
#         local_conf_file_path,
#         gcs_conf_path,
#     )