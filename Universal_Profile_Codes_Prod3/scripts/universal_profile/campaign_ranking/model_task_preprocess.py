import json
from .utils import prepare_conf_files, prepare_scoring_params
from .bq_conf import PARAMS_QUERY
from scripts.universal_profile.campaign_ranking.base_trigger_campaign_table import base_trigger_campaign_table


def campaign_ranking_task_preprocess(gcs_bucket, directory_path, model_name, execution_date):
    # campaign_ranking preprocess steps

    campaign_ranking_sql = base_trigger_campaign_table(PARAMS_QUERY)
    campaign_ranking_scoring_parmas = prepare_scoring_params(
        gcs_bucket, directory_path, model_name, execution_date
    )
    campaign_ranking_conf_data = json.dumps(campaign_ranking_scoring_parmas)
    campaign_ranking_sql_output = "up_stg_customer.campaign_ranking_" + execution_date
    
    local_conf_file_path, gcs_conf_path = prepare_conf_files(
        model_name, campaign_ranking_conf_data, gcs_bucket, directory_path
    )

    return (
        campaign_ranking_sql,
        campaign_ranking_scoring_parmas["input_file"],
        campaign_ranking_sql_output,
        local_conf_file_path,
        gcs_conf_path,
        campaign_ranking_scoring_parmas["output_path"],
    )