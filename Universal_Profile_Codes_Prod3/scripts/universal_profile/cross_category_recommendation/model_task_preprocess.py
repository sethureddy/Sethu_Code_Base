import json
from .recommendation_input import recommendation_jewellery, recommendation_diamonds, recommendation_settings, lastpurchase_product
from .utils import prepare_conf_files, prepare_scoring_params
from .bq_conf import QUERY_PARAMS

def task_preprocess(gcs_bucket, directory_path, model_name, execution_date):

	QUERY_PARAMS["delta_date"] = int(execution_date) 
	
	
	jewellery_sql = recommendation_jewellery(QUERY_PARAMS)
	jewellery_sql = recommendation_jewellery(QUERY_PARAMS)
	diamond_sql = recommendation_diamonds(QUERY_PARAMS)
	setting_sql = recommendation_settings(QUERY_PARAMS)
	lastpurchase_sql = lastpurchase_product(QUERY_PARAMS)
	
	scoring_parmas = prepare_scoring_params(gcs_bucket, directory_path, model_name, execution_date)
	
	conf_data = json.dumps(scoring_parmas)
	jewellery_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_cross_reco_{date}".format(model=model_name[0], date=execution_date)
	setting_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_cross_reco_{date}".format(model=model_name[1], date=execution_date)
	diamond_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_cross_reco_{date}".format(model=model_name[2], date=execution_date)
	lastpurchase_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_cross_reco_{date}".format(model=model_name[3], date=execution_date)
	
	local_conf_file_path, gcs_conf_path = prepare_conf_files(
	"crosscategory", conf_data, gcs_bucket, directory_path
	)
	
	return (
	jewellery_sql,
	diamond_sql,
	setting_sql,
	lastpurchase_sql,
	scoring_parmas["jewel_input_file"],
	scoring_parmas["diamond_input_file"],
	scoring_parmas["setting_input_file"],
	scoring_parmas["last_purchase"],
	jewellery_sql_output,
	diamond_sql_output,
	setting_sql_output,
	lastpurchase_sql_output,
	local_conf_file_path,
	gcs_conf_path,
	scoring_parmas["output_path"],
	)

def recommendation_task_preprocess(output_path, gcs_bucket, directory_path, model_name, recommendation_model_name, execution_date):
	# Recommendation Conf file prep
	recommendation_scoring_parmas = prepare_scoring_params(
	gcs_bucket, directory_path, model_name, execution_date
	)
	recommendation_scoring_parmas["recommendation_file"] = output_path
	recommendation_scoring_parmas.pop("jewel_input_file")
	recommendation_scoring_parmas.pop("setting_input_file")
	recommendation_scoring_parmas.pop("diamond_input_file")
	recommendation_scoring_parmas.pop("last_purchase")
	
	recommendation_conf_data = json.dumps(recommendation_scoring_parmas)
	local_conf_file_path, gcs_conf_path = prepare_conf_files(
	recommendation_model_name, recommendation_conf_data, gcs_bucket, directory_path
	)
	return (
	local_conf_file_path,
	gcs_conf_path,
	)