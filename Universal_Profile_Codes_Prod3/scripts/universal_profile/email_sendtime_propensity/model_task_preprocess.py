import json
import datetime 
import dateutil
from datetime import date
from .user_input_data import user_activity, opentime_counts, lastopen_slot, email_subscription
from .utils import prepare_conf_files, prepare_scoring_params
from .bq_conf import QUERY_PARAMS

def task_preprocess(gcs_bucket, directory_path, model_name, execution_date):

	execution_date = str(execution_date)
	run_date = int(execution_date)
    
	execution_date1 = execution_date[:-2] + '01'
	QUERY_PARAMS["delta_date"] = int(execution_date1)
	day_diff = run_date - int(execution_date1)
	run_date = date.today() - dateutil.relativedelta.relativedelta(months=3,days=day_diff)
	QUERY_PARAMS["run_date"] = int(run_date.strftime("%Y%m%d"))
	
	activity_sql = user_activity(QUERY_PARAMS)
	opencount_sql = opentime_counts(QUERY_PARAMS)
	lastopen_sql = lastopen_slot(QUERY_PARAMS)
	subscription_sql = email_subscription(QUERY_PARAMS)
	
	scoring_parmas = prepare_scoring_params(gcs_bucket, directory_path, model_name, execution_date)
	
	conf_data = json.dumps(scoring_parmas)
	activity_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_email_prop_{date}".format(model=model_name[1], date=execution_date)
	opencount_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_email_prop_{date}".format(model=model_name[0], date=execution_date)
	lastopen_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_email_prop_{date}".format(model=model_name[2], date=execution_date)
	subscription_sql_output = QUERY_PARAMS["stg_data"] + ".{model}_intermediate_email_prop_{date}".format(model=model_name[3], date=execution_date)
	
	local_conf_file_path, gcs_conf_path = prepare_conf_files(
	"email_sendtime", conf_data, gcs_bucket, directory_path
	)
	
	return (
	activity_sql,
	opencount_sql,
	lastopen_sql,
	subscription_sql,
	scoring_parmas["clickfact"],
	scoring_parmas["openfact"],
	scoring_parmas["lastopen"],
	scoring_parmas["subscribed"],
	activity_sql_output,
	opencount_sql_output,
	lastopen_sql_output,
	subscription_sql_output,
	local_conf_file_path,
	gcs_conf_path,
	scoring_parmas["output_path"],
	)
