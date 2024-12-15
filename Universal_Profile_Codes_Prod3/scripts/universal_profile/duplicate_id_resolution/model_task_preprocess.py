import json
from .utils import prepare_conf_files, prepare_scoring_params
from .bnq_conf import PARAMS_QUERY
from scripts.universal_profile.duplicate_id_resolution.user_input import address
from scripts.universal_profile.duplicate_id_resolution.user_input import email_base1
from scripts.universal_profile.duplicate_id_resolution.user_input import email_base2
from scripts.universal_profile.duplicate_id_resolution.user_input import phone_base1
from scripts.universal_profile.duplicate_id_resolution.user_input import phone_base2
from scripts.universal_profile.duplicate_id_resolution.user_input import payment


def id_resolution_task_preprocess(gcs_bucket, directory_path, model_name, execution_date):
    # id_resolution preprocess steps

    address_sql = address(PARAMS_QUERY)
    email_base1_sql = email_base1(PARAMS_QUERY)
    email_base2_sql = email_base2(PARAMS_QUERY)
    phone_base1_sql = phone_base1(PARAMS_QUERY)
    phone_base2_sql = phone_base2(PARAMS_QUERY)
    payment_sql = payment(PARAMS_QUERY)
    
    id_resolution_scoring_parmas = prepare_scoring_params(
        gcs_bucket, directory_path, model_name, execution_date
    )
    id_resolution_conf_data = json.dumps(id_resolution_scoring_parmas)

    address_sql_output= "up_stg_customer.address_dup_id_resolution_" + execution_date
    email_base1_sql_output= "up_stg_customer.email_base1_dup_id_resolution_" + execution_date
    email_base2_sql_output= "up_stg_customer.email_base2_dup_id_resolution_" + execution_date
    payment_sql_output= "up_stg_customer.payment_dup_id_resolution_" + execution_date
    phone_base1_sql_output= "up_stg_customer.phone_base1_dup_id_resolution_" + execution_date
    phone_base2_sql_output= "up_stg_customer.phone_base2_dup_id_resolution_" + execution_date
    
    local_conf_file_path, gcs_conf_path = prepare_conf_files(
        model_name, id_resolution_conf_data, gcs_bucket, directory_path
    )
    
    return (
        address_sql,
        email_base1_sql,
        email_base2_sql,
        payment_sql,
        phone_base1_sql,
        phone_base2_sql,
        
        id_resolution_scoring_parmas["address"],
        id_resolution_scoring_parmas["email_base1"],
        id_resolution_scoring_parmas["email_base2"],
        id_resolution_scoring_parmas["payment"],
        id_resolution_scoring_parmas["phone_base1"],
        id_resolution_scoring_parmas["phone_base2"],

        address_sql_output,
        email_base1_sql_output,
        email_base2_sql_output,
        payment_sql_output,
        phone_base1_sql_output,
        phone_base2_sql_output,
        
        local_conf_file_path,
        gcs_conf_path,
        id_resolution_scoring_parmas["output_path"],
    )