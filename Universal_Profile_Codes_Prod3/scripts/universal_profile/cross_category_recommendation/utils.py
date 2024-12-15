def prepare_scoring_params(gcs_bucket, directory_path, model_name, execution_date):
    """
    Prepares params for model scoring run
    """
    scoring_params = {
		"jewel_input_file": "gs://{gcs_bucket}/{directory_path}/input/{execution_date}/{model_name1}.csv".format(
            gcs_bucket=gcs_bucket,
            directory_path=directory_path,
            model_name1 =model_name[0],
            execution_date=execution_date,
        ),
		"setting_input_file":"gs://{gcs_bucket}/{directory_path}/input/{execution_date}/{model_name2}.csv".format(
            gcs_bucket=gcs_bucket,
            directory_path=directory_path,
            model_name2 =model_name[1],
            execution_date=execution_date,
        ),
		"diamond_input_file":"gs://{gcs_bucket}/{directory_path}/input/{execution_date}/{model_name3}.csv".format(
            gcs_bucket=gcs_bucket,
            directory_path=directory_path,
            model_name3 =model_name[2],
            execution_date=execution_date,
        ),
		"last_purchase":"gs://{gcs_bucket}/{directory_path}/input/{execution_date}/{model_name4}.csv".format(
            gcs_bucket=gcs_bucket,
            directory_path=directory_path,
            model_name4 =model_name[3],
            execution_date=execution_date,
        ),
        "output_path": "gs://{gcs_bucket}/{directory_path}/output/{execution_date}/".format(
            gcs_bucket=gcs_bucket,
            directory_path=directory_path,
            model_name="crosscategory",
            execution_date=execution_date,
        ),
        "scoring_date": execution_date,
    }
    return scoring_params


def prepare_conf_files(model_conf_name, model_conf_data, gcs_bucket, directory_path):
    """
    Prepares conf files of models to load to GCS
    """
    file_path = "/home/airflow/gcs/{conf_name}.json".format(conf_name=model_conf_name)
    conf_file = open(file_path, "w")
    conf_file.write(model_conf_data)
    conf_file.flush()
    conf_file.close()
    gcs_path = "gs://{gcs_bucket}/{directory_path}/conf/".format(
        gcs_bucket=gcs_bucket, directory_path=directory_path
    )

    return file_path, gcs_path