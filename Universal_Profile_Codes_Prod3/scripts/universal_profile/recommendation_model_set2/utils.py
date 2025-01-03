def prepare_scoring_params(gcs_bucket, directory_path, model_name, execution_date):
    """
    Prepares params for model scoring run
    """
    scoring_params = {
        "input_file": "gs://{gcs_bucket}/{directory_path}/input/{execution_date}/{model_name}.csv".format(
            gcs_bucket=gcs_bucket,
            directory_path=directory_path,
            model_name=model_name,
            execution_date=execution_date,
        ),
        "output_path": "gs://{gcs_bucket}/{directory_path}/output/{execution_date}/{model_name}.csv".format(
            gcs_bucket=gcs_bucket,
            directory_path=directory_path,
            model_name=model_name,
            execution_date=execution_date,
        ),
        "scoring_date": execution_date,
    }
    return scoring_params


def prepare_conf_files(model_conf_name, model_conf_data, gcs_bucket, directory_path, execution_date):
    """
    Prepares conf files of models to load to GCS
    """
    file_path = "/home/airflow/gcs/rc2_{conf_name}.json".format(conf_name=model_conf_name)
    conf_file = open(file_path, "w")
    conf_file.write(model_conf_data)
    conf_file.flush()
    conf_file.close()
    gcs_path = "gs://{gcs_bucket}/{directory_path}/conf/{execution_date}/".format(
        gcs_bucket=gcs_bucket, directory_path=directory_path, execution_date=execution_date
    )

    return file_path, gcs_path
