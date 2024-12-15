def sc_models_insert_into_final_tables(params):
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]

    final_table_bnid_level = params["final_table_bnid_level"]
    intermediate_table_bnid_level = params["intermediate_table_bnid_level"]

    final_table_journey_level = params["final_table_journey_level"]
    intermediate_table_journey_level = params["intermediate_table_journey_level"]

    bql = f"""
	INSERT INTO
	`{project_name}.{dataset_name}.{final_table_bnid_level}`
	SELECT
	*
	FROM (
	SELECT
		bnid,
		Predicted_probabilities as propensity_score,
		Propensity_category as propensity_phase,
		PARSE_DATE('%Y%m%d',
		CAST(RUN_DATE AS string)) as model_run_date
	FROM
		`{project_name}.{dataset_name}.{intermediate_table_bnid_level}`)aa
	WHERE
	aa.model_run_date NOT IN(
	SELECT
		DISTINCT model_run_date
	FROM
		`{project_name}.{dataset_name}.{final_table_bnid_level}`);
		
		
		
	INSERT INTO
	`{project_name}.{dataset_name}.{final_table_journey_level}`
	SELECT
	*
	FROM (
	SELECT
		bnid,
		Journey as product_category,
		Predicted_probabilities as propensity_score,
		Propensity_category as propensity_phase,
		PARSE_DATE('%Y%m%d',
		CAST(RUN_DATE AS string)) as model_run_date
	FROM
		`{project_name}.{dataset_name}.{intermediate_table_journey_level}`)aa
	WHERE
	aa.model_run_date NOT IN(
	SELECT
		DISTINCT model_run_date
	FROM
		`{project_name}.{dataset_name}.{final_table_journey_level}`)""".format(
        project_name,
        dataset_name,
        final_table_bnid_level,
        intermediate_table_bnid_level,
        final_table_journey_level,
        intermediate_table_journey_level,
    )
    return bql