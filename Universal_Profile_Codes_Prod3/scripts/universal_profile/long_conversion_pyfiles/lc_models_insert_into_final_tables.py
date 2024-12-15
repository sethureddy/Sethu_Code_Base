def lc_models_insert_into_final_tables(params):
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
		BNID,
		Predicted_probabilities,
		Propensity_category,
		PARSE_DATE('%Y%m%d',
		CAST(RUN_DATE AS string))RUN_DATE
	FROM
		`{project_name}.{dataset_name}.{intermediate_table_bnid_level}`)aa
	WHERE
	aa.RUN_DATE NOT IN(
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
		BNID,
		Journey,
		Predicted_probabilities,
		Propensity_category,
		PARSE_DATE('%Y%m%d',
		CAST(RUN_DATE AS string))RUN_DATE
	FROM
		`{project_name}.{dataset_name}.{intermediate_table_journey_level}`)aa
	WHERE
	aa.RUN_DATE NOT IN(
	SELECT
		DISTINCT model_run_date
	FROM
		`{project_name}.{dataset_name}.{final_table_journey_level}`)""".format(
        project_name=project_name,
        dataset_name=dataset_name,
        final_table_bnid_level=final_table_bnid_level,
        intermediate_table_bnid_level=intermediate_table_bnid_level,
        final_table_journey_level=final_table_journey_level,
        intermediate_table_journey_level=intermediate_table_journey_level,
    )
    return bql
