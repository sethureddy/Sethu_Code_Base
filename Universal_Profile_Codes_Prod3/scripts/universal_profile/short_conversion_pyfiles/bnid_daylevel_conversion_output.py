def bnid_daylevel_conversion_output(params):
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    table_name = params["table_name"]
    bql = f'''
    SELECT
      known.*,
      date.WEEKEND_FLAG,
      date.SEASON_NAME,
      date.QUARTER,
      date.RETAIL_DAY_OF_MONTH,
      RETAIL_WEEK_OF_MONTH,
      RETAIL_DAY_OF_QUARTER,
      RETAIL_DAY_OF_YEAR,
      RETAIL_LAST_DAY_OF_MONTH_FLAG,
      RETAIL_LAST_DAY_OF_QTR_FLAG,
      RETAIL_MONTH_NAME,
      CASE
        WHEN lower(date.HOLIDAY_FLAG) = 'y' THEN 1
      ELSE
      0
    END
      AS HOLIDAY_FLAG
    FROM
      `{project_name}.{dataset_name}.{table_name}` known
    LEFT JOIN
      `{project_name}.{dataset_name_dssprod}.date_dim` date
    ON
      date.date_key =known.date_key'''.format(project_name, dataset_name, dataset_name_dssprod, table_name)

    return bql