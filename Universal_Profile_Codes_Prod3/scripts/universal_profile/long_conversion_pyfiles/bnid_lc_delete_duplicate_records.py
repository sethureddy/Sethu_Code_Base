def bnid_lc_delete_duplicate_records(params):
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    intermediate_table = params["table_name"]

    bql = f"""Delete from `{project_name}.{intermediate_table}` where guid_key in (
    select distinct guid_key from (
    select guid_key,date_key,count(*)  from  `{project_name}.{intermediate_table}` 
    group by 1,2
    having count(*) > 1)) """.format(
        project_name, intermediate_table
    )
    return bql
