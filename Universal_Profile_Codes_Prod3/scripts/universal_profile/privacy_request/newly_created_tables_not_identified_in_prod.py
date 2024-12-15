from pandas.io import gbq
import pandas as pd
import numpy as np
from google.cloud import bigquery as bgq
client = bgq.Client()

# taking the identification fields storing into the list
sql = """SELECT field_name FROM `bnile-cdw-prod.CCPA_deletion.pii_stage_table` where identification_flag = true"""
column_names  = client.query(sql).to_dataframe(progress_bar_type='tqdm')
coulmns_required = list(column_names.field_name)

# taking the pii fields storing into the list
sql = """SELECT field_name FROM `bnile-cdw-prod.CCPA_deletion.pii_stage_table` where mask_flag = true"""
pii_field_names  = client.query(sql).to_dataframe(progress_bar_type='tqdm')
pii_fields = list(pii_field_names.field_name)

# loading the tables for deletion queries
sql3 = """select * from bnile-cdw-prod.CCPA_deletion.ccpa_fields"""
tables_to_be_modified  = client.query(sql3).to_dataframe(progress_bar_type='tqdm')

new_fields_created =  "select * from bnile-cdw-prod.CCPA_deletion.new_fields_created"
new_fields = client.query(new_fields_created).to_dataframe(progress_bar_type='tqdm')

# new table names created
new_table_names = list(new_fields['table_names'].unique())

# new table names created that are not available in the deletion queries tables
tables_to_check = np.setdiff1d(new_table_names,tables_to_be_modified)

new_tables = tables_to_be_modified[np.in1d(tables_to_be_modified.table_names, tables_to_check)]

table_id = 'bnile-cdw-prod.CCPA_deletion.table_names_not_indentified'
job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")

# Make an API request for creating the table 
job = client.load_table_from_dataframe(
    new_tables, table_id, job_config=job_config
) 