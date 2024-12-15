from pandas.io import gbq
import pandas as pd
import numpy as np
from google.cloud import bigquery as bgq
client = bgq.Client()

#load all tables having pii
sql_1="""select table_names,column_name,data_type,ColumnNameInInput from bnile-cdw-prod.CCPA_deletion.ccpa_fields """
tables_having_pii = client.query(sql_1).to_dataframe(progress_bar_type='tqdm')

#load tables in prod which have data related to the customer
sql_2="""select table_name from bnile-cdw-prod.CCPA_deletion.tables_having_data_prod"""
tables_having_data_prod  = client.query(sql_2).to_dataframe(progress_bar_type='tqdm')

#load all fileds' original values and mask values other than guid and ip address
sql_3="""select * from `bnile-cdw-prod.CCPA_deletion.pii_stage_table`"""
pii_stage_table=client.query(sql_3).to_dataframe(progress_bar_type='tqdm')


identification_data=pii_stage_table[pii_stage_table.identification_flag==True]
mask_data=pii_stage_table[pii_stage_table.mask_flag==True]


identification_fields_list=list( identification_data['field_name'])
masking_fields_list =list( mask_data['field_name'])

#Creating dictionary with filed name and original value
field_value_dict=dict(zip( identification_data.field_name, identification_data.value))
#creating for field name and mask value
field_mask_value_dict=dict(zip(mask_data.field_name, mask_data.mask_value))

#filtering all columns required as per the current ccpa/gdpr request
tables_required_masking_stage=pd.merge( tables_having_pii,pii_stage_table ,left_on='ColumnNameInInput',right_on='field_name',how='inner')

#Filtering only those tables which have data related to the customer
tables_required_masking=pd.merge( tables_required_masking_stage,tables_having_data_prod,left_on='table_names',right_on='table_name',how='inner')

tables_required_masking.drop(['field_name', 'value', 'mask_value', 'identification_flag', 'mask_flag','table_name'],inplace=True,axis=1)


tables_required_masking_concat_1=tables_required_masking.groupby('table_names')['ColumnNameInInput'].apply(lambda x: ','.join(x)).reset_index()

tables_required_masking_concat_2=tables_required_masking.groupby('table_names')['column_name'].apply(lambda x: ','.join(x)).reset_index()

tables_required_masking_concat_3=tables_required_masking.groupby('table_names')['data_type'].apply(lambda x: ','.join(x)).reset_index()

stage_table=pd.merge(tables_required_masking_concat_1,tables_required_masking_concat_2,on='table_names',how='inner')

final_table=pd.merge(stage_table,tables_required_masking_concat_3,on='table_names',how='inner')

def is_guid_ip_only(ColumnNameInInput):
    standard_columns=ColumnNameInInput.split(',')
    other_columns=[]
    mask_fields=[]
    for column in standard_columns:
        if column!='guid' and column!='ip_address' and column in identification_fields_list:
            other_columns.append(column)
        if column!='guid'and column!='ip_address' and column in masking_fields_list:
            mask_fields.append(column)
    if len(other_columns)==0 and len(mask_fields)>0:
        return True
    else:
        return False

final_table['guid_ip_flag']=False
final_table['guid_ip_flag']=final_table['ColumnNameInInput'].apply(is_guid_ip_only)

#Filter those table which have guid and ip address only as identification fields
final_table=final_table[final_table.guid_ip_flag==True]

# final_table.to_csv('data/privacy_request/output/update_query_1.csv')

# providing the table_name 
table_id = 'bnile-cdw-prod.CCPA_deletion.update_query_1'

job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")

# Make an API request for creating the table
job = client.load_table_from_dataframe(
    final_table, table_id, job_config=job_config
)  
print(job.result())


