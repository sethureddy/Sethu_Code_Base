from pandas.io import gbq
import pandas as pd
import numpy as np
from google.cloud import bigquery as bgq
client = bgq.Client()

#loading all tables having pii fields
sql_1="""select table_names,column_name,data_type,ColumnNameInInput from bnile-cdw-prod.CCPA_deletion.ccpa_fields """
tables_having_pii  = client.query(sql_1).to_dataframe(progress_bar_type='tqdm')

#loading those tables in prod which have data related to the customer
sql_2="""select table_name from bnile-cdw-prod.CCPA_deletion.tables_having_data_prod """
tables_having_data_prod  = client.query(sql_2).to_dataframe(progress_bar_type='tqdm')

#loading original values and mask values for identification fields
sql_3="""select * from `bnile-cdw-prod.CCPA_deletion.pii_stage_table` where identification_flag=true"""
identification_data=client.query(sql_3).to_dataframe(progress_bar_type='tqdm')

#Creating dictionary with filed name and original value
field_value_dict=dict(zip( identification_data.field_name, identification_data.value))
#creating for field name and mask value
field_mask_value_dict=dict(zip(identification_data.field_name, identification_data.mask_value))

guid_values=field_value_dict['guid'].split(',')
ip_values=field_value_dict['ip_address'].split(',')

guid_mask_values=field_mask_value_dict ['guid'].split(',')
ip_mask_values= field_mask_value_dict['ip_address'].split(',')

#Filter required columns only as per the current ccpa/gdpr request
tables_having_identification_fields=pd.merge(tables_having_pii, identification_data,left_on='ColumnNameInInput',right_on='field_name',how='inner')

#Filter those tables which have data related to the customer
tables_required_masking=pd.merge( tables_having_identification_fields,tables_having_data_prod,left_on='table_names',right_on='table_name',how='inner')

tables_required_masking.drop('table_name',inplace=True,axis=1)

tables_required_masking_concat_1=tables_required_masking.groupby('table_names')['ColumnNameInInput'].apply(lambda x: ','.join(x)).reset_index()

tables_required_masking_concat_2=tables_required_masking.groupby('table_names')['column_name'].apply(lambda x: ','.join(x)).reset_index()

final_table=pd.merge(tables_required_masking_concat_1,tables_required_masking_concat_2,on='table_names',how='inner')

def is_guid_ip_only(ColumnNameInInput):
    columns=ColumnNameInInput.split(',')
    other_columns=[]
    for column in columns:
        if column!='guid' and column!='ip_address':
            other_columns.append(column)
    if len(other_columns)==0:
        return True
    else:
        return False

final_table['flag']=False
final_table['flag']=final_table['ColumnNameInInput'].apply(is_guid_ip_only)

#Filter those tables which have guid and ip address only as identification fields
final_table=final_table[final_table.flag==True]

#load the ip and guids from other bigquery tables which are associated with other customers also
###
sql_4="""select guid from bnile-cdw-prod.CCPA_deletion.problematic_guid"""
problematic_guid=client.query(sql_4).to_dataframe(progress_bar_type='tqdm')
problematic_guid=list(problematic_guid.guid)




sql_5="""select ip_address from bnile-cdw-prod.CCPA_deletion.problematic_ip_address"""
problematic_ip=client.query(sql_5).to_dataframe(progress_bar_type='tqdm')
problematic_ip=list(problematic_ip.ip_address)
print(problematic_ip)

def create_update_query_guid_ip(table_names,ColumnNameInInput,column_name):
    standard_columns=ColumnNameInInput.split(',')
    original_columns=column_name.split(',')
    query=''
    query_1='update '+table_names+' set '
    for i in range(len(standard_columns)):
        standard_column=standard_columns[i]
        original_column=original_columns[i]
        if standard_column=='guid':
            for j in range(len(guid_values)):
                if guid_values[j] in problematic_guid:
                    continue
                else:
                    query_2=query_1+original_column+' = '+"'"+guid_mask_values[j]+"'"+' where '+original_column+' = '+"'"+guid_values[j]+"';"
                    query=query+query_2
        if standard_column=='ip_address':
            for j in range(len(ip_values)):
                if ip_values[j] in problematic_ip:
                    continue
                else:
                    query_3=query_1+original_column+' = '+"'"+ip_mask_values[j]+"'"+' where '+original_column+' = '+"'"+ip_values[j]+"';"
                    query=query+query_3
    return query

final_table['query']=''
final_table['query']=final_table.apply(lambda x:create_update_query_guid_ip(x.table_names,x.ColumnNameInInput,x.column_name),axis=1)

# final_table.to_csv('data/privacy_request/output/update_query_2_2.csv')

# providing the table_name 
table_id = 'bnile-cdw-prod.CCPA_deletion.update_query_2_2'

job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")

# Make an API request for creating the table
job = client.load_table_from_dataframe(
    final_table, table_id, job_config=job_config
)  
print(job.result())