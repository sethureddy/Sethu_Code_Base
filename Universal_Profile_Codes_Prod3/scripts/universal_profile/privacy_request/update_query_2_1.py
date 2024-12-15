#generate update query for guid and ip address for those tables which have some other identification fields like email,bnid etc
#generate a single string having multiple update queries separated by semicolon for each table.

from pandas.io import gbq
import pandas as pd
import numpy as np
from google.cloud import bigquery as bgq
client = bgq.Client()

sql_1="""select table_names,column_name,data_type,ColumnNameInInput from bnile-cdw-prod.CCPA_deletion.ccpa_fields """
tables_having_pii  = client.query(sql_1).to_dataframe(progress_bar_type='tqdm')

sql_2="""select table_name from bnile-cdw-prod.CCPA_deletion.tables_having_data_prod """
tables_having_data_prod  = client.query(sql_2).to_dataframe(progress_bar_type='tqdm')

sql_3="""select * from `bnile-cdw-prod.CCPA_deletion.pii_stage_table` where identification_flag=true"""
identification_data=client.query(sql_3).to_dataframe(progress_bar_type='tqdm')

#Creating dictionary with filed name and original value
field_value_dict=dict(zip( identification_data.field_name, identification_data.value))
#creating for field name and mask value
field_mask_value_dict=dict(zip(identification_data.field_name, identification_data.mask_value))

#Filtering required columns only
tables_having_identification_fields=pd.merge(tables_having_pii, identification_data,left_on='ColumnNameInInput',right_on='field_name',how='inner')

#Filtering the tables which have data related to the customer
tables_required_masking=pd.merge( tables_having_identification_fields,tables_having_data_prod,left_on='table_names',right_on='table_name',how='inner')

tables_required_masking.drop('table_name',inplace=True,axis=1)

tables_required_masking_concat_1=tables_required_masking.groupby('table_names')['ColumnNameInInput'].apply(lambda x: ','.join(x)).reset_index()

tables_required_masking_concat_2=tables_required_masking.groupby('table_names')['column_name'].apply(lambda x: ','.join(x)).reset_index()

tables_required_masking_concat_3=tables_required_masking.groupby('table_names')['data_type'].apply(lambda x: ','.join(x)).reset_index()

stage_table=pd.merge(tables_required_masking_concat_1,tables_required_masking_concat_2,on='table_names',how='inner')

final_table=pd.merge(stage_table,tables_required_masking_concat_3,on='table_names',how='inner')

def is_guid_ip_present( ColumnNameInInput):
    columns= ColumnNameInInput.split(',')
    if 'ip_address' in columns or 'guid' in columns:
        return True
    else:
        return False
    

final_table['guid_ip_flag']=False
final_table['guid_ip_flag']=final_table['ColumnNameInInput'].apply(is_guid_ip_present)

#Filter those tables which have guid or ip_address as column
final_table=final_table[final_table.guid_ip_flag==True]

#Update query for all guid column and all ip address columns for those tables having other identification fields
#to confirm the record is related to this customer
#There may be more than one ip_address columns in some table. Same is true for guid

#Update query for all guid column and all ip address columns for those tables having other identification fields
#to confirm the record is related to this customer
#There may be more than one ip_address columns in some table. Same is true for guid
def create_update_query_guid_ip(table_names,column_name,ColumnNameInInput,data_type):
    original_columns=column_name.split(',')
    standard_columns=ColumnNameInInput.split(',')
    data_types=data_type.split(',')
    guid_columns=[]
    ip_columns=[]
    other_columns_original=[]
    other_columns_standard=[]
    other_data_type=[]
    guid_values=field_value_dict['guid'].split(',')
    guid_mask_values=field_mask_value_dict['guid'].split(',')
    ip_values=field_value_dict['ip_address'].split(',')
    ip_mask_values=field_mask_value_dict['ip_address'].split(',')
    for i in range(len(standard_columns)):
        if standard_columns[i]=='guid':
            guid_columns.append(original_columns[i])
        elif standard_columns[i]=='ip_address':
            ip_columns.append(original_columns[i])
        else:
            other_columns_original.append(original_columns[i])
            other_columns_standard.append(standard_columns[i])
            other_data_type.append(data_types[i])
    if len(other_columns_original)==0:
        return None
    query_guid_all=''
    for column in guid_columns:
        query_guid=''
        for i in range(len(guid_values)):
            query_1='update '+table_names+' set '+column+' = '+"'"+guid_mask_values[i]+"'"+' where '+column+' = '+"'"+guid_values[i]+"'"+' and ('
            query_2=''
            for j in range(len(other_columns_standard)):
                other_colum_standard=other_columns_standard[j]
                other_column=other_columns_original[j]
                data_type_1=other_data_type[j]
                values=field_value_dict[other_columns_standard[j]].split(',')
                if  other_colum_standard=='email':
                    query_2=query_2+'lower(trim('+other_column+')) in ('
                else:
                    query_2=query_2+other_column+' in ('
                query_3=''
                if data_type_1.lower()!='string':
                    for value in values:
                        query_3=query_3+'cast('+"'"+value+"'"+' as '+data_type_1+'),'
                    query_2=query_2+query_3[:-1]+') or '
                else:
                    for value in values:
                        query_3=query_3+"'"+value+"'"+','
                    query_2=query_2+query_3[:-1]+') or '
            query_1=query_1+query_2[:-3]+');'
            query_guid=query_guid+query_1
        query_guid_all=query_guid_all+query_guid
    query_ip_all=''
    for column in ip_columns:
        query_ip=''
        for i in range(len(ip_values)):
            query_4='update '+table_names+' set '+column+' = '+"'"+ip_mask_values[i]+"'"+' where '+column+' = '+"'"+ip_values[i]+"'"+' and ('
            query_5=''
            for j in range(len(other_columns_standard)):
                other_colum_standard=other_columns_standard[j]
                other_column=other_columns_original[j]
                data_type_2=other_data_type[j]
                values=field_value_dict[other_columns_standard[j]].split(',')
                if  other_colum_standard=='email':
                    query_5=query_5+'lower(trim('+other_column+')) in ('
                else:
                    query_5=query_5+other_column+' in ('
                query_6=''
                if data_type_2.lower()!='string':
                    for value in values:
                        query_6=query_6+'cast('+"'"+value+"'"+' as '+data_type_2+'),'
                    query_5=query_5+query_6[:-1]+') or '
                else:
                    for value in values:
                        query_6=query_6+"'"+value+"'"+','
                    query_5=query_5+query_6[:-1]+') or '
            query_4=query_4+query_5[:-3]+');'
            query_ip=query_ip+query_4
        query_ip_all=query_ip_all+query_ip
    return query_guid_all+query_ip_all
    

final_table['query']=final_table.apply(lambda row:  create_update_query_guid_ip(row['table_names'],row['column_name'],row['ColumnNameInInput'],row['data_type']),axis=1)


# providing the table_name 
table_id = 'bnile-cdw-prod.CCPA_deletion.update_query_2_1'

job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")

# Make an API request for creating the table
job = client.load_table_from_dataframe(
    final_table, table_id, job_config=job_config
)  
print(job.result())



