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
sql_3="""select * from `bnile-cdw-prod.CCPA_deletion.pii_stage_table` where field_name not in ('guid','ip_address') """
pii_stage_table=client.query(sql_3).to_dataframe(progress_bar_type='tqdm')


identification_data=pii_stage_table[pii_stage_table.identification_flag==True]
 

#load all fileds' original values and mask values other than guid and ip address
sql_4="""select * from `bnile-cdw-prod.CCPA_deletion.pii_stage_table` where field_name not in ('guid','ip_address') and mask_flag=true and mask_value not like '%,%'"""
mask_data=client.query(sql_4).to_dataframe(progress_bar_type='tqdm')


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

def set_column_original(column_original,column_input):
    column_original_list=column_original.split(',')
    column_input_list=column_input.split(',')
    set_columns=''
    for i in range(len(column_input_list)):
        if column_input_list[i] in masking_fields_list:
            set_columns=set_columns+column_original_list[i]+','
    return set_columns[:-1]
    

def where_column_original(column_original,column_input):
    column_original_list=column_original.split(',')
    column_input_list=column_input.split(',')
    where_columns=''
    for i in range(len(column_input_list)):
        if column_input_list[i] in identification_fields_list:
            where_columns=where_columns+column_original_list[i]+','
    return where_columns[:-1]
    

def set_column_input(column_input):
    column_input_list=column_input.split(',')
    set_columns=''
    for i in range(len(column_input_list)):
        if column_input_list[i] in masking_fields_list:
            set_columns=set_columns+column_input_list[i]+','
    return set_columns[:-1]
    

def where_column_input(column_input):
    column_input_list=column_input.split(',')
    where_columns=''
    for i in range(len(column_input_list)):
        if column_input_list[i] in identification_fields_list:
            where_columns=where_columns+column_input_list[i]+','
    return where_columns[:-1]
    

final_table['set_column_original']=final_table.apply(lambda row : set_column_original(row['column_name'],row['ColumnNameInInput']),axis=1)

final_table['where_column_original']=final_table.apply(lambda row : where_column_original(row['column_name'],row['ColumnNameInInput']),axis=1)

final_table['set_column_input']=final_table['ColumnNameInInput'].apply( set_column_input)

final_table['where_column_input']=final_table['ColumnNameInInput'].apply( where_column_input)

final_table['set_data_type']=final_table.apply(lambda row : set_column_original(row['data_type'],row['ColumnNameInInput']),axis=1)

final_table['where_data_type']=final_table.apply(lambda row : where_column_original(row['data_type'],row['ColumnNameInInput']),axis=1)

def create_upadate_query_priority_1(table_names,set_column_original,where_column_original,set_column_input,where_column_input,set_data_type,where_data_type):
    if set_column_original=='':
        return None
    if where_column_original=='':
        return None
    
    set_column_original_list=set_column_original.split(',')
    where_column_original_list=where_column_original.split(',')
    set_column_input_list=set_column_input.split(',')
    where_column_input_list=where_column_input.split(',')
    set_data_type_list=set_data_type.split(',')
    where_data_type_list=where_data_type.split(',')
    query='update '+ table_names +' set '
    query_1=''
    for i in range(len(set_column_original_list)):
        
        column=set_column_original_list[i]
        mask= field_mask_value_dict[set_column_input_list[i]]
        data_type=set_data_type_list[i]
        if data_type.lower()!='string':
            query_1=query_1 + column +' = ' + 'cast('+ "'" + mask + "'" + ' as '+data_type +'),'
        else:
            query_1=query_1 + column +' = ' + "'" + mask + "'" +','
    query=query+query_1[:-1]
    query_2=' where '
    #creating where part
    for i in range(len(where_column_original_list)):
        column_standard=where_column_input_list[i]
        column=where_column_original_list[i]
        values= field_value_dict[where_column_input_list[i]].split(',')
        data_type=where_data_type_list[i]
        if  column_standard=='email':
            query_2=query_2+'trim(lower ('+ column + '))' 
        else:
            query_2=query_2+ column
        if len(values)==1:
            if data_type.lower()!='string':
                query_2=query_2 +  ' = ' 'cast(' + "'" + values[0] + "'"+' as ' + data_type+') or '
            else:
                query_2=query_2 +  ' = ' + "'" + values[0] + "'" + ' or '
        else:
            query_2=query_2 + ' in ('
            if data_type.lower()!='string':
                for value in values:
                    query_2=query_2+' cast ('+"'"+value+"'"+' as '+data_type+'),'
            else:
                for value in values:
                    query_2=query_2 + "'" + value+ "'"+','
            query_2=query_2[:-1]+') or '
    query=query+query_2[:-3]
    return query
                
    
            

final_table['query']=final_table.apply(lambda row: create_upadate_query_priority_1(row['table_names'],row['set_column_original'],row['where_column_original'],row['set_column_input'],row['where_column_input'],row['set_data_type'],row['where_data_type']),axis=1)


#creating the table containing update_queries combined
table_id = 'bnile-cdw-prod.CCPA_deletion.update_query_3'
job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")
# Make an API request for creating the table
job = client.load_table_from_dataframe(
     final_table, table_id,job_config=job_config
) 

