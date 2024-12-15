from pandas.io import gbq
import pandas as pd
import numpy as np
from google.cloud import bigquery as bgq
client = bgq.Client()

# taking the indentification fields storing into the list
sql_1 = """SELECT * FROM `bnile-cdw-prod.CCPA_deletion.pii_stage_table`where identification_flag=true and (field_name='guid' or value not Like '%,%')"""
identification_data  = client.query(sql_1).to_dataframe(progress_bar_type='tqdm')
 
identification_fields= list(identification_data.field_name)
identification_values=list(identification_data.value)


for x,y in zip( identification_fields, identification_values ):
    globals()[x] = y

# loading the tables having pii and identification fields both
sql_2 = """select * from bnile-cdw-prod.CCPA_deletion.ccpa_fields"""
tables_having_pii  = client.query(sql_2).to_dataframe(progress_bar_type='tqdm')


tables_to_check=pd.merge(tables_having_pii, identification_data,left_on='ColumnNameInInput',right_on='field_name',how='inner')

sql_3="""select table_name from bnile-cdw-prod.CCPA_deletion.tables_having_data_prod """
tables_having_data_prod  = client.query(sql_3).to_dataframe(progress_bar_type='tqdm')

tables_to_check=pd.merge(tables_to_check ,tables_having_data_prod,left_on='table_names',right_on='table_name',how='inner')

tables_to_check_1= tables_to_check.groupby('table_names')['ColumnNameInInput'].apply(lambda x: ','.join(x)).reset_index()

tables_to_check_2= tables_to_check.groupby('table_names')['column_name'].apply(lambda x: ','.join(x)).reset_index()

tables_to_check=pd.merge(tables_to_check_1,tables_to_check_2,on='table_names',how='inner')

#This function will check if guid and some other field is present for a table 
def is_guid_present(column):
    columns=column.split(',')
    if 'guid' in columns and len(columns)>1:
        return True
    else: 
        return False

tables_to_check['both_flag']=False
tables_to_check['both_flag']=tables_to_check['ColumnNameInInput'].apply(is_guid_present)

tables_to_check=tables_to_check[tables_to_check.both_flag==True]

#Funtion to create group by statements over guid
def create_group_by_statement(table_name,column_name,ColumnNameInInput):
    original_column_names=column_name.split(',')
    standard_column_names=ColumnNameInInput.split(',')
    check_columns=[]
    guid_original=[]
    for i in range(len(original_column_names)):
        if  standard_column_names[i]=='guid':
            guid_original.append(original_column_names[i])
        if standard_column_names[i]!='guid' and standard_column_names[i]!='ip_address':
            check_columns.append(original_column_names[i])
    guid_values=guid.split(',')
    query=''
    for i in range(len(guid_original)):
        query_1='select '+ guid_original[i] 
        query_2=''
        for column in  check_columns:
            query_2=query_2+', count(distinct '+ column+') as no_of_'+column
        query_1=query_1+query_2+' from '+table_name+ ' where '+guid_original[i] +' in ('
        query_3=''
        for value in guid_values:
            query_3=query_3+"'"+value+"'"+','
        query_1=query_1+query_3[:-1]+') group by '+guid_original[i] +' having '
        query_4=''
        for column in check_columns:
            query_4=query_4+' no_of_'+column+ ' >1 or '
        query_1=query_1+query_4[:-3]
        query=query+query_1+';'
    return query[:-1]

tables_to_check['query']=''
tables_to_check['query']=tables_to_check.apply(lambda x:create_group_by_statement(x['table_names'],x['column_name'],x['ColumnNameInInput']),axis=1)

tables_to_check=tables_to_check.reset_index(drop=True)

# tables_to_check.to_csv('data/privacy_request/output/problematic_guid_groupby_query.csv')

list_queries_not_executed= []
list_tables_having_data= []
 
guid_associated_with_multiple_customer=[]
for i, j in zip(tables_to_check.index, tables_to_check['query']):
    queries=j.split(';')
    for query in queries:
        try:
            a = client.query(query)
            a.result() # raising error when the query is not executed
            result = client.query(query).to_dataframe()
            if result.empty==False:
                guid_original=list(result.columns)[0]
                problematic_guid=list(result[guid_original])
                guid_associated_with_multiple_customer=list(set(guid_associated_with_multiple_customer+problematic_guid))
            print('Executed',i)
        except:
            print('not executed',i)
            list_queries_not_executed.append(i)

#creating the table for which query not executed        
query_not_executed =  tables_to_check[np.in1d( tables_to_check.index,  list_queries_not_executed)]
#creating the table for deletion statements which are not executed
table_id = 'bnile-data-eng-dev.CCPA_deletion.query_not_executed_problematic_guid'
job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")
# Make an API request for creating the table
job = client.load_table_from_dataframe(
     query_not_executed[['table_names','query']], table_id,job_config=job_config
)  

df=pd.DataFrame(columns=['guid'])
for i in range(len(guid_associated_with_multiple_customer)):
    df.loc[i,'guid']=guid_associated_with_multiple_customer[i]


# providing the table_name 
table_id = 'bnile-cdw-prod.CCPA_deletion.problematic_guid'

job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")

# Make an API request for creating the table
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  
print(job.result())

