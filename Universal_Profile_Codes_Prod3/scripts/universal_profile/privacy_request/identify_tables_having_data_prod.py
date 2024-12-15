from pandas.io import gbq
import pandas as pd
import numpy as np
from google.cloud import bigquery as bgq 
client = bgq.Client()

# loading field name original value and mask value
sql_1 = """SELECT * FROM `bnile-cdw-prod.CCPA_deletion.pii_stage_table`"""
pii_stage_table  = client.query(sql_1).to_dataframe(progress_bar_type='tqdm')

identification_fields= list(pii_stage_table[pii_stage_table.identification_flag==True]['field_name'])
identification_values = list(pii_stage_table[pii_stage_table.identification_flag==True]['value'])
pii_fields= list(pii_stage_table[pii_stage_table.mask_flag==True]['field_name'])

for x,y in zip(identification_fields ,  identification_values):
    globals()[x] = y

# loading the tables having pii and identification fields both
sql_3 = """select * from bnile-cdw-prod.CCPA_deletion.ccpa_fields"""
tables_to_be_modified  = client.query(sql_3).to_dataframe(progress_bar_type='tqdm')

def ind_pii_check(Input):
    count =0
    count2 =0
    for value in Input.split(','):
        if count ==1:
            break
        if value in identification_fields:
            count =1
    for value in Input.split(','):
        if count2 ==1:
            break
        if value in pii_fields:
            count2 =1
    if count+count2>1:
        return True
    else:
        return False

# grouping the fields that exists in each table
concated_fields= tables_to_be_modified.groupby('table_names')['ColumnNameInInput'].apply(lambda x: ','.join(x)).reset_index()
concated_fields['Fields_exists'] = concated_fields.apply(lambda row : ind_pii_check(row['ColumnNameInInput']), axis =1)


# filtering the Fields based on the indentification fields and pii fields
tables_to_be_modified = pd.merge(concated_fields['table_names'][concated_fields['Fields_exists'] == True],tables_to_be_modified, on = 'table_names', how = 'inner' )

tables_to_be_modified = tables_to_be_modified[np.in1d(tables_to_be_modified.ColumnNameInInput,  identification_fields)]

def concate_select(column, table_names, input_column, data_type):
    if input_column == 'bnid':
        return multiple_value_select_queries(column, table_names, bnid)
    if input_column == 'email':
        return  multiple_value_select_queries_email(column, table_names, email) 
    elif input_column == 'phone_number' :
        return multiple_value_select_queries_cust_id(column, table_names, phone_number, data_type) 
    elif input_column == 'customer_id' :
        return multiple_value_select_queries_cust_id(column, table_names, customer_id,data_type)
    elif input_column == 'shipment_tracking_id':
        return multiple_value_select_queries_cust_id(column, table_names, shipment_tracking_id,data_type)
    elif input_column == 'order_number':
        return multiple_value_select_queries_cust_id(column, table_names, order_number,data_type)
    elif input_column == 'up_email_address_key':
        return multiple_value_select_queries_cust_id(column, table_names, up_email_address_key,data_type)
    elif input_column == 'ip_address':
        return multiple_value_select_queries(column, table_names, ip_address)
    elif input_column == 'guid':
        return multiple_value_select_queries(column, table_names, guid)
    elif input_column == 'basket_id':
        return multiple_value_select_queries_cust_id(column, table_names, basket_id,data_type)
    elif input_column == 'email_address_key':
        return multiple_value_select_queries_cust_id(column, table_names, email_address_key, data_type)
    elif input_column == 'address_key':
        return multiple_value_select_queries_cust_id(column, table_names, address_key, data_type)
    else:
        return 1


## sub function for customer id
def multiple_value_select_queries_cust_id(column, table_names, value,data_type):
    
    list_of_queries = ''
    query2 = ''
    
    query1 = 'select count(*) as no_of_rows' + ' from ' + table_names + ' where ' + column + ' in ' + '('
        
    for i in value.split(','):
        
        query2 = query2 + 'cast('+ "'"+ i + "'" + ' as ' + data_type +  ')'+','
        
    query2 =  query2 [:-1]
        
    list_of_queries = query1+query2 + ');'
        
    return list_of_queries


### Sub Function Generates the Queries for Mutiple values for  bnid, phone_number
def multiple_value_select_queries(column, table_names, value):
    
    list_of_queries = ''
    query2 = ''
    
    query1 = 'select count(*) as no_of_rows' + ' from ' + table_names + ' where ' + column + ' in ' + '('
        
    for i in value.split(','):
        
        query2 = query2 + "'"+ i  + "'"+','
    query2 =  query2 [:-1]
        
    list_of_queries = query1+query2 + ');' 
        
    return list_of_queries



### Sub-function Generates the Queries for Mutiple values for email
def multiple_value_select_queries_email(column, table_names, value):
    
    list_of_queries = ''
    query2 = ''
    
    query1 = 'select count(*) as no_of_rows' + ' from ' + table_names + ' where ' +'trim(lower ('+ column + '))' + ' in ' + '('
        
    for i in value.split(','):
        
        query2 = query2 + "'"+ i  + "'"+','
    query2 =  query2 [:-1]
        
    list_of_queries = query1+query2 + ');' 
        
    return list_of_queries


# Applying the Function to the Tabular data to generate the Queries
tables_to_be_modified['queries'] = ''
tables_to_be_modified['queries'] = tables_to_be_modified.apply(lambda x : concate_select(x['column_name'], x['table_names'],x['ColumnNameInInput'], x['data_type']),axis=1)

## Combining the mutiple delete statements for single table
# taking the empty dictonary
select_statements={}

# converting the Mutiple queries for single table to single Query for a table
for i,b in zip(tables_to_be_modified['table_names'], tables_to_be_modified['queries']):
    if i not in select_statements.keys():
        select_statements[i] = b[:-1]
    elif i in select_statements.keys():
        select_statements[i] = select_statements[i] +' or '+ b.split('where',1)[-1][:-1]



# creating the  data frame       
select_statements_data = pd.DataFrame(data = select_statements, index = [0]).transpose().reset_index()



# Function for adding the column at the end of the delete statements
def semicolon(value):
    if value[-1] == ';':
        return value
    else: 
        return value + ';'

# renaming the columns
select_statements_data.rename(columns = {'index':'table_name', 0: 'query'}, inplace = True) 


# Adding the Semicolon to the Query
select_statements_data['query'] =  select_statements_data.apply (lambda x :  semicolon(x['query']), axis=1 )


# adding the new column for the row_number starting with index 1
select_statements_data.index = np.arange(1, len(select_statements_data)+1)
select_statements_data.reset_index(inplace = True)
select_statements_data.rename(columns = {'index':'row_num'}, inplace = True)

select_statements_data.to_csv('select_statements_prod.csv')

##Executing those select queries to filter the tables which have data related to the customer
list_queries_not_executed= []
list_tables_having_data= []
select_statements_data ['number_of_rows_selected'] = np.nan
for i, j in zip(select_statements_data['row_num'], select_statements_data['query']):
    try:
        a = client.query(j)
        a.result() # raising error when the query is not executed
        result = client.query(j).to_dataframe()
        count=list(result['no_of_rows'])[0]
        if count>0:
            select_statements_data.loc[select_statements_data['row_num']== i,'number_of_rows_selected'] = count
            list_tables_having_data.append(i)
    except:
        list_queries_not_executed.append(i)
        
tables_having_data_prod = select_statements_data[np.in1d(select_statements_data.row_num, list_tables_having_data)]
tables_for_select_query_not_executed_prod = select_statements_data[np.in1d(select_statements_data.row_num,  list_queries_not_executed )]
tables_for_select_query_not_executed_prod.drop('number_of_rows_selected',inplace=True,axis=1)

# tables_having_data_prod.to_csv('data/privacy_request/output/tables_having_data_prod.csv')
# tables_for_select_query_not_executed_prod.to_csv('data/privacy_request/output/tables_for_select_query_not_executed_prod.csv')

#creating the table containing the details: tables having data to be updated in prod
table_id = 'bnile-cdw-prod.CCPA_deletion.tables_having_data_prod'
job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")
# Make an API request for creating the table
job = client.load_table_from_dataframe(
    tables_having_data_prod, table_id,job_config=job_config
) 

#creating the table containing the details: select query is not executed
table_id = 'bnile-cdw-prod.CCPA_deletion.select_query_not_executed_prod'
job_config = bgq.LoadJobConfig( write_disposition="WRITE_TRUNCATE")
# Make an API request for creating the table
job = client.load_table_from_dataframe(
     tables_for_select_query_not_executed_prod, table_id,job_config=job_config
) 