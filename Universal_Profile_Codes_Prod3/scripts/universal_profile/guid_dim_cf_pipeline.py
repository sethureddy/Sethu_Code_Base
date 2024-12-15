#Install all the required python libraries and import them as shown below
from pandas.io import gbq
import pandas as pd
import numpy as np
from google.cloud import bigquery as bgq
#this library is will help to get the values for the variables that are stored in Airflow server
from airflow.models import Variable
import cx_Oracle

#Create a bigquery connection to google bigquery using gbq library
client = bgq.Client()
#getting the dev credentials that are stored securely in the airflow server
DB_USER=Variable.get("user")
DB_PASS=Variable.get("password")
service=Variable.get("dss_service")
o_port=Variable.get("port")
o_project=Variable.get("project_id")
o_stg_dataset=Variable.get("stg_dataset")
o_tgt_dataset=Variable.get("tgt_dataset")
o_dssprod_o_warehouse_dataset=Variable.get("dssprod_o_warehouse_dataset")
#Connectig to on-prem oracle
dsn=cx_Oracle.makedsn('"'+service+'"', o_port, service_name='"'+service+'"')
connection=cx_Oracle.connect(DB_USER, DB_PASS, dsn, encoding="UTF-8")

#Pulling the maximum date from the targt (google bigquery)
max_date_key = """SELECT coalesce(max(date_key),20200101) FROM `{0}.up_tgt_ora_tables.guid_dim_cf`""".format(o_project)
#Pulling the minium date from the targt (google bigquery)
min_date_key= """select 20200101"""
#Try for maximum data if not take the minimum data and pass it to the date_1 variable
try:
	df= client.query(max_date_key).to_dataframe()
except:
	df= client.query(min_date_key).to_dataframe()
# df['f0_'] = df['f0_'].apply(lambda x : pd.to_datetime(str(x)))
# df['f0_'] = df['f0_'].dt.datetime
x = df['f0_'].values[0]
date_key=x
#getting the maximum date_key count from the target google bigquery
tgt_max_date_key_count=f"""SELECT count(*) as count FROM `{o_project}.{o_dssprod_o_warehouse_dataset}.click_fact` where date_key={date_key} """.format(o_project=o_project, o_dssprod_o_warehouse_dataset=o_dssprod_o_warehouse_dataset,date_key=date_key)
#getting the maximum date_key count from the target google bigquery
tgt_count=client.query(tgt_max_date_key_count).to_dataframe()
#getting the maximum date_key count from the target google bigquery
bq_max_date_key_count=tgt_count['count'].values[0]
#getting the maximum date_key count from the source oracle
src_max_date_key_count=f"""select count(*) as count from  o_warehouse.click_fact where date_key={date_key}""".format(date_key=date_key)
#getting the maximum date_key count from the source oracle
src_count=pd.read_sql(src_max_date_key_count,connection)
#getting the maximum date_key count from the source oracle
ora_max_date_key_count=src_count['COUNT'].values[0]
#validating the sourcce count and target count and passing the relational operator dynamycally.
if(bq_max_date_key_count==ora_max_date_key_count):
    o_operator='>'
else:
    o_operator='>='

#Query to pull the incremental data with date filtered.
query=f"""select cf.guid_key,gd.guid,cf.date_key
from 
(
select distinct cf.guid_key,cf.date_key
from O_WAREHOUSE.CLICK_FACT cf
where cf.date_key{o_operator}{date_key}
)cf
join
O_WAREHOUSE.GUID_DIM gd
on cf.guid_key=gd.guid_key""".format(o_operator=o_operator,date_key=date_key)
#executing the query and loading the data to data frame in chunks
df1=pd.read_sql(query,connection,chunksize=500000)
#Loading the data from  dataframe df1 to google bigquery staging table in chunks
for chunk in df1:
    df2=chunk
    df2.to_gbq(destination_table=o_stg_dataset+'.guid_dim_cf',project_id=o_project,if_exists='append',chunksize=500000)
#Check for the  stage stable if not create the stage table.
check_for_stag_table="""select 'TRUE' CHECK from `{0}.{1}.guid_dim_cf` limit 1""".format(o_project, o_stg_dataset)
try:
    df= client.query(check_for_stag_table).to_dataframe()
    print("table already exists")
except:
    df3=pd.read_sql(query,connection)
    df3.to_gbq(destination_table=o_stg_dataset+'.guid_dim_cf',project_id=o_project,if_exists='append',chunksize=100000)