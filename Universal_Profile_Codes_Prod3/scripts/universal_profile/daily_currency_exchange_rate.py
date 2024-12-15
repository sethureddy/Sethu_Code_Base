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
o_warehouse_dataset=Variable.get("gcp_project_warehouse_dataset")
#Connectig to on-prem oracle
dsn=cx_Oracle.makedsn('"'+service+'"', o_port, service_name='"'+service+'"')
connection=cx_Oracle.connect(DB_USER, DB_PASS, dsn, encoding="UTF-8")
#Pulling the maximum date from the targt (google bigquery)
# max_date = """SELECT coalesce(max(timestamp),'1775-04-04 00:00:00 UTC') FROM `{0}.{1}.offer_dim`""".format(o_project, o_product_dataset)
#Pulling the minium date from the targt (google bigquery)
# min_date= """select cast('1775-04-04 00:00:00 UTC' as timestamp)"""
#Try for maximum data if not take the minimum data and pass it to the date_1 variable
# try:
	# df= client.query(max_date).to_dataframe()
# except:
	# df= client.query(min_date).to_dataframe()
# df['f0_'] = df['f0_'].apply(lambda x : pd.to_datetime(str(x)))
# df['f0_'] = df['f0_'].dt.date
# x = df['f0_'].values[0]
# date_1=str(x)
#Query to pull the full data.
query="""
SELECT
trim(REGEXP_REPLACE(conversion_day,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS conversion_day
,trim(REGEXP_REPLACE(currency_code_from,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS currency_code_from
,trim(REGEXP_REPLACE(currency_code_to,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS currency_code_to
,trim(REGEXP_REPLACE(conversion_rate,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS conversion_rate
,trim(REGEXP_REPLACE(last_oracle_change_date,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS last_oracle_change_date
FROM
(
select to_char(conversion_day, 'YYYY-MM-DD') AS conversion_day
,currency_code_from as currency_code_from
,currency_code_to as currency_code_to
,conversion_rate as conversion_rate
,to_char(last_oracle_change_date, 'YYYY-MM-DD') as last_oracle_change_date
from o_bluenile.currency_exchange_daily@nileprod_read 
where conversion_day = trunc(sysdate)
)
"""
#executing the query and loading the data to data frame in chunks
df1=pd.read_sql(query,connection,chunksize=1000000)
#Loading the data from  dataframe df1 to google bigquery staging table in chunks
for chunk in df1:
    df2=chunk
    df2.to_gbq(destination_table=o_stg_dataset+'.daily_currency_exchange_rate',project_id=o_project,if_exists='append',chunksize=1000000)
#Check for the  stage stable if not create the stage table.
check_for_stag_table="""select 'TRUE' CHECK from `{0}.{1}.daily_currency_exchange_rate` limit 1""".format(o_project, o_stg_dataset)
try:
    df= client.query(check_for_stag_table).to_dataframe()
    print("table already exists")
except:
    df3=pd.read_sql(query,connection)
    df3.to_gbq(destination_table=o_stg_dataset+'.daily_currency_exchange_rate',project_id=o_project,if_exists='append',chunksize=1000000)