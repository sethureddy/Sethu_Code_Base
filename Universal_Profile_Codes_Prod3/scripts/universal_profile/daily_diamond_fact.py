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
o_diamond_dataset=Variable.get("diamond_dataset")
#Connectig to on-prem oracle
dsn=cx_Oracle.makedsn('"'+service+'"', o_port, service_name='"'+service+'"')
connection=cx_Oracle.connect(DB_USER, DB_PASS, dsn, encoding="UTF-8")
#Pulling the maximum date from the targt (google bigquery)
max_date = """select coalesce(timestamp(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)))"""
#max_date_key = """SELECT coalesce(max(snapshot_date_key),20170101) FROM `{0}.{1}.daily_diamond_fact`""".format(o_project, o_diamond_dataset)
#Pulling the minium date from the targt (google bigquery)
min_date= """select cast('1775-04-04 00:00:00 UTC' as timestamp)"""
#Try for maximum data if not take the minimum data and pass it to the date_1 variable
try:
	df= client.query(max_date).to_dataframe()
except:
	df= client.query(min_date).to_dataframe()
df['f0_'] = df['f0_'].apply(lambda x : pd.to_datetime(str(x)))
df['f0_'] = df['f0_'].dt.date
x = df['f0_'].values[0]
date_1=str(x)
#Query to pull the full data.
query="""SELECT
trim(REGEXP_REPLACE(d.sku,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS sku
,trim(REGEXP_REPLACE(ds.site,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS site
,coalesce(first_available_date,to_date('1775-04-04 00:00:00','YYYY-MM-DD HH24:MI:SS'))  AS first_available_date
,trim(REGEXP_REPLACE(sell,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS sell
,trim(REGEXP_REPLACE(sell_default,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS sell_default
,trim(REGEXP_REPLACE(is_three_stone_cntr_compatible,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_three_stone_cntr_compatible
,trim(REGEXP_REPLACE(is_pendant_compatible,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_pendant_compatible
,trim(REGEXP_REPLACE(dsp.price_all_inclusive,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS all_inclusive_price
,trim(REGEXP_REPLACE(dsp.price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS price
,trim(REGEXP_REPLACE(dsp.transactional_currency_code,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS transactional_currency_code
,trim(REGEXP_REPLACE(dsp.price_override_mode,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS price_override_mode
,trim(REGEXP_REPLACE(d.vendor_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS usd_vendor_price
,trim(REGEXP_REPLACE(d.vendor_price_override_mode,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS vendor_price_override_mode
,trim(REGEXP_REPLACE(d.invoice_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS usd_invoice_price
,trim(REGEXP_REPLACE(d.vendor_sku,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS vendor_sku
,trim(REGEXP_REPLACE(d.memo_to_vendor_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS memo_to_vendor_id
,trim(REGEXP_REPLACE(d.owned_by_vendor_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS owned_by_vendor_id
,trim(REGEXP_REPLACE(d.vendor_on_memo,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS vendor_on_memo
,trim(REGEXP_REPLACE(d.internal_vendor_notes,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS internal_vendor_notes
,trim(REGEXP_REPLACE(d.vendor_location_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS vendor_location_id
,coalesce(d.purchase_date,to_date('1775-04-04 00:00:00','YYYY-MM-DD HH24:MI:SS'))  AS purchase_date
,trim(REGEXP_REPLACE(d.purchased_from_vendor_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS purchased_from_vendor_id
,trim(REGEXP_REPLACE(d.promised_basket_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS promised_basket_id
,trim(REGEXP_REPLACE(d.ships_in_days,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS ships_in_days
,trim(REGEXP_REPLACE(d.supplied_vendor_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS supplied_usd_vendor_price
,trim(REGEXP_REPLACE(ds.exclusion_reason,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS exclusion_reason
from
O_PRODUCT.DIAMOND@nileprod_read d,
O_PRODUCT.DIAMOND_SITE@nileprod_read ds,
O_PRODUCT.DIAMOND_SITE_PRICE@nileprod_read dsp
where(
d.change_date>=to_date(:date_1,'YYYY-MM-DD')
or ds.change_date>=to_date(:date_1,'YYYY-MM-DD')
or dsp.change_date>=to_date(:date_1,'YYYY-MM-DD')
)
and
d.sku = ds.sku
and d.sku = dsp.sku
and ds.sku = dsp.sku
and ds.site = dsp.site
and case
        when dsp.site = 'BN' and transactional_currency_code = 'USD' then 1
        when dsp.site = 'BNUK' and transactional_currency_code = 'GBP' then 1
        when dsp.site = 'BNCA' and transactional_currency_code = 'CAD' then 1
        when dsp.site = 'BNCN' and transactional_currency_code = 'CNY' then 1
        else 0
    end = 1"""
#executing the query and loading the data to data frame in chunks
df1=pd.read_sql(query,connection,params=[date_1],chunksize=1000000)
#Loading the data from  dataframe df1 to google bigquery staging table in chunks
for chunk in df1:
    df2=chunk
    df2.to_gbq(destination_table=o_stg_dataset+'.daily_diamond_fact',project_id=o_project,if_exists='append',chunksize=1000000)
#Check for the  stage stable if not create the stage table.
check_for_stag_table="""select 'TRUE' CHECK from `{0}.{1}.daily_diamond_fact` limit 1""".format(o_project, o_stg_dataset)
try:
    df= client.query(check_for_stag_table).to_dataframe()
    print("table already exists")
except:
    df3=pd.read_sql(query,connection,params=[date_1])
    df3.to_gbq(destination_table=o_stg_dataset+'.daily_diamond_fact',project_id=o_project,if_exists='append',chunksize=1000000)