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
service=Variable.get("nile_service")
o_port=Variable.get("port")
o_project=Variable.get("project_id")
o_stg_dataset=Variable.get("stg_dataset")
o_tgt_dataset=Variable.get("tgt_dataset")
#Connectig to on-prem oracle
dsn=cx_Oracle.makedsn('"'+service+'"', o_port, service_name='"'+service+'"')
print("DSN is created")

connection=cx_Oracle.connect(DB_USER, DB_PASS, dsn, encoding="UTF-8")

print("Connection is created")
#Pulling the maximum date from the targt (google bigquery)
# max_date = """SELECT coalesce(max(timestamp),'1775-04-04 00:00:00 UTC') FROM `{0}.{1}.daily_offer_fact`""".format(o_project, o_tgt_dataset)
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
query="""SELECT
distinct
trim(REGEXP_REPLACE(wop.offer_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS offer_id
,trim(REGEXP_REPLACE(wop.site,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS site
,trim(REGEXP_REPLACE(wop.currency_code,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS currency_code
,trim(REGEXP_REPLACE(website_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS website_price
,trim(REGEXP_REPLACE(website_list_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS website_list_price
,trim(REGEXP_REPLACE(representative_sku,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS representative_sku
,trim(REGEXP_REPLACE(ring_is_comfort_fit,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS ring_is_comfort_fit
,trim(REGEXP_REPLACE(min_stone_count,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS min_stone_count
,trim(REGEXP_REPLACE(max_stone_count,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS max_stone_count
,trim(REGEXP_REPLACE(max_chain_length_inches,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS max_chain_length_inches
,trim(REGEXP_REPLACE(min_chain_length_inches,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS min_chain_length_inches
,trim(REGEXP_REPLACE(average_customer_rating,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS average_customer_rating
,trim(REGEXP_REPLACE(sell_without_diamond,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS sell_without_diamond
,trim(REGEXP_REPLACE(is_sale_item,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_sale_item
,trim(REGEXP_REPLACE(product_current_status,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS product_current_status
,trim(REGEXP_REPLACE(any_product_sellable,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS any_product_sellable
,trim(REGEXP_REPLACE(eligible_to_search,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS eligible_to_search
,trim(REGEXP_REPLACE(is_all_backordered,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_all_backordered
,trim(REGEXP_REPLACE(is_any_backordered,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_any_backordered
,trim(REGEXP_REPLACE(is_limited_availability_item,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_limited_availability_item
,trim(REGEXP_REPLACE(is_engraveable,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_engraveable
,trim(REGEXP_REPLACE(is_engrave_is_monogrammable,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_engrave_is_monogrammable
,trim(REGEXP_REPLACE(is_locally_sourced,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_locally_sourced
,trim(REGEXP_REPLACE(max_ships_in_days,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS max_ships_in_days
,trim(REGEXP_REPLACE(min_ships_in_days,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS min_ships_in_days
,trim(REGEXP_REPLACE(min_price_with_contained,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS min_price_with_contained
,trim(REGEXP_REPLACE(min_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS min_price
,trim(REGEXP_REPLACE(max_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS max_price
,trim(REGEXP_REPLACE(min_price_all_totalled,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS min_price_all_totalled
,trim(REGEXP_REPLACE(max_price_all_totalled,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS max_price_all_totalled
,trim(REGEXP_REPLACE(retail_comparison_price,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS retail_comparison_price
,trim(REGEXP_REPLACE(retail_comparison_price_tot,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS retail_comparison_price_tot

FROM (
  SELECT
    offer_id,
    site,
    currency_code,
    nvl(min_price_with_contained,
      min_price) website_price,
    nvl(retail_comparison_price,
      min_price_with_contained) website_list_price,
    min_price_with_contained,
    min_price,
    max_price,
    min_price_all_totalled,
    max_price_all_totalled,
    retail_comparison_price,
    retail_comparison_price_tot
  FROM
    o_product.website_offer_price_v
  WHERE
    CASE
      WHEN site = 'BN' AND currency_code = 'USD' THEN 1
      WHEN site = 'BNUK'
    AND currency_code = 'GBP' THEN 1
      WHEN site = 'BNCA' AND currency_code = 'CAD' THEN 1
      WHEN site = 'BNCN'
    AND currency_code = 'CNY' THEN 1
    ELSE
    0
  END
    = 1) wop
LEFT JOIN (
  SELECT
    offer_id,
    site,
    representative_sku,
    ring_is_comfort_fit,
    min_stone_count,
    max_stone_count,
    max_chain_length_inches,
    min_chain_length_inches,
    average_customer_rating,
    sell_without_diamond,
    is_sale_item,
    product_current_status,
    any_product_sellable,
    eligible_to_search,
    is_all_backordered,
    is_any_backordered,
    is_limited_availability_item,
    is_engraveable,
    is_engrave_is_monogrammable,
    is_locally_sourced,
    max_ships_in_days,
    min_ships_in_days
  FROM
    O_PRODUCT.WEBSITE_OFFER_V o) wo
ON
  (wop.offer_id = wo.offer_id
    AND wop.site = wo.site)"""
#executing the query and loading the data to data frame in chunks
df1=pd.read_sql(query,connection,chunksize=1000000)
#Loading the data from  dataframe df1 to google bigquery staging table in chunks
for chunk in df1:
    df2=chunk
    df2.to_gbq(destination_table=o_stg_dataset+'.stg_daily_offer_fact',project_id=o_project,if_exists='append',chunksize=1000000)
#Check for the  stage stable if not create the stage table.
check_for_stag_table="""select 'TRUE' CHECK from `{0}.{1}.stg_daily_offer_fact` limit 1""".format(o_project, o_stg_dataset)
try:
    df= client.query(check_for_stag_table).to_dataframe()
    print("table already exists")
except:
    df3=pd.read_sql(query,connection)
    df3.to_gbq(destination_table=o_stg_dataset+'.stg_daily_offer_fact',project_id=o_project,if_exists='append',chunksize=1000000)