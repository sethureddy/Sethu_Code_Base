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
o_product_dataset=Variable.get("product_dataset")
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
SELECT final.*,trim(REGEXP_REPLACE(regexp_substr(feed_category,'[> ]*([A-Za-z& ]*)$',1,1,NULL,1),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS MARKETING_CATEGORY
FROM
(
SELECT
trim(REGEXP_REPLACE(OFFER_ID,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS offer_id
,trim(REGEXP_REPLACE(MAX(NAME),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS name
,trim(REGEXP_REPLACE(MAX(DESCRIPTION),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS description
,trim(REGEXP_REPLACE(MAX(BRAND),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS brand
,trim(REGEXP_REPLACE(MAX(OFFER_DISPLAY_TYPE),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS offer_display
,trim(REGEXP_REPLACE(MAX(TARGET_GENDER),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS target_gender
,trim(REGEXP_REPLACE(MAX(DEPARTMENT_NAME),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS department_name
,trim(REGEXP_REPLACE(MAX(CATEGORY_NAME),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS category_name
,trim(REGEXP_REPLACE(MAX(CLASS_NAME),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS class_name
,trim(REGEXP_REPLACE(MAX(SUB_CLASS_NAME),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS sub_class_name
,trim(REGEXP_REPLACE(MAX(MERCH_PRODUCT_CATEGORY),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS merch_product_category
,trim(REGEXP_REPLACE(MAX(MERCH_CATEGORY_ROLLUP),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS merch_category_rollup
,trim(REGEXP_REPLACE(MAX(MERCH_SUB_CATEGORY_ROLLUP),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS merch_sub_category_rollup
,trim(REGEXP_REPLACE(MAX(PRIMARY_METAL_NAME),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS primary_metal_name
,trim(REGEXP_REPLACE(MAX(PRIMARY_METAL_COLOR),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS primary_metal_color
,trim(REGEXP_REPLACE(MAX(PRIMARY_METAL_PURITY),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS primary_metal_purity
,trim(REGEXP_REPLACE(MAX(PRIMARY_STONE_CATEGORY),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS primary_stone_category
,trim(REGEXP_REPLACE(MAX(PRIMARY_STONE_TYPE),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS primary_stone_type
,trim(REGEXP_REPLACE(MAX(IMAGE_URL),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS image_url
,trim(REGEXP_REPLACE(MAX(FEED_CATEGORY),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS feed_category
,trim(REGEXP_REPLACE(MAX(NAME_PLAINTEXT),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS name_plaintext
,trim(REGEXP_REPLACE(MAX(DESCRIPTION_PLAINTEXT),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS description_plaintext
,trim(REGEXP_REPLACE(MAX(SHORT_NAME_PLAINTEXT),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS short_name_plaintext
,trim(REGEXP_REPLACE(MAX(ALTERNATE_IMAGE_URL),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS alternate_image_url
,trim(REGEXP_REPLACE(MAX(REPRESENTATIVE_SKU),'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS representative_sku

FROM
(
SELECT 
DISTINCT(to_char(OFFER_ID))  AS OFFER_ID
,NAME  AS NAME
,DESCRIPTION  AS DESCRIPTION
,BRAND  AS BRAND
,OFFER_DISPLAY_TYPE  AS OFFER_DISPLAY_TYPE
,TARGET_GENDER  AS TARGET_GENDER
,NULL DEPARTMENT_NAME
,NULL CATEGORY_NAME
,NULL CLASS_NAME
,NULL SUB_CLASS_NAME
,NULL MERCH_PRODUCT_CATEGORY
,NULL MERCH_CATEGORY_ROLLUP
,NULL MERCH_SUB_CATEGORY_ROLLUP
,NULL PRIMARY_METAL_NAME
,NULL PRIMARY_METAL_COLOR
,NULL PRIMARY_METAL_PURITY
,NULL PRIMARY_STONE_CATEGORY
,NULL PRIMARY_STONE_TYPE
,NULL IMAGE_URL
,NULL FEED_CATEGORY
,NAME_PLAINTEXT  AS NAME_PLAINTEXT
,DESCRIPTION_PLAINTEXT  AS DESCRIPTION_PLAINTEXT
,SHORT_NAME_PLAINTEXT  AS SHORT_NAME_PLAINTEXT
,NULL AS ALTERNATE_IMAGE_URL
,REPRESENTATIVE_SKU AS REPRESENTATIVE_SKU
FROM o_product.website_offer_v@nileprod_read

UNION
select
to_char(OFFER_KEY)  AS OFFER_ID
,NAME  AS NAME
,DESCRIPTION  AS DESCRIPTION
,BRAND  AS BRAND
,DISPLAY_TYPE  AS OFFER_DISPLAY_TYPE
,TARGET_GENDER  AS TARGET_GENDER
,DEPARTMENT_NAME  AS DEPARTMENT_NAME
,CATEGORY_NAME  AS CATEGORY_NAME
,CLASS_NAME  AS CLASS_NAME
,SUB_CLASS_NAME  AS SUB_CLASS_NAME
,MERCH_PRODUCT_CATEGORY  AS MERCH_PRODUCT_CATEGORY
,MERCH_CATEGORY_ROLLUP  AS MERCH_CATEGORY_ROLLUP
,MERCH_SUB_CATEGORY_ROLLUP  AS MERCH_SUB_CATEGORY_ROLLUP
,PRIMARY_METAL_NAME  AS PRIMARY_METAL_NAME
,PRIMARY_METAL_COLOR  AS PRIMARY_METAL_COLOR
,PRIMARY_METAL_PURITY  AS PRIMARY_METAL_PURITY
,PRIMARY_STONE_CATEGORY  AS PRIMARY_STONE_CATEGORY
,PRIMARY_STONE_TYPE  AS PRIMARY_STONE_TYPE
,NULL AS IMAGE_URL
,NULL AS FEED_CATEGORY
,NULL AS NAME_PLAINTEXT
,NULL AS DESCRIPTION_PLAINTEXT
,NULL AS SHORT_NAME_PLAINTEXT
,SCENE7_FULL_SECURE_TYPE AS ALTERNATE_IMAGE_URL
,NULL AS REPRESENTATIVE_SKU
from o_warehouse.offer_dim o
WHERE OFFER_KEY <> 0

UNION
select
to_char(ID)  AS OFFER_ID
,NAME  AS NAME
,DESCRIPTION  AS DESCRIPTION
,BRAND  AS BRAND
,NULL OFFER_DISPLAY_TYPE
,TARGET_GENDER  AS TARGET_GENDER
,NULL DEPARTMENT_NAME
,NULL CATEGORY_NAME
,NULL CLASS_NAME
,NULL SUB_CLASS_NAME
,NULL MERCH_PRODUCT_CATEGORY
,NULL MERCH_CATEGORY_ROLLUP
,NULL MERCH_SUB_CATEGORY_ROLLUP
,NULL PRIMARY_METAL_NAME
,NULL PRIMARY_METAL_COLOR
,NULL PRIMARY_METAL_PURITY
,NULL PRIMARY_STONE_CATEGORY
,NULL PRIMARY_STONE_TYPE
,IMAGE_URL  AS IMAGE_URL
,CATEGORY  AS FEED_CATEGORY
,NULL NAME_PLAINTEXT
,NULL DESCRIPTION_PLAINTEXT
,NULL SHORT_NAME_PLAINTEXT
,NULL ALTERNATE_IMAGE_URL
,NULL AS REPRESENTATIVE_SKU
from app_bluenile_v2.product_feed_base_v@nileprod_read
where is_product = 1
)
group by OFFER_ID) final
"""
#executing the query and loading the data to data frame in chunks
df1=pd.read_sql(query,connection,chunksize=1000000)
#Loading the data from  dataframe df1 to google bigquery staging table in chunks
for chunk in df1:
    df2=chunk
    df2.to_gbq(destination_table=o_stg_dataset+'.offer_attributes_dim',project_id=o_project,if_exists='append',chunksize=1000000)
#Check for the  stage stable if not create the stage table.
check_for_stag_table="""select 'TRUE' CHECK from `{0}.{1}.offer_attributes_dim` limit 1""".format(o_project, o_stg_dataset)
try:
    df= client.query(check_for_stag_table).to_dataframe()
    print("table already exists")
except:
    df3=pd.read_sql(query,connection)
    df3.to_gbq(destination_table=o_stg_dataset+'.offer_attributes_dim',project_id=o_project,if_exists='append',chunksize=1000000)