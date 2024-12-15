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
max_date = """SELECT coalesce(max(timestamp(sku_create_date)),'1775-04-04 00:00:00 UTC') FROM `{0}.{1}.diamond_attributes_dim`""".format(o_project, o_diamond_dataset)
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
trim(REGEXP_REPLACE(sku,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS sku
,trim(REGEXP_REPLACE(shape,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS shape
,trim(REGEXP_REPLACE(carat,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS carat
,trim(REGEXP_REPLACE(cut,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cut
,trim(REGEXP_REPLACE(color,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS color
,trim(REGEXP_REPLACE(clarity,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS clarity
,trim(REGEXP_REPLACE(symmetry,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS symmetry
,trim(REGEXP_REPLACE(polish,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS polish
,trim(REGEXP_REPLACE(fluorescence,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fluorescence
,trim(REGEXP_REPLACE(brand,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS brand
,trim(REGEXP_REPLACE(depth_percent,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS depth_percent
,trim(REGEXP_REPLACE(table_percent,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS table_percent
,trim(REGEXP_REPLACE(length,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS length
,trim(REGEXP_REPLACE(width,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS width
,trim(REGEXP_REPLACE(height,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS height
,trim(REGEXP_REPLACE(girdle_size_min,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS girdle_size_min
,trim(REGEXP_REPLACE(girdle_size_max,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS girdle_size_max
,trim(REGEXP_REPLACE(girdle_facet,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS girdle_facet
,trim(REGEXP_REPLACE(culet,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS culet
,trim(REGEXP_REPLACE(crown,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS crown
,trim(REGEXP_REPLACE(pavillion,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS pavillion
,trim(REGEXP_REPLACE(milky,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS milky
,trim(REGEXP_REPLACE(eye_clean,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS eye_clean
,trim(REGEXP_REPLACE(crown_angle,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS crown_angle
,trim(REGEXP_REPLACE(crown_height,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS crown_height
,trim(REGEXP_REPLACE(pavillion_angle,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS pavillion_angle
,trim(REGEXP_REPLACE(pavillion_depth,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS pavillion_depth
,trim(REGEXP_REPLACE(star_length,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS star_length
,trim(REGEXP_REPLACE(inscription,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS inscription
,trim(REGEXP_REPLACE(lower_half,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS lower_half
,trim(REGEXP_REPLACE(rough_origin,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS rough_origin
,trim(REGEXP_REPLACE(country_of_origin,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS country_of_origin
,trim(REGEXP_REPLACE(country_of_mfg,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS country_of_mfg
,trim(REGEXP_REPLACE(is_canada_mark,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_canada_mark
,trim(REGEXP_REPLACE(is_rio_tinto,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_rio_tinto
,trim(REGEXP_REPLACE(promised_basket_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS promised_basket_id
,trim(REGEXP_REPLACE(ships_in_days,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS ships_in_days
,trim(REGEXP_REPLACE(oracle_apps_status,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS oracle_apps_status
,trim(REGEXP_REPLACE(display,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS display
,trim(REGEXP_REPLACE(status,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS status
,trim(REGEXP_REPLACE(owned_by_vendor_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS owned_by_vendor_id
,trim(REGEXP_REPLACE(purchased_from_vendor_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS purchased_from_vendor_id
,trim(REGEXP_REPLACE(memo_to_vendor_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS memo_to_vendor_id
,coalesce(purchase_date,to_date('1775-04-04 00:00:00','YYYY-MM-DD HH24:MI:SS'))  AS purchase_date
,trim(REGEXP_REPLACE(vendor_sku,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS vendor_sku
,trim(REGEXP_REPLACE(vendor_on_memo,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS vendor_on_memo
,trim(REGEXP_REPLACE(vendor_location_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS vendor_location_id
,trim(REGEXP_REPLACE(imp_log_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS imp_log_id
,trim(REGEXP_REPLACE(pending_import_log_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS pending_import_log_id
,trim(REGEXP_REPLACE(comment_text,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS comment_text
,trim(REGEXP_REPLACE(internal_vendor_notes,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS internal_vendor_notes
,trim(REGEXP_REPLACE(fancy_color_code,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_code
,trim(REGEXP_REPLACE(fancy_color_description,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_description
,trim(REGEXP_REPLACE(fancy_base_color,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_base_color
,trim(REGEXP_REPLACE(fancy_color_origin,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_origin
,trim(REGEXP_REPLACE(fancy_color_intensity,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_intensity
,trim(REGEXP_REPLACE(fancy_color_overtone1,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_overtone1
,trim(REGEXP_REPLACE(fancy_color_overtone2,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_overtone2
,trim(REGEXP_REPLACE(fancy_color_overtone3,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_overtone3
,trim(REGEXP_REPLACE(fancy_color_distribution,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS fancy_color_distribution
,trim(REGEXP_REPLACE(merch_sku,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS merch_sku
,trim(REGEXP_REPLACE(merch_category_code,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS merch_category_code
,trim(REGEXP_REPLACE(back_of_rap_percent,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS back_of_rap_percent
,trim(REGEXP_REPLACE(merch_pricing_quartile,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS merch_pricing_quartile
,trim(REGEXP_REPLACE(is_marked_delivered,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS is_marked_delivered
,trim(REGEXP_REPLACE(cert_id,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_id
,trim(REGEXP_REPLACE(cert_lab,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_lab
,trim(REGEXP_REPLACE(cert_file,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_file
,trim(REGEXP_REPLACE(cert_file_zoom,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_file_zoom
,trim(REGEXP_REPLACE(cert_file_zoom_2,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_file_zoom_2
,trim(REGEXP_REPLACE(cert_cut,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_cut
,trim(REGEXP_REPLACE(cert_file_raw,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_file_raw
,trim(REGEXP_REPLACE(cert_id_2,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_id_2
,trim(REGEXP_REPLACE(cert_lab_2,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_lab_2
,trim(REGEXP_REPLACE(cert_file_2,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_file_2
,trim(REGEXP_REPLACE(cert_file_raw_2,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS cert_file_raw_2
,coalesce(create_date,to_date('1775-04-04 00:00:00','YYYY-MM-DD HH24:MI:SS'))  AS sku_create_date
,trim(REGEXP_REPLACE(image_url,'([[:space:]]{2,}|[[:cntrl:]])', ' ')) AS alternate_image_url
,null AS image_url
FROM O_PRODUCT.DIAMOND@nileprod_read WHERE change_date>to_date(:date_1,'YYYY-MM-DD')
"""
#executing the query and loading the data to data frame in chunks
df1=pd.read_sql(query,connection,params=[date_1],chunksize=1000000)
#Loading the data from  dataframe df1 to google bigquery staging table in chunks
for chunk in df1:
    df2=chunk
    df2.to_gbq(destination_table=o_stg_dataset+'.diamond_attributes_dim',project_id=o_project,if_exists='append',chunksize=1000000)
#Check for the  stage stable if not create the stage table.
check_for_stag_table="""select 'TRUE' CHECK from `{0}.{1}.diamond_attributes_dim` limit 1""".format(o_project, o_stg_dataset)
try:
    df= client.query(check_for_stag_table).to_dataframe()
    print("table already exists")
except:
    df3=pd.read_sql(query,connection,params=[date_1])
    df3.to_gbq(destination_table=o_stg_dataset+'.diamond_attributes_dim',project_id=o_project,if_exists='append',chunksize=1000000)