def get_all_table_names_having_pii():
	bql='''
	declare a  int64 default 1;
	declare project string;
	declare dataset string;
	declare c string;
	declare d string;

	create or replace table `bnile-cdw-prod.CCPA_deletion.dataset_names`
	as
	(
	select catalog_name	as project_name, schema_name as dataset_name, row_number() over() as rank
	from
	(SELECT
	* EXCEPT(schema_owner)
	FROM
	`bnile-cdw-prod`.INFORMATION_SCHEMA.SCHEMATA));


	create or replace table   `bnile-cdw-prod.CCPA_deletion.ccpa_fields_to_be_updated`
	(table_names string, table_name string,column_name string,
	data_type string, ColumnNameInInput string,project string, dataset string ); 

	create or replace table   `bnile-cdw-prod`.CCPA_deletion.all_tables_1
	(table_names string, table_name string,column_name string,
	data_type string,project string, dataset string ); 

	create or replace table   `bnile-cdw-prod`.CCPA_deletion.table_type
	(table_name_original string, table_type string );  



	while a<= ( select count(*) from `bnile-cdw-prod.CCPA_deletion.dataset_names`)

	DO

	set project = (select project_name from `bnile-cdw-prod.CCPA_deletion.dataset_names` where rank = a);

	set dataset = ( select dataset_name from `bnile-cdw-prod.CCPA_deletion.dataset_names` where rank = a);

	set c = concat ('create or replace table `bnile-cdw-prod.CCPA_deletion.data1` as ' , 'select table_name,column_name,data_type, ',"'"
	, project,"'", ' as project', ',',"'" , dataset,"'", ' as dataset',' from ' ,'`',project,'`', '.',dataset, '.', 'INFORMATION_SCHEMA.COLUMNS') ;

	execute immediate c;

	set c = concat ('create or replace table `bnile-cdw-prod.CCPA_deletion.data2` as ' , 'select table_name,table_type, ',"'"
	, project,"'", ' as project', ',',"'" , dataset,"'", ' as dataset',' from ' ,'`',project,'`', '.',dataset, '.', 'INFORMATION_SCHEMA.TABLES') ;

	execute immediate c;

	insert into  `bnile-cdw-prod.CCPA_deletion.ccpa_fields_to_be_updated`
	select * from
	(SELECT
	concat ('`',project,'.',dataset, '.', table_name, '`' )  table_names,
	table_name,
	column_name,
	data_type,
	CASE
	when lower(column_name) like '%bnid%' then 'bnid'
	when lower(column_name) = 'name' and table_name like '%payment_fact%' then 'fullname' 
	when lower(column_name) = 'email_address_key' then 'email_address_key'
	when lower(column_name) like '%up_email_address_key%' then 'up_email_address_key'
	when lower(column_name) like '%shipment_tracking_id%' then 'shipment_tracking_id'
	when lower(column_name) like '%order_number%' then 'order_number'
	WHEN LOWER(column_name) like '%email%' THEN 'email'
	when column_name = 'primary_email' then 'email'
	when lower(column_name) = 'emailaddress' then 'email'
	when lower(column_name) = 'contact_id' then 'email'
	when lower(column_name) =  'login_name' then 'email'
	WHEN LOWER(column_name) LIKE '%first_name%' THEN 'first_name'
	WHEN LOWER(column_name) LIKE '%last_name%' THEN 'last_name'
	when lower(column_name) like 'middle_name' then 'middle_name'
	WHEN LOWER(column_name) LIKE '%phone_number%' THEN 'phone_number'
	WHEN LOWER(column_name) in ('bn_consignee_phone_number') then 'phone_number'
	WHEN LOWER(column_name) LIKE '%customer_guid%' THEN 'guid'
	WHEN LOWER(column_name) = 'md_customer_ip' then 'ip_address'
	when lower(column_name) like '%ip_address%' then 'ip_address'
	when lower(column_name) like '%mobile%' then 'phone_number'
	when lower(column_name) like '%phone%' then 'phone_number'
	when lower(column_name) =  'full_name' then 'fullname'
	when lower(column_name) like '%guid%' THEN 'guid'
	when lower(column_name) = 'customer_name' then 'fullname'
	when lower(column_name) = 'customer_location_state' then 'state'
	when lower(column_name) = 'customer_location_country' then 'country'
	when lower(column_name) like '%customer%' THEN 'customer_id'
	when lower(column_name) like 'customer_account_id' then  'customer_id'
	when lower(column_name) in ('fe_recipient_name', 'bn_consignee_name','contact_name','customer_name','reviewer_display_name') then 'fullname'
	when lower(column_name) = 'fax_number' then 'fax_number'
	when lower(column_name) = 'initial_first_name' then 'first_name'
	when lower(column_name) = 'initial_customer_account_key' then 'customer_id'
	when lower(column_name) in ('city', 'bill_to_city','ship_to_city','fe_recipient_city','bn_consignee_city','mailing_addr_city') then 'city'
	when lower(column_name) in ('country', 'country_name', 'md_customer_country','bn_consignee_country','bill_to_country_name',
	'mailing_addr_country','customer_location_country','reviewer_country','ship_to_country_name','ship_to_country') then 'country'
	when lower(column_name) in ('mailing_addr_street_1','street_address_1','bill_to_street_address_1','ship_to_street_address_1',
	'fe_recipient_address_line_1','bn_consignee_address_line_1') then 'street_address_1'
	when lower(column_name) in ('mailing_addr_street_2','street_address_2','bill_to_street_address_2','ship_to_street_address_2','fe_recipient_address_line_2',
	'bn_consignee_address_line_2') then 'street_address_2'
	when lower(column_name) 
	in ('mailing_addr_street_3', 'mailing_addr_street_4','street_address_3','bill_to_street_address_3','ship_to_street_address_3' ) then 'street_address_3'
	when lower(column_name) in ( 'postal_code', 'primary_dma_zip','bill_to_postal_code','fe_recipient_zip_code' ,'mailing_addr_zip','bn_consignee_zip_code','ship_to_postal_code') then 'postal_code'
	when 
	lower(column_name) in 
	( 'primary_dma_state','state','bill_to_state','ship_to_state','fe_recipient_state','bn_consignee_state','mailing_addr_state','customer_location_state') then 'state'
	when lower(column_name) in ( 'primary_ship_to_country_code','fe_recipient_country_code','bill_to_country_code','ship_to_country_code') then 'country_code'
	when lower(column_name) in ('bill_to_street','bill_to_street_address','ship_to_street') then 'street_address'
	when lower(column_name) = 'country_of_origin' then 'country_of_origin'
	when lower(column_name)  = 'region' then 'region'
	when lower(column_name) like '%basket_id%' then 'basket_id'


	ELSE
	NULL
	END
	ColumnNameInInput,
	project,
	dataset,
	FROM
	`bnile-cdw-prod.CCPA_deletion.data1`
	WHERE
	table_name<>'CCPA_GDPR_Input'
	and table_name <> 'CCPA_GDPR_INPUT'
	and table_name <> 'Dev_CCPA_GDPR_TableNames'
	and table_name <> 'map_name_gender'
	and table_name not in  ('airflow_run_log_s', 'vendor_v', 'user_location_performance_daily_report','destination')
	AND data_type<>'TIMESTAMP'
	and data_type<>'BOOL'
	and table_name not in ('mg_aud_200off600_ecl', 'mg_aud_20200831_fall_sale', 'mg_aud_20200902_fall_sale_sup1', 'mg_aud_sept_birthstone_ecl', 'mg_aud_band_discount_final', 'mg_aud_PPA_showroom_awareness', 'mg_aud_sept_birthstone', 'mg_aud_20200831_fall_sale', 'mg_aud_1010_launch_automated'
	,'mg_aud_oct_birthstone', 'mg_aud_light_box', 'mg_aud_band_discount', 'mg_aud_1010_launch_backfill', 'mg_aud_window_shopping_t4', 'mg_aud_ongoing_nps',
	'mg_aud_20200922_jewelry_survey', 'mg_aud_back_in_stock', 'mg_aud_20201005_ring_event', 'mg_aud_credit_card_activation', 'mg_aud_promo_no_lc', 
	'mg_aud_promo_lc_low_intent', 'mg_aud_standard', 'mg_aud_refer_ER', 'WB_Relationship_Journey_Default', 'mg_aud_bounce_test', 'mg_aud_promo_us_only',
	'mg_aud_mens_reco', 'mg_aud_wb_reco_180days', 'mg_aud_promo_no_male_lc', 'mg_aud_promo_site_not_null','mg_aud_nov_birthstone','mg_aud_promo_lc_alone', 'mg_aud_band_discount_reminder','ER_Shipped_190days', 'mg_aud_20200902_fall_sale_sup1','mg_aud_20201103_LB_ed', 'mg_aud_promo_rec_no_eng_ring','mg_aud_promo_lc_us_only','mg_aud_promo_std','mg_aud_thankyou_nonER','mg_aud_exo_20201209', 'mg_aud_apology_20201119', 
	'mg_aud_window_shopping_t3','mg_aud_lapsed','mg_aud_promo_std_3mo','mg_aud_refer_non_ER','mg_aud_promo_rec_no_eng_rings_no_wbs','mg_aud_std_noconv',
	'mg_aud_ring_sizer','mg_aud_thankyou_ER','mg_aud_dec_birthstone', 'mg_aud_mens_reco','mg_aud_refer_ER', 'country', 'customer_event_fact',
	'dk_webroom', 'webroom', 'mg_aud_promo_std_6mo', 'promo_20200817_200off600','mg_aud_standard_with_holdout', 'airflow_task_logs_raw2','mg_aud_lightbox_withtraffic','mg_aud_jan_birthstone'
	,'airflow_task_logs_raw','dag_run','logs_1','dag_run_1','airflow_logs', 'country', 'dk_country', 'ad_performance_details', 'ad_performance_fact'
	,'ad_performance_fact_old', 'mg_aud_jan_birthstone','placement_dim','dssprod_deepthim_placement_dim','dk_dssprod_o_warehouse_product_dim','ship_to_country_language'
	,'daily_product_fact','diamond','daily_product_fact_backup_dssprod_o_warehouse_dec2','dk_product_dim','diamond_snapshot','product_dim'
	,'diamond_snap_snippet','diamond_attributes_dim','dssprod_o_diamond_diamond_snapshot','dssprod_o_product_diamond','dssprod_o_warehouse_placement_dim' ,'dssprod_o_warehouse_product_dim','diamond_attributes_dim','diamond_snapshot','daily_product_fact','daily_product_fact_2017','daily_product_fact_backfill2018_20200922','dssprod_deepthim_placement_dim','bn_rfm','offer_dim','glob_name_gender','offer_attributes_dim')
	and lower(table_name)  not like '%offer_dim%' and lower(table_name) not like '%offer_combination%'
	and table_name not in ('dk_webroom_ip_address','ip_address_dim','dssprod_o_warehouse_ip_address_dim','webroom_ip_address')
	and 
	( lower(column_name) IN ('ip_address')
	OR LOWER(column_name) LIKE '%first_name%'
	OR LOWER(column_name) LIKE '%last_name%'
	OR LOWER(column_name) LIKE '%phone_number%'
	OR LOWER(column_name) like '%email_addre%'
	OR LOWER(column_name) like '%guid%'
	OR LOWER(column_name) LIKE '%customer_guid%'
	or LOWER(column_name) LIKE '%ip_address%'
	or Lower(Column_name) like '%mobile%'
	or lower(column_name) like '%phone%'
	or lower(column_name) like '%street%'
	or lower(column_name) =  'email'
	or lower(column_name) like '%bnid%'
	or lower(column_name) = 'name' 
	or lower(column_name) = 'full_name'
	or  lower(column_name) like '%shipment_tracking_id%' 
	or lower(column_name) like '%order_number%' 
	or lower(column_name) like '%up_email_address_key%' 
	or lower (column_name) in ('customer_account_key', 'customer_id', 'initial_customer_account_key', 'fax_number', 'customer_account_id')
	or lower(column_name) in 
	( 'primary_dma_zip' , 'primary_dma_state' , 'city', 'state', 'postal_code', 'country', 'country_name', 'mailing_addr_zip','mailing_addr_state',
	'mailing_addr_city','primary_ship_to_country_code','fe_recipient_country_code','fe_recipient_state','bn_consignee_state','fe_recipient_zip_code',
	'bn_consignee_zip_code','mailing_addr_country','reviewer_display_name','reviewer_country')
	or column_name = 'primary_email'
	or lower(column_name) = 'emailaddress'
	or lower(column_name) = 'region'
	or lower(column_name) like '%basket_id%'
	or lower(column_name) like '%bill_to%'
	or  lower(column_name) like '%ship_to%'
	or lower(column_name) in ( 'contact_id','country_of_origin','login_name','middle_name','md_customer_ip','fe_recipient_name','customer_name',
	'bn_consignee_name','fe_recipient_city','mailing_addr_street_1','street_address_1','bill_to_street_address_1','ship_to_street_address_1',
	'fe_recipient_address_line_1','bn_consignee_address_line_1','mailing_addr_street_2','street_address_2','bill_to_street_address_2','ship_to_street_address_2','fe_recipient_address_line_2','bill_to_country_name','bill_to_country_code','contact_name','customer_location_state','customer_location_country',
	'bn_consignee_address_line_2','mailing_addr_street_3', 'mailing_addr_street_4','street_address_3','bill_to_street_address_3','ship_to_street_address_3' ) )
	and column_name <> 'guid_key' and column_name <> 'CREATED_BY_USERNAME' 
	and column_name <>  'CHANGED_BY_USERNAME'  and lower(column_name) not like '%d_key%'
	and lower(column_name) not like 'number_of_%' and (column_name)  <> 'EMAIL_DOMAIN_NAME' and
	column_name <> 'EMAIL_TRACKING_QUERYSTRING' and column_name <> 'EMAIL_VERSION' and column_name <> 'MOBILE_FLAG'
	and column_name <> 'GUID_CAPTURE_CHECK' and lower(column_name) not like '%flag%'  
	and lower(column_name) not like '%source%' and column_name <> 'GOOGLE_EMAIL_ADDRESS_HASH' 
	--and lower(column_name) <> 'email_address_key'
	and lower(column_name) <> 'email_address_test_key' and 
	lower(column_name) not in ('email_guid_count','guid_email_count', 'customer_desktop_or_mobile','first_name_gender', 'first_email_address_date','first_name_timestamp', 'guid_capture_key', 'unique_mobile_email_open_count' , 'unique_mobile_email_click_count', 'same_last_name', 'same_first_name','bnid_per_guid_cnt','guid_per_bnid_cnt', 'first_name_is_valid','last_name_is_valid','initial_ship_to_addr_key','fe_invoice_bill_to_account', 'ship_to_address_key', 'bill_to_address_key', 'ship_to_country_key', 
	'force_ship_to','ship_to_harmonized_code','bill_to_address_key', 'bill_to_country_key', 'initial_bill_to_addr_key', 'ship_to_address_key', 'ship_to_address_id',
	'google_order_number','fe_orig_purchase_order_number','bnid_row','bnids','duplicate_bnid','preferred_phone','bn_hold_phone_number')
	and lower(column_name) not in ('phone_impressions','mobile_link', 'phone_calls' , 'count_bnid')
	and lower(column_name) not like 'total%' and lower(column_name) not like '%url'
	) where ColumnNameInInput is not null
	ORDER BY 1 ;

	insert into `bnile-cdw-prod`.CCPA_deletion.all_tables_1

	SELECT
	concat ('`',project,'.',dataset, '.', table_name, '`' )  table_names,
	table_name,
	column_name,
	data_type,
	project,
	dataset,
	FROM
	`bnile-cdw-prod.CCPA_deletion.data1`;


	insert into  `bnile-cdw-prod.CCPA_deletion.table_type`
	SELECT
	concat ('`',project,'.',dataset, '.', table_name, '`' )  table_names,
	table_type
	FROM
	`bnile-cdw-prod.CCPA_deletion.data2`;

	set a = a+1;
	end while;

	create or replace table `bnile-cdw-prod.CCPA_deletion.ccpa_fields`
	as
	( SELECT a.* FROM `bnile-cdw-prod.CCPA_deletion.ccpa_fields_to_be_updated` a
	join
	`bnile-cdw-prod.CCPA_deletion.table_type` b 
	on a.table_names =  b.table_name_original 
	where table_type  = 'BASE TABLE');



	create or replace table `bnile-cdw-prod.CCPA_deletion.new_fields_created` as (
	select *  from `bnile-cdw-prod`.CCPA_deletion.all_tables_1 where table_names  not in ( select distinct table_names from
	`bnile-cdw-prod`.CCPA_deletion.master_data_set) ) ;'''
	return bql