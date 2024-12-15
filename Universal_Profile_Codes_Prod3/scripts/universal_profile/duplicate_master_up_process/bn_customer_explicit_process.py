def bn_customer_explicit_process():
    bql = '''
	create or replace table `bnile-cdw-prod.dag_migration_test.bnid_pii_mapping_cleaned_dedup`
	as
	select 
	ifnull(map.primary_bnid,f.bnid) as bnid
	,f.event_type
	,f.email_address
	,f. first_name
	,f.last_name
	,f.gender
	,f.derived_gender
	,f.phone_number
	,f.country_code
	,f.extension
	,f.phone
	,f.mobile
	,f.last_4_digits
	,f.birthday
	,f.age
	,f.bill_to_street
	,f.bill_to_city
	,f.bill_to_state
	,f.bill_to_postal_code
	,f.bill_to_country_code 
	,f.bill_to_country_name
	,f.ship_to_street
	,f.ship_to_city
	,f.ship_to_state
	,f.ship_to_postal_code
	,f.ship_to_country_code 
	,f.ship_to_country_name 
	,f.first_name_is_valid
	,f.last_name_is_valid
	,f.email_is_valid
	,f.v_to_v_flag
	,f.event_date

	from 
	`bnile-cdw-prod.dag_migration_test.bnid_pii_mapping_cleaned` f
	left join
	(
	SELECT
	distinct primary_bnid,duplicate_bnid
	FROM
	`bnile-cdw-prod.dag_migration_test.bnid_dedup_fin`
	)map
	on f.bnid=map.duplicate_bnid;


	CREATE OR REPLACE TABLE `bnile-cdw-prod.dag_migration_test.stg_final_customer_bnid` as
	(
	select 
	 bnid
	,first_name
	,last_name
	,gender
	,derived_gender
	,birthday
	,age
	,phone_number
	,country_code
	,extension
	,phone
	,mobile
	,case when last_4_digits is not null  then LPAD(last_4_digits,4,'0') else null end last_4_digits
	,v_to_v_flag
	,bill_to_street
	,bill_to_city
	,bill_to_state
	,bill_to_postal_code
	,bill_to_country_name
	,bill_to_country_code
	,ship_to_street
	,ship_to_city
	,ship_to_state
	,ship_to_postal_code
	,ship_to_country_name
	,ship_to_country_code
	,first_name_is_valid
	,last_name_is_valid

	from

	(SELECT DISTINCT
			bnid,

			FIRST_VALUE(first_name) OVER(PARTITION BY bnid ORDER BY CASE WHEN first_name is NULL then 0 else 1 END DESC, first_name_priority asc, event_date desc,  first_name ) AS first_name,
			FIRST_VALUE(last_name) OVER(PARTITION BY bnid ORDER BY CASE WHEN last_name is NULL then 0 else 1 END DESC, last_name_priority asc, event_date desc,  last_name) AS last_name,
			FIRST_VALUE(birthday) OVER(PARTITION BY bnid ORDER BY CASE WHEN birthday is NULL then 0 else 1 END DESC, birth_date_priority asc, event_date desc,  birthday) AS birthday,
			FIRST_VALUE(age) OVER(PARTITION BY bnid ORDER BY CASE WHEN age is NULL then 0 else 1 END DESC, birth_date_priority asc, event_date desc,  age) AS age,
			FIRST_VALUE(phone_number) OVER(PARTITION BY bnid ORDER BY CASE WHEN phone_number is NULL then 0 else 1 END DESC, phone_number_priority asc, event_date desc,  phone_number) AS phone_number,

			FIRST_VALUE(country_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN country_code is NULL then 0 else 1 END DESC, phone_number_priority asc, event_date desc,  country_code) AS country_code,
			FIRST_VALUE(extension) OVER(PARTITION BY bnid ORDER BY CASE WHEN extension is NULL then 0 else 1 END DESC, phone_number_priority asc, event_date desc,  extension) AS extension,
			FIRST_VALUE(phone) OVER(PARTITION BY bnid ORDER BY CASE WHEN phone is NULL then 0 else 1 END DESC, phone_number_priority asc, event_date desc,  phone) AS phone,
			FIRST_VALUE(mobile) OVER(PARTITION BY bnid ORDER BY CASE WHEN mobile is NULL then 0 else 1 END DESC, phone_number_priority asc, event_date desc,  mobile) AS mobile,

			FIRST_VALUE(gender) OVER(PARTITION BY bnid ORDER BY CASE WHEN gender is NULL then 0 else 1 END DESC, gender_priority asc, event_date desc,  gender) AS gender,
			FIRST_VALUE(derived_gender) OVER(PARTITION BY bnid ORDER BY CASE WHEN derived_gender is NULL then 0 else 1 END DESC, gender_priority asc, event_date desc,  derived_gender) AS derived_gender,
			FIRST_VALUE(last_4_digits) OVER(PARTITION BY bnid ORDER BY CASE WHEN last_4_digits is NULL then 0 else 1 END DESC, Last_4_CC_priority asc, event_date desc,  last_4_digits) AS last_4_digits,

			FIRST_VALUE(bill_to_street) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_street is NULL then 0 else 1 END DESC, billing_address_priority asc, event_date desc,  bill_to_street) AS bill_to_street,
			FIRST_VALUE(bill_to_city) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_city is NULL then 0 else 1 END DESC, billing_address_priority asc, event_date desc,  bill_to_city) AS bill_to_city,
			FIRST_VALUE(bill_to_state) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_state is NULL then 0 else 1 END DESC, billing_address_priority asc, event_date desc,  bill_to_state) AS bill_to_state,
			FIRST_VALUE(bill_to_postal_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_postal_code is NULL then 0 else 1 END DESC, billing_address_priority asc, event_date desc,  bill_to_postal_code) AS bill_to_postal_code,
			FIRST_VALUE(bill_to_country_name) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_country_name is NULL then 0 else 1 END DESC, billing_address_priority asc, event_date desc,  bill_to_country_name) AS bill_to_country_name,
			FIRST_VALUE(bill_to_country_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_country_code is NULL then 0 else 1 END DESC, billing_address_priority asc, event_date desc,  bill_to_country_code) AS bill_to_country_code,


			FIRST_VALUE(ship_to_street) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_street is NULL then 0 else 1 END DESC, shipping_address_priority asc, event_date desc,  ship_to_street) AS ship_to_street,
			FIRST_VALUE(ship_to_city) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_city is NULL then 0 else 1 END DESC, shipping_address_priority asc, event_date desc,  ship_to_city) AS ship_to_city,
			FIRST_VALUE(ship_to_state) OVER(PARTITION BY bnid ORDER BY CASE	 WHEN ship_to_state is NULL then 0 else 1 END DESC, shipping_address_priority asc, event_date desc,  ship_to_state) AS ship_to_state,
			FIRST_VALUE(ship_to_postal_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_postal_code is NULL then 0 else 1 END DESC, shipping_address_priority asc, event_date desc,  ship_to_postal_code) AS ship_to_postal_code,
			FIRST_VALUE(ship_to_country_name) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_country_name is NULL then 0 else 1 END DESC, shipping_address_priority asc, event_date desc,  ship_to_country_name) AS ship_to_country_name,
			FIRST_VALUE(ship_to_country_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_country_code is NULL then 0 else 1 END DESC, shipping_address_priority asc, event_date desc,  ship_to_country_code) AS ship_to_country_code,


			FIRST_VALUE(v_to_v_flag) OVER(PARTITION BY bnid ORDER BY CASE WHEN v_to_v_flag is NULL then 0 else 1 END DESC, default_priority asc, event_date desc,  v_to_v_flag) AS v_to_v_flag,
			FIRST_VALUE(first_name_is_valid) OVER(PARTITION BY bnid ORDER BY CASE WHEN first_name_is_valid is NULL then 0 else 1 END DESC, first_name_priority asc, event_date desc,  first_name_is_valid) AS first_name_is_valid,
			FIRST_VALUE(last_name_is_valid) OVER(PARTITION BY bnid ORDER BY CASE WHEN last_name_is_valid is NULL then 0 else 1 END DESC, last_name_priority asc, event_date desc,  last_name_is_valid) AS last_name_is_valid

		from
	(SELECT 
    distinct 
    *,
    case 
    when phone_number is not null and event_type='freshdesk_contact' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='phone_number') 
    when phone_number is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='phone_number') 
    when phone_number is not null and event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='phone_number') 
    when phone_number is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='phone_number')
    when phone_number is not null and event_type='backbone' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='backbone' and pii_field='phone_number')
    else null
    end  phone_number_priority,

    case 
    when first_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='first_name') 
    when first_name is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='first_name') 
    when first_name is not null and event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='first_name') 
    when first_name is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='first_name') 
    when first_name is not null and event_type like '%freshdesk%' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='first_name')  else null

    end  first_name_priority,

    case 
    when last_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='last_name') 
    when last_name is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='last_name') 
    when last_name is not null and event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='last_name') 
    when last_name is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='last_name')
    when last_name is not null and event_type like '%freshdesk%' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='last_name')
    else null
    end  last_name_priority,

    case 
    when birthday is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='birth_date') 
    when birthday is not null and event_type='customer_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_event' and pii_field='birth_date') 
    when birthday is not null and event_type='email_survey' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='email_survey' and pii_field='birth_date') 
    when birthday is not null and event_type='backbone' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='backbone' and pii_field='birth_date') 
    else null
    end  birth_date_priority,

    case 
    when gender is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='gender') 
    when gender is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='gender')
    when gender is not null and event_type='freshdesk_contact' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='gender')
    when gender is not null and event_type='customer_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_event' and pii_field='gender')
    when gender is not null and event_type='email_survey' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='email_survey' and pii_field='gender')
    when gender is not null and event_type='backbone' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='backbone' and pii_field='gender')
    when gender is not null and event_type='review_pii' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='review_pii' and pii_field='gender')
    else null
    end  gender_priority,

    case 
    when Last_4_digits is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='Last_4_CC') 
    else null
    end  Last_4_CC_priority,

    case 

    when bill_to_street is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_city is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_state is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_postal_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address')
    when bill_to_country_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_country_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    else null
    end  billing_address_priority,

    case 

    when ship_to_street is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_street is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')


    when ship_to_city is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_city is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')

    when ship_to_state is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_state is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')

    when ship_to_country_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_country_code is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')
    when ship_to_country_code is not null and event_type='review_pii' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='review_pii' and pii_field='shipping_address')

    when ship_to_country_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_country_name is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')

    when ship_to_postal_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_postal_code is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')
    when ship_to_postal_code is not null and event_type='customer_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_event' and pii_field='shipping_address')

    else null
    end  shipping_address_priority,

    case 
    when event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='default') 
    when event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='default') 
    when event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer' and pii_field='default') 
    when  event_type like '%freshdesk%' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='default')
    when event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='default')
    when  event_type='click_fact' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='click_fact' and pii_field='default')
    when event_type='email_subscription_change_log' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='email_subscription_change_log' and pii_field='default')
    else null
    end  default_priority

	from `bnile-cdw-prod.dag_migration_test.bnid_pii_mapping_cleaned_dedup`  order by phone_number_priority,first_name_priority,last_name_priority,birth_date_priority,gender_priority,Last_4_CC_priority,
	billing_address_priority,shipping_address_priority,default_priority asc)aa));





	CREATE OR REPLACE TABLE  `bnile-cdw-prod.dag_migration_test.bn_customer_profile`
	as
	select * except(bnid_row)
	from  
	(
	 SELECT 
	 bnid
	,first_name
	,last_name
	,gender
	,derived_gender
	,birthday
	,age
	,phone_number
	,country_code
	,extension
	,phone
	,mobile
	,last_4_digits
	,v_to_v_flag
	,bill_to_street
	,bill_to_city
	,bill_to_state
	,bill_to_country_code
	,bill_to_country_name
	,bill_to_postal_code
	,case when 
    (bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') 
    or regexp_contains (bill_to_country_name,r"^[U]S.*" ) or bill_to_country_code in ('US','USA')) 
    and REGEXP_contains (f.bill_to_postal_code,r"^\d{5}(?:[-\s]\d{4})?$")
    and safe_cast(substr(f.bill_to_postal_code,0,5) as numeric) = safe_cast (billing.zip_code as numeric) then 'Y' else 'N' 
    end billing_postal_code_valid_flag
	,ship_to_street
	,ship_to_city
	,ship_to_state
	,ship_to_country_code 
	,ship_to_country_name 
	,ship_to_postal_code
	,case when (ship_to_country_name in 
	('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')  
	or regexp_contains (ship_to_country_name,r"^[U]S.*" ) or ship_to_country_code in ('US','USA')) 
	and REGEXP_contains (f.ship_to_postal_code,r"^\d{5}(?:[-\s]\d{4})?$")
	and safe_cast(substr(f.ship_to_postal_code,0,5) as numeric) = safe_cast (shipping.zip_code as numeric) then 'Y' else 'N' 
	end shipping_postal_code_valid_flag
	,first_name_is_valid
	,last_name_is_valid
	,row_number() over(partition by bnid) as bnid_row
	FROM `bnile-cdw-prod.dag_migration_test.stg_final_customer_bnid` f
	left join
	`bnile-cdw-prod.backup_tables.NBER_2016` billing
	on safe_cast (billing.zip_code as numeric) = safe_cast(substr(f.bill_to_postal_code,0,5) as numeric)
	left join
	`bnile-cdw-prod.backup_tables.NBER_2016` shipping
	on safe_cast (shipping.zip_code as numeric) = safe_cast(substr(f.ship_to_postal_code,0,5) as numeric)
	)
	where bnid_row=1;


	CREATE OR REPLACE TABLE  `bnile-cdw-prod.dag_migration_test.bn_explicit_attributes` 
	as

	select
	bnid
	,min(account_create_date) AS account_create_date
	,max(account_create_source) AS account_create_source
	,max(last_account_login_date) AS last_account_login_date
	,max(wish_list_flag) AS wish_list_flag
	,max(account_holder_flag) AS account_holder_flag
	,max(primary_dma_zip) AS primary_dma_zip
	,max(primary_dma_state) AS primary_dma_state
	,max(primary_dma_name) AS primary_dma_name
	,max(primary_dma_source) AS primary_dma_source
	,max(primary_site) AS primary_site
	,max(primary_hostname) AS primary_hostname
	,max(primary_currency_code) AS primary_currency_code
	,max(primary_language_code) AS primary_language_code
	,max(marital_status) AS marital_status
	,max(wedding_date) AS wedding_date
	,max(region) AS region
	,max(marital_status_source) AS marital_status_source
	,max(marital_status_timestamp) AS marital_status_timestamp
	,min(first_promo_subscribe_date) AS first_promo_subscribe_date
	,max(email_open_last_dt) AS email_open_last_dt
	,max(last_subscribe_date) AS last_subscribe_date
	,max(credit_card_status) AS credit_card_status
	,min(credit_card_create_date) AS credit_card_create_date
	,max(queens_english_flag) AS queens_english_flag
	,max(ip_time_zone) AS ip_time_zone
	,max(pending_order_flag) AS pending_order_flag
	,max(sweepstakes_entry_last_dt) AS sweepstakes_entry_last_dt
	,sum(order_cnt) AS order_cnt
	,sum(order_amt) AS order_amt
	,max(order_last_dt) AS order_last_dt
	,sum(order_engagement_cnt) AS order_engagement_cnt
	,sum(order_engagement_amt) AS order_engagement_amt
	,avg(order_engagement_avg_amt) AS order_engagement_avg_amt
	,max(order_engagement_last_dt) AS order_engagement_last_dt
	,min(order_engagement_first_dt) AS order_engagement_first_dt
	,sum(order_engagement_ring_cnt) AS order_engagement_ring_cnt
	,max(order_engagement_ring_last_dt) AS order_engagement_ring_last_dt
	,sum(order_jewelry_cnt) AS order_jewelry_cnt
	,sum(order_jewelry_amt) AS order_jewelry_amt
	,avg(order_jewelry_avg_amt) AS order_jewelry_avg_amt
	,max(order_jewelry_last_dt) AS order_jewelry_last_dt
	,max(order_wedding_band_last_dt) AS order_wedding_band_last_dt
	,sum(browse_cnt) AS browse_cnt
	,sum(browse_engagement_cnt) AS browse_engagement_cnt
	,max(browse_engagement_last_dt) AS browse_engagement_last_dt
	,min(browse_engagement_first_dt) AS browse_engagement_first_dt
	,sum(browse_jewelry_cnt) AS browse_jewelry_cnt
	,min(browse_jewelry_first_dt) AS browse_jewelry_first_dt
	,max(browse_jewelry_last_dt) AS browse_jewelry_last_dt
	,min(browse_first_dt) AS browse_first_dt
	,max(browse_last_dt) AS browse_last_dt
	,sum(browse_wedding_band_cnt) AS browse_wedding_band_cnt
	,max(browse_wedding_band_last_dt) AS browse_wedding_band_last_dt
	,min(browse_wedding_band_first_dt) AS browse_wedding_band_first_dt
	FROM `bnile-cdw-prod.dag_migration_test.up_explicit_attributes`
	group by 1'''
    return bql