def stage_pii_sql_stmts():
    bql = '''

	CREATE OR REPLACE TABLE
	`bnile-cdw-prod.up_stg_customer.stg_customer_event_fact` AS (
	SELECT
		DISTINCT 'customer_event' AS envent_type,
		parse_DATE('%Y%m%d',
		CAST(date_key AS string)) AS event_date,
		cef.email_address_key,
		ead.email_address,
		CASE
		WHEN TRIM(gender)='0' THEN 'F'
		WHEN TRIM(gender)='1' THEN 'M'
		ELSE
		gender
	END
		AS gender,
		CASE
		WHEN safe.parse_DATE('%d/%m/%Y', birthday ) IS NULL THEN safe.parse_DATE('%m/%d/%Y', birthday )
		ELSE
		safe.parse_DATE('%d/%m/%Y',
		birthday )
	END
		AS birthday,
		CAST(wedding_date AS date) AS wedding_date,
		age_range,
		postal_code,
		CASE
		WHEN TRIM(marital_status)='single' THEN 'Single'
		WHEN TRIM(marital_status)='married' THEN 'Married'
		WHEN TRIM(marital_status)='N/A' THEN NULL
		ELSE
		marital_status
	END
		AS marital_status,
		wedding_anniversary
	FROM
		`bnile-cdw-prod.dssprod_o_warehouse.customer_event_fact` cef
	LEFT JOIN
		`bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead
	ON
		ead.email_address_key=cef.email_address_key
	WHERE
		cef.email_address_key IS NOT NULL);
	
	
	
	CREATE OR REPLACE TABLE
	`bnile-cdw-prod.up_stg_customer.stg_email_survey_summary` AS
	SELECT
	* EXCEPT (wedding_date),
	CAST (SPLIT(CAST (wedding_date AS string), '-') [
	OFFSET
		(0)] AS numeric)AS year,
	CASE
		WHEN REGEXP_CONTAINS(CAST (wedding_date AS string), r'\d{4}-\d{2}-\d{2}') AND CAST (SPLIT(CAST (wedding_date AS string), '-') [ OFFSET (0)] AS int64) <25 THEN safe_cast (concat ('20', CAST (SPLIT(CAST (wedding_date AS string), '-') [ OFFSET (0)] AS numeric), '-', SPLIT(CAST (wedding_date AS string), '-') [ OFFSET (1)], '-', SPLIT(CAST (wedding_date AS string), '-') [ OFFSET (2)]) AS date)
		WHEN REGEXP_CONTAINS(CAST (wedding_date AS string), r'\d{4}-\d{2}-\d{2}')
	AND CAST (SPLIT(CAST (wedding_date AS string), '-') [
	OFFSET
		(0)] AS int64) <1900
	AND CAST (SPLIT(CAST (wedding_date AS string), '-') [
	OFFSET
		(0)] AS int64) >25 THEN safe_cast (concat ('19',
		CAST (SPLIT(CAST (wedding_date AS string), '-') [
		OFFSET
			(0)] AS numeric),
		'-',
		SPLIT(CAST (wedding_date AS string), '-') [
		OFFSET
		(1)],
		'-',
		SPLIT(CAST (wedding_date AS string), '-') [
		OFFSET
		(2)]) AS date)
	ELSE
	wedding_date
	END
	wedding_date
	FROM (
	SELECT
		'email_survey' AS envent_type,
		parse_DATE('%Y%m%d',
		CAST(SURVEY_SEND_DATE_KEY AS string)) AS event_date,
		ess.email_address_key,
		ead.email_address,
		basket_id,
		CASE
		WHEN TRIM(gender)='0' THEN 'F'
		WHEN TRIM(gender)='1' THEN 'M'
		WHEN TRIM(gender)='female' THEN 'F'
		WHEN TRIM(gender)='male' THEN 'M'
		ELSE
		gender
	END
		AS gender,
		CASE
		WHEN safe.parse_DATE('%m/%d/%Y', birth_date ) IS NULL THEN safe.parse_DATE('%d/%m/%Y', birth_date )
		ELSE
		safe.parse_DATE('%m/%d/%Y',
		birth_date )
	END
		AS birth_date,
		--birth_date,
	IF
		(
		IF
		(safe.parse_DATE('%m/%d/%Y',
			TRIM(wedding_date) )IS NULL,
			safe.parse_DATE('%d/%m/%Y',
			TRIM(wedding_date)),
			safe.parse_DATE('%m/%d/%Y',
			TRIM(wedding_date)))
		--as bd
		IS NULL,
		safe.parse_DATE('%Y-%m-%d',
			TRIM(wedding_date)),
		safe.parse_DATE('%m/%d/%Y',
			TRIM(wedding_date))) AS wedding_date,
		CASE
		WHEN TRIM(marital_status)='single' THEN 'Single'
		WHEN TRIM(marital_status)='married' THEN 'Married'
		WHEN TRIM(marital_status)='N/A' THEN NULL
		ELSE
		marital_status
	END
		AS marital_status
	FROM
		`bnile-cdw-prod.dssprod_o_warehouse.email_survey_summary` ess
	LEFT JOIN
		`bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead
	ON
		ead.email_address_key=ess.email_address_key
	WHERE
		ess.email_address_key IS NOT NULL ); 
	
	
	
	CREATE OR REPLACE TABLE
	`bnile-cdw-prod.up_stg_customer.stg_backbone` AS (
	SELECT
		DISTINCT 'backbone' AS event_type,
		bb.created_date AS event_date,
		nullif(aa.bnid,
		'') bnid,
		bb.email_address,
		bb.phone_number,
		nullif(CAST(age AS string),
		'') age,
		nullif(age_range,
		'') age_range,
		nullif(dob,
		'') dob,
		CASE
		WHEN REGEXP_CONTAINS(TRIM(nullif(dob,'')), r'\d{4}-\d{2}') THEN concat (TRIM(nullif(dob,'')), '-15')
		ELSE
		CAST(TRIM(nullif(dob,'')) AS string)
	END
		birth_date,
		nullif(gender,
		'') gender,
		nullif(marital_status,
		'') marital_status,
	FROM
		`bnile-cdw-prod.bnile_gale_backbone.bnid_backbone_matches_features` aa
	LEFT JOIN
		`bnile-cdw-prod.up_stg_customer.master_bn_table` bb
	ON
		aa.bnid=bb.bnid
		where  safe_cast (age AS int64) >18);
	
	
	
	CREATE OR REPLACE TABLE
	`bnile-cdw-prod.up_stg_customer.stg_review_pii` AS (
	SELECT
		'review_pii' AS event_type,
		DATE(review_date) AS event_date,
		reviewer_email_address,
		reviewer_country,
		gender,
		marital_status,
		anniversary_month,
		wedding_month,
		wedding_year
	FROM
		`bnile-data-eng-dev.o_customer.reviews_pii`);
	'''
    return bql