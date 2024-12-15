def bn_up_dashboard_codes():
    bql = '''
	---- query for bnid_scorecard------
	
    CREATE TABLE IF NOT EXISTS  `bnile-cdw-prod.o_customer.bnid_scorecard`
	( 
	Subscription	STRING,
	Activity	STRING,
	BNIDs	int64,
	emails	int64,
	existing_customer	int64,
	run_date	DATE
	);
	
	insert into `bnile-cdw-prod.o_customer.bnid_scorecard`
	
	select * from(
	SELECT
	CASE WHEN COALESCE (b.up_email_address_key, e.up_email_address_key) is NOT NULL AND subscribe_flag = 'Y' THEN 'MER'
	WHEN COALESCE (b.up_email_address_key, e.up_email_address_key) is NOT NULL THEN 'CER'
	ELSE 'Unknown' END as Subscription,
	CASE WHEN DATE_DIFF (current_date() ,COALESCE( email_open_last_dt, browse_last_dt),MONTH) <= 6 THEN 'Active'
	WHEN DATE_DIFF (current_date() ,COALESCE( email_open_last_dt, browse_last_dt),MONTH) > 6 AND
	DATE_DIFF (current_date() ,COALESCE( email_open_last_dt, browse_last_dt),MONTH) <= 24 THEN 'Lapse'
	ELSE 'Dormant' END as Activity,
	COUNT(distinct COALESCE (b.BNID, e.bnid)) as BNIDs,
	SUM(CASE WHEN COALESCE (b.up_email_address_key, e.up_email_address_key) is not null then 1 ELSE 0 END) as emails,
	SUM(CASE WHEN order_cnt > 0 then 1 ELSE 0 END) as existing_customer, current_date() as run_date
	FROM `bnile-cdw-prod.o_customer.up_explicit_attributes` e
	RIGHT JOIN `bnile-cdw-prod.o_customer.up_customer_attributes` as b ON b.BNID = e.bnid
	WHERE email_is_valid = 'true'
	GROUP BY
	CASE WHEN COALESCE (b.up_email_address_key, e.up_email_address_key) is NOT NULL AND subscribe_flag = 'Y' THEN 'MER'
	WHEN COALESCE (b.up_email_address_key, e.up_email_address_key) is NOT NULL THEN 'CER'
	ELSE 'Unknown' END ,
	CASE WHEN DATE_DIFF (current_date() ,COALESCE( email_open_last_dt, browse_last_dt),MONTH) <= 6 THEN 'Active'
	WHEN DATE_DIFF (current_date() ,COALESCE( email_open_last_dt, browse_last_dt),MONTH) > 6 AND
	DATE_DIFF (current_date() ,COALESCE( email_open_last_dt, browse_last_dt),MONTH) <= 24 THEN 'Lapse'
	ELSE 'Dormant' END)aa
	where aa.run_date not in(select distinct run_date from `bnile-cdw-prod.o_customer.bnid_scorecard`);
	
	--- query for pj_progression-------------
	
	CREATE TABLE IF NOT EXISTS  `bnile-cdw-prod.up_stg_customer.pj_progression`
	( bnid	STRING
	,propensity_phase STRING
	,bucket string
	,modified_bucket STRING
	,run_date Date );
	
	insert into  `bnile-cdw-prod.up_stg_customer.pj_progression`
	
	with modified_bucket as
	(SELECT bnid, bucket, 
	Case when bucket = 'LC' then 'LC' else 'SC' end as modified_bucket,
	run_date
	FROM `bnile-cdw-prod.up_stg_customer.history_bn_pj_tagging`
	where run_date =  current_date()
	),
	
	union_all_propensity as
	
	( select bnid, 'LC' as bucket, propensity_phase, model_run_date  from `bnile-cdw-prod.o_customer.bn_lc_propensity`
	where model_run_date =  current_date()
	union all
	select bnid, 'SC' as bucket, propensity_phase, model_run_date  from `bnile-cdw-prod.o_customer.bn_sc_propensity`
	where model_run_date =  current_date()
	)
	
	select distinct mb.bnid ,unp.propensity_phase, mb.bucket, mb.modified_bucket, unp.model_run_date as run_date from modified_bucket mb
	join
	union_all_propensity unp
	on mb.bnid = unp.bnid 
	and mb.run_date = unp.model_run_date 
	and mb.modified_bucket = unp.bucket
	where unp.model_run_date  not in ( select distinct run_date from `bnile-cdw-prod.up_stg_customer.pj_progression` );'''
    return bql