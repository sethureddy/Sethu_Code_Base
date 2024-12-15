def user_activity(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	customer_data = params["customer_data"]
	dssprod_data = params["dssprod_data"]
	o_warehouse = params["o_warehouse"]
	run_date = params["run_date"]
	
	bql = f"""with email_subscription as 
	(
	SELECT distinct bnid FROM `{project_name}.{dssprod_data}.email_address_subscription` eas
	left join
	(SELECT bnid,email_address FROM `{project_name}.{stg_data}.bn_email_guid`
	group by 1,2) beg
	on eas.email_address = beg.email_address
	where subscribe_flag = 'Y' and bnid is not null
	)
	, click_fact  as (
	SELECT distinct guid_key,cf.date_key, DISPLAY_TIME_24_HOUR_FORMAT, hour, day_name, year, month_name , event_seq, landing_page_flag
	FROM (Select distinct guid_key, date_key, time_key, event_seq, landing_page_flag 
	from `{project_name}.{dssprod_data}.click_fact`
	) cf 
	left join `{project_name}.{dssprod_data}.time_dim` td
	on cf.time_key=td.time_key
	left join (Select distinct date_key, day_name, month_name, year from `{project_name}.{o_warehouse}.date_dim`)dd
	on cf.date_key=dd.date_key
	)
	, browse as 
	(
	Select distinct bnid, cf.guid_key, date_key,DISPLAY_TIME_24_HOUR_FORMAT, event_seq, landing_page_flag, year, month_name,day_name
	From click_fact cf
	left join (Select distinct guid_key, guid from `{project_name}.{dssprod_data}.guid_dim`) gd
	on cf.guid_key = gd.guid_key
	left join (Select distinct bnid,guid from `{project_name}.{stg_data}.bn_guid_all`) bg
	on gd.guid= bg.guid
	where bnid in (select distinct bnid from email_subscription) and date_key >= {run_date}
	)
	
	,browse_counts as (
	Select  bnid, year, month_name, day_name,
		  sum ( case when DISPLAY_TIME_24_HOUR_FORMAT    between '08:00:00' and '09:59:59' then 1 else 0 end) as Browse_count_class_1,
		  sum ( case when DISPLAY_TIME_24_HOUR_FORMAT    between '10:00:00' and '11:59:59' then 1 else 0 end) as Browse_count_class_2,
		  sum ( case when DISPLAY_TIME_24_HOUR_FORMAT    between '12:00:00' and '13:59:59' then 1 else 0 end) as Browse_count_class_3,
		  sum ( case when DISPLAY_TIME_24_HOUR_FORMAT    between '14:00:00' and '15:59:59' then 1 else 0 end) as Browse_count_class_4,
		  sum ( case when DISPLAY_TIME_24_HOUR_FORMAT    between '16:00:00' and '17:59:59' then 1 else 0 end) as Browse_count_class_5,
		  sum ( case when DISPLAY_TIME_24_HOUR_FORMAT    between '18:00:00' and '19:59:59' then 1 else 0 end) as Browse_count_class_6,
		  sum ( case when ((DISPLAY_TIME_24_HOUR_FORMAT    between '20:00:00' and '23:59:59') or (DISPLAY_TIME_24_HOUR_FORMAT 
		  between '00:00:00' and '07:59:59' ))  then 1 else 0 end) as Browse_count_class_7
		  From (
		  Select *
		  From browse 
		  )
	group by bnid,year, month_name, day_name
	)

	,session_cte as (
	Select *, case when time_diff>1800 or time_diff is null then 1 else 0 end as new_session_flag
	From (
	Select *, time_diff(time, lag_time, second) as time_diff
	From(
	Select * ,cast(DISPLAY_TIME_24_HOUR_FORMAT as time) as time , lag(cast(DISPLAY_TIME_24_HOUR_FORMAT as time)) over (partition by bnid, date_key order by cast(DISPLAY_TIME_24_HOUR_FORMAT as time))  as lag_time
	From browse
	))
	)
	,Aggregation as (
	Select distinct bnid, year,month_name, day_name, 
									   case when class_1_flag=1 then sum(browse_time) over (partition by bnid, year, month_name, day_name, class_1_flag) end as Time_spent_class1
									   ,case when class_2_flag=1 then sum(browse_time) over (partition by bnid, year, month_name, day_name, class_2_flag) end as Time_spent_class2
									   ,case when class_3_flag=1 then sum(browse_time) over (partition by bnid, year, month_name, day_name, class_3_flag) end as Time_spent_class3
									   ,case when class_4_flag=1 then sum(browse_time) over (partition by bnid,year,  month_name, day_name, class_4_flag) end as Time_spent_class4
										,case when class_5_flag=1 then sum(browse_time) over (partition by bnid,year,  month_name, day_name, class_5_flag) end as Time_spent_class5
									   ,case when class_6_flag=1 then sum(browse_time) over (partition by bnid, year, month_name, day_name, class_6_flag) end as Time_spent_class6
										,case when class_7_flag=1 then sum(browse_time) over (partition by bnid,year,  month_name, day_name, class_7_flag) end as Time_spent_class7
											  
											  From (
	Select distinct bnid,year,month_name, day_name, date_key, session,min_time_session,
								max_time_session
	,                            max(browse_time) over (partition by bnid,year,month_name, day_name, date_key, session) as browse_time
								,case when  (min_time_session  between '08:00:00' and '9:59:59'
											  and 
											 max_time_session  between '08:00:00' and '9:59:59' ) 
											 then 1 else 0 end as class_1_flag
								,case when ( min_time_session   between '10:00:00' and '11:59:59' 
											and 
											max_time_session   between '10:00:00' and '11:59:59' )
											  then 1 else 0 
											  end as class_2_flag
								 ,case when ( min_time_session   between '12:00:00' and '13:59:59'
											and 
											max_time_session   between '12:00:00' and '13:59:59' )
											  then 1 else 0 
											  end as class_3_flag
								  ,case when ( min_time_session   between  '14:00:00' and '15:59:59'
											and 
											max_time_session   between  '14:00:00' and '15:59:59' )
											  then 1 else 0 
											  end as class_4_flag
								   ,case when ( min_time_session   between  '16:00:00' and '17:59:59'
											and 
											max_time_session   between '16:00:00' and '17:59:59' )
											  then 1 else 0 
											  end as class_5_flag
								   ,case when ( min_time_session   between  '18:00:00' and '19:59:59'
											and 
											max_time_session   between '18:00:00' and '19:59:59' )
											  then 1 else 0 
											  end as class_6_flag
								, case when (((min_time_session    between '20:00:00' and '23:59:59')
											and 
											(max_time_session    between '20:00:00' and '23:59:59')) 
											  or    (min_time_session   between '00:00:00' and '07:59:59' 
											  and
											  max_time_session between '00:00:00' and '07:59:59' ))  
											then 1 else 0 
											end as class_7_flag
	From (
	Select distinct *, 
			max(time) over (partition by bnid, date_key,session )  as max_time_session, min(time) over (partition by bnid, date_key,session ) as min_time_session,
			time_diff(max(time) over (partition by bnid, date_key,session ) , min(time) over (partition by bnid, date_key,session ), second) as browse_time
	From (
	Select *, sum(new_session_flag) over (partition by bnid, date_key  rows between unbounded preceding and current row) as session
	From session_cte)

	))
	)
	Select distinct agg.bnid, agg.year, agg.month_name, agg.day_name, bea.marital_status, bcp.derived_gender, bcp.ship_to_city,
				ifnull(first_value(Time_spent_class1) over (partition by agg.bnid,agg.year, agg.month_name, agg.day_name order by Time_spent_class1 desc),0) as Time_spent_class1
				,ifnull(first_value(Time_spent_class2) over (partition by agg.bnid, agg.year, agg.month_name, agg.day_name order by Time_spent_class2 desc),0) as Time_spent_class2
				,ifnull(first_value(Time_spent_class3) over (partition by agg.bnid, agg.year, agg.month_name, agg.day_name order by Time_spent_class3 desc),0) as Time_spent_class3
				,ifnull(first_value(Time_spent_class4) over (partition by agg.bnid, agg.year,  agg.month_name, agg.day_name order by Time_spent_class4 desc),0) as Time_spent_class4
				,ifnull(first_value(Time_spent_class5) over (partition by agg.bnid,agg.year,  agg.month_name, agg.day_name order by Time_spent_class5 desc),0) as Time_spent_class5
				,ifnull(first_value(Time_spent_class6) over (partition by agg.bnid,agg.year,  agg.month_name, agg.day_name order by Time_spent_class6 desc),0) as Time_spent_class6
				,ifnull(first_value(Time_spent_class7) over (partition by agg.bnid,agg.year,  agg.month_name, agg.day_name order by Time_spent_class7 desc),0) as Time_spent_class7
				,bc.* except (bnid,  month_name, day_name, year)
	From Aggregation agg
	join browse_counts bc
	on agg.bnid= bc.bnid  and agg.year=bc.year and  agg.month_name= bc.month_name and agg.day_name= bc.day_name
	left join
	(SELECT bnid,marital_status FROM `{project_name}.{customer_data}.bn_explicit_attributes`) bea
	on agg.bnid = bea.bnid
	left join 
	(Select bnid, derived_gender,ship_to_city,ship_to_country_code,ship_to_postal_code from `{project_name}.{customer_data}.bn_customer_profile`) bcp
	on agg.bnid = bcp.bnid
	order by agg.bnid,agg.year,agg.month_name,agg.day_name""".format(
		project_name,
		stg_data,
		customer_data,
		o_warehouse,
		dssprod_data,
		run_date,
	)
	return bql


def opentime_counts(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	customer_data = params["customer_data"]
	dssprod_data = params["dssprod_data"]
	o_warehouse = params["o_warehouse"]
	delta_date = params["delta_date"]

	bql = f"""
	SELECT
	eop.bnid,
	FORMAT_DATE('%Y', DATETIME(activity_date)) as year,
	FORMAT_DATE('%B', DATETIME(activity_date)) as month_name,
	FORMAT_DATE('%A', DATETIME(activity_date)) as day_name,
	marital_status,
	derived_gender,
	ship_to_city,
	sum(Case when time(activity_time) between '08:00:00' and '10:00:00' then 1 else 0 end) as oc_8_10,
	sum(Case when time(activity_time) between '10:00:00' and '12:00:00' then 1 else 0 end) as oc_10_12,
	sum(Case when time(activity_time) between '12:00:00' and '14:00:00' then 1 else 0 end) as oc_12_14,
	sum(Case when time(activity_time) between '14:00:00' and '16:00:00' then 1 else 0 end) as oc_14_16,
	sum(Case when time(activity_time) between '16:00:00' and '18:00:00' then 1 else 0 end) as oc_16_18,
	sum(Case when time(activity_time) between '18:00:00' and '20:00:00' then 1 else 0 end) as oc_18_20,
	sum(Case when ((time(activity_time) between '00:00:00' and '08:00:00') or (time(activity_time) between '20:00:00' and '23:59:59')) then 1 else 0 end) as oc_20_8,
	count(*) as Count
	FROM `{project_name}.{o_warehouse}.email_open_fact` eop
	left join
	(SELECT bnid,marital_status FROM `{project_name}.{customer_data}.bn_explicit_attributes`) bea
	on eop.bnid = bea.bnid
	left join 
	(Select bnid, derived_gender,ship_to_city,ship_to_country_code,ship_to_postal_code from `{project_name}.{customer_data}.bn_customer_profile`) bcp
	on bea.bnid = bcp.bnid
	where eop.bnid in 
	(
	SELECT distinct bnid FROM `{project_name}.{dssprod_data}.email_address_subscription` eas
	left join
	(SELECT bnid,email_address FROM `{project_name}.{stg_data}.bn_email_guid`
	group by 1,2) beg
	on eas.email_address = beg.email_address
	where subscribe_flag = 'Y' and bnid is not null 
	)
	and eop.bnid is not null and eop.activity_date >= date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 3 month)
	group by 1,2,3,4,5,6,7
	order by 1,2,3,4,5,6,7""".format(
		project_name,
		stg_data,
		customer_data,
		o_warehouse,
		dssprod_data,
		delta_date,
	)
	return bql
	
def lastopen_slot(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	customer_data = params["customer_data"]
	dssprod_data = params["dssprod_data"]
	o_warehouse = params["o_warehouse"]
	delta_date = params["delta_date"]

	bql = f"""WITH lastopen_time as
	(
	SELECT bnid, 
	first_value(activity_time) over (partition by bnid order by activity_time desc) as last_opentime
	FROM `{project_name}.{o_warehouse}.email_open_fact` eop
	where bnid is not null
	)
	Select bnid,
	Case when time(last_opentime) between '08:00:00' and '10:00:00' then '08:00:00'
	when time(last_opentime) between '10:00:00' and '12:00:00' then '10:00:00'
	when time(last_opentime) between '12:00:00' and '14:00:00' then '12:00:00'
	when time(last_opentime) between '14:00:00' and '16:00:00' then '14:00:00'
	when time(last_opentime) between '16:00:00' and '18:00:00' then '16:00:00'
	when time(last_opentime) between '18:00:00' and '20:00:00' then '18:00:00'
	when ((time(last_opentime) between '00:00:00' and '08:00:00') or (time(last_opentime) between '20:00:00' and '23:59:59')) then '20:00:00'
	else NULL 
	end as last_openslot,
	from lastopen_time
	group by 1,2""".format(
		project_name,
		stg_data,
		customer_data,
		dssprod_data,
		o_warehouse,
		delta_date,
	)
	return bql
	
def email_subscription(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	customer_data = params["customer_data"]
	dssprod_data = params["dssprod_data"]
	o_warehouse = params["o_warehouse"]
	delta_date = params["delta_date"]

	bql = f"""with email_subscription as 
	(
	SELECT distinct beg.bnid,ship_to_city FROM `{project_name}.{dssprod_data}.email_address_subscription` eas
	left join
	(SELECT bnid,email_address FROM `{project_name}.{stg_data}.bn_email_guid`
	group by 1,2) beg
	on eas.email_address = beg.email_address
	left join 
	(Select bnid,ship_to_city from `{project_name}.{customer_data}.bn_customer_profile`) bcp
	on beg.bnid = bcp.bnid
	where subscribe_flag = 'Y' and beg.bnid is not null
	)
	Select * from email_subscription
	""".format(
		project_name,
		stg_data,
		customer_data,
		dssprod_data,
		o_warehouse,
		delta_date,
	)
	return bql