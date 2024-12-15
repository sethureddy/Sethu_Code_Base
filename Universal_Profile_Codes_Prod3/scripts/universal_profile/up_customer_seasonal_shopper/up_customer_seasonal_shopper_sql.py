def up_customer_seasonal_shopper(QUERY_PARAMS):
    project_name = QUERY_PARAMS["project_name"]
    customer_data = QUERY_PARAMS["dataset_name1"]
    dataset_name_dssprod = QUERY_PARAMS["dataset_name_dssprod"]
    bql=f"""create or replace table {project_name}.{customer_data}.up_customer_seasonal_shopper as
		/*Get all orders from last 2 years*/
	with all_shopper as 
	(
	select distinct 
	uph.email_address_key,
	eg.email_address,
	submit_date,
	from 
	  `{project_name}.{customer_data}.up_purchase_history` uph
	  /*up_purchase history taken as base table for transaction/purchase*/
	  left join `{project_name}.{dataset_name_dssprod}.email_guid` eg on uph.email_address_key = eg.email_address_key
	  /*email_address taken from email_guid table against each email_address_key*/
	  where submit_date between date_sub(current_date, interval 2 year) and current_date
	  /*only purchases made in 2 year window considered*/
	  and merch_sub_category_rollup <> 'Engagement'
	  /*filter for removing engagement purchases*/
	  and return_date is null
	  /*returned orders not considered*/
	  and email_address is not null
	  /*remove rows where email_address is nor available*/
	)

	/*table to generate dates and date range of different seasonal events-
	  -valentine day period
	  -mother's day period
	  -black friday and christmas period
	*/
	,seasonal_dates_base as
	(
	select 
	year,
	valentine_start,
	valentine_end,
	mothers_day,
	date_sub(mothers_day, interval 19 day) as mothers_day_start,
	date_sub(mothers_day, interval 1 day) as mothers_day_end,
	/*generate date range of seasonal period before mother's day*/
	thanks_giving,
	black_friday,
	date_sub(black_friday, interval 7 day) as blackfriday_christmas_start,
	date_add(date_add(date, interval 11 month), interval 22 day) as blackfriday_christmas_end
	/*generate date range of seasonal period before black friday- christmas*/
	from
	  (
	  select *,
	  date_add(thanks_giving, interval 1 day) as black_friday
	  /*get black friday date for any year*/
	  from
		(
		select *,
		date_add(date, interval 1 month) as valentine_start,                           /*get 1st Feb as start of valentine period*/
		date_add(date_add(date, interval 1 month), interval 12 day) as valentine_end,  /*get 13th Feb as end of valentine period*/
		case 
		  when extract(dayofweek from date_add(date, interval 4 month)) != 1
		  then date_add(date_add(date, interval 4 month), interval 8+7-extract(dayofweek from date_add(date, interval 4 month)) day)
		  else date_add(date_add(date, interval 4 month), interval 7 day)
		  end as mothers_day,
		/*above logic is used to get mother's day for any year*/
		case 
		  when extract(dayofweek from date_add(date, interval 10 month)) = 6
		  then date_add(date_add(date, interval 10 month), interval 27 day)
		  when extract(dayofweek from date_add(date, interval 10 month)) = 7
		  then date_add(date_add(date, interval 10 month), interval 26 day)
		  else date_add(date_add(date, interval 10 month), interval 26-extract(dayofweek from date_add(date, interval 10 month)) day)
		  end as thanks_giving
		/*above logic is used to get thanksgiving day for any year*/
		from
		  (
		  select
		  extract(year from date) as year,
		  date
		  from 
			unnest(generate_date_array(date_sub(DATE_TRUNC(current_date(), year), interval 2 year),DATE_TRUNC(current_date(), year), interval 1 year)) 
			AS date
		  /*get different years in the 2 year period*/
		  )
		)
	  )
	)

	/*final calendar for with all dates which comes under the seasonal period which will be used to tag a transaction as seasonal/non-seasonal*/
	,seasonal_calendar as
	(
	select *
	from
	  (
	  select cld.date as seasonal_date
		from 
		(
		select 
		date
		from 
		  unnest(
		  generate_date_array(date_sub(DATE_TRUNC(current_date(), year), interval 2 year),
		  date_add(date_add(DATE_TRUNC(current_date(), year), interval 11 month), interval 30 day), interval 1 day)) AS date
		  ) cld
		  /*generate calendar table for the 2 year period which will be used to extract all required seasonal period dates */
		  inner join 
		  (
		  select year, valentine_start as start_date, valentine_end as end_date from seasonal_dates_base
		  union all
		  (select year, mothers_day_start, mothers_day_end from seasonal_dates_base)
		  union all
		  (select year, blackfriday_christmas_start, blackfriday_christmas_end from seasonal_dates_base)
		) seacld
		/*seasonal date range*/
		on cld.date between seacld.start_date and seacld.end_date
	  )
	  where seasonal_date between date_sub(current_date, interval 2 year) and current_date
	  /*get only those seasonal period dates which fall within the 2 year window*/
	)

	/*
	Seasonal Shopper-
	  Customers who ONLY made orders before holiday seasons(in a particular window), which includes:
		-Valentine's Day
		-Motherâ€™s Day
		-Black Friday and Christmas
	All other customers are year-round shoppers
	*/
	,final_output as
	(
	select 
	ss.email_address,
	embd.subscribe_flag,
	ss.seasonal_shopper_flag,
	embd.bnid
	from
	  (
	  select distinct 
	  email_address_key,
	  email_address,
	  seasonal_shopper_flag
	  from
		(
		select *,
		min(seasonal_transaction_flag) over (partition by email_address_key, email_address) as seasonal_shopper_flag 
		/*rolling the transaction level tagging over email_address level
		  - if all transactions of a customer are 1(seasonal) then seasonal shopper
		  - if any transaction of a customer is 0(non seasonal) the year-round shopper
		*/
		from
		  (
		  select *,
		  case 
		  when submit_date in (select * from seasonal_calendar) 
		  then 1 else 0 end as seasonal_transaction_flag
		  /*tag a transaction as seasonal/non-seasonal
			-if purchase date(submit date) fall within our seasaonal calendar then 1(seasonal)
			-else 0(year around shopper)
		  */
		  from all_shopper 
		  ) 
		)
	  ) ss
	  left join (select distinct email_address, bnid, subscribe_flag from `{project_name}.{customer_data}.email_customer_profile`) embd
	  on ss.email_address = embd.email_address
	  /*getting the corresponding bnid and subscription status of every email_address from email_customer_profile table*/
	)

	select *
	from final_output""".format(
        project_name,
        customer_data,
        dataset_name_dssprod
        )
    return bql