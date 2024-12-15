def bnid_daylevel_conversion(params):
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name2 = params["dataset_name2"]
    dataset_name3 = params["dataset_name3"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""WITH customer_base
	AS (
		SELECT cf.date_key
			,eg.email_address
			,cf.PAGE_KEY
			,cast(cf.offer_id AS string) offer_id
			,pgd.PRIMARY_BROWSE_SEGMENT
			,pgd.SECONDARY_BROWSE_SEGMENT
			,pgd.ACTIVITY_SET_CODE
			,dtd.DEVICE_TYPE
			,ud.URI
			,pgd.PAGE_GROUP_NAME
			,pof.guid_key AS product_order_guid_key
			,pof.offer_key AS product_order_offer_key
			,pof.customer_submit_date_key
			,parse_time('%T', cast(DISPLAY_TIME_24_HOUR_FORMAT as string)) time_log
			,extract(year FROM parse_date('%Y%m%d', cast(date_key AS string))) year
			,extract(month FROM parse_date('%Y%m%d', cast(date_key AS string))) month
			,bnid as guid_key
		FROM (select guid_key,date_key,page_key,offer_id,page_group_key,device_type_key,time_key,uri_key,offer_key from `{project_name}.{dataset_name_dssprod}.click_fact` where parse_date('%Y%m%d', cast(date_key AS string)) between DATE_SUB(parse_date('%Y%m%d', cast({delta_date} AS string)),interval 90 day ) and parse_date('%Y%m%d', cast({delta_date} AS string))) cf
		LEFT JOIN `{project_name}.{dataset_name_dssprod}.page_group_dim` pgd
			ON cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
		LEFT JOIN `{project_name}.{dataset_name_dssprod}.device_type_dim` dtd
			ON cf.Device_TYPE_KEY = dtd.DEVICE_TYPE_KEY
		LEFT JOIN `{project_name}.{dataset_name_dssprod}.time_dim` td
			ON cf.TIME_KEY = td.TIME_KEY
		LEFT JOIN `{project_name}.{dataset_name_dssprod}.uri_dim` ud
			ON cf.URI_KEY = ud.URI_KEY
		LEFT JOIN (select guid_key,offer_key,CUSTOMER_SUBMIT_DATE_KEY  from `{project_name}.{dataset_name_dssprod}.product_order_fact` where extract(year FROM parse_date('%Y%m%d', cast(CUSTOMER_SUBMIT_DATE_KEY AS string))) >= 2015) pof
			ON cf.guid_key = pof.guid_key
				AND cf.offer_key = pof.offer_key
        join `{project_name}.{dataset_name_dssprod}.email_guid` eg
            on cf.guid_key = eg.guid_key
		join {project_name}.{dataset_name2}.bn_guid_all   bgm 
			on eg.guid = bgm.guid
		WHERE cf.guid_key NOT IN (2074336457, 1262559502,2068228906,2081498144) and  bnid in (Select distinct bnid from `{project_name}.{dataset_name3}.bn_pj_tagging` where Bucket='LC')
		)
	,flags as(
			SELECT *
				,CASE 
					WHEN (
							(
								lower(primary_browse_segment) = 'engagement'
								AND lower(secondary_browse_segment) IN ('byor', 'byo3sr', 'preset', 'general')
								)
							OR (
								lower(primary_browse_Segment) = 'diamond'
								AND lower(secondary_browse_segment) = 'loose_diamond'
								)
							)
						THEN 1
					ELSE 0
					END AS Engagement_flag
				,CASE 
					WHEN (
							lower(primary_browse_segment) = 'engagement'
							AND lower(secondary_browse_segment) IN ('byor', 'byo3sr', 'general')
							)
						THEN 1
					ELSE 0
					END AS byo_flag
				,CASE 
					WHEN (
							lower(primary_browse_segment) = 'engagement'
							AND lower(secondary_browse_segment) IN ('preset')
							)
						THEN 1
					ELSE 0
					END AS preset_flag
				,CASE 
					WHEN (
							lower(primary_browse_Segment) = 'diamond'
							AND lower(secondary_browse_segment) = 'loose_diamond'
							)
						THEN 1
					ELSE 0
					END AS ld_flag
				,CASE 
					WHEN (
							lower(primary_browse_segment) = 'engagement'
							AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
							)
						AND lower(page_group_name) IN ('byor add to basket', 'byo3sr add to basket')
						THEN 1
					ELSE 0
					END AS byo_cart_flag
				,CASE 
					WHEN (
							lower(primary_browse_segment) = 'engagement'
							AND lower(secondary_browse_segment) IN ('preset')
							)
						AND lower(page_group_name) IN ('preset engagement add to basket')
						THEN 1
					ELSE 0
					END AS preset_cart_flag
				,CASE 
					WHEN (
							lower(primary_browse_Segment) = 'diamond'
							AND lower(secondary_browse_segment) = 'loose_diamond'
							)
						AND lower(page_group_name) IN ('loose diamond add to basket')
						THEN 1
					ELSE 0
					END AS ld_cart_flag
				,CASE 
					WHEN lower(activity_set_code) IN ('diamond search', 'diamond detail')
						AND (
							(
								lower(primary_browse_segment) = 'engagement'
								AND lower(secondary_browse_segment) IN ('byor', 'byo3sr', 'general')
								)
							OR (
								lower(primary_browse_Segment) = 'diamond'
								AND lower(secondary_browse_segment) = 'loose_diamond'
								)
							)
						THEN 1
					ELSE 0
					END AS diamond_search_flag
				,CASE 
					WHEN lower(activity_set_code) = 'diamond search'
						AND (
								lower(primary_browse_segment) = 'engagement'
								AND lower(secondary_browse_segment) IN ('byor', 'byo3sr', 'general')
								)
						THEN 1
					ELSE 0
					END AS byo_diamond_catalog_flag
				,CASE 
					WHEN lower(activity_set_code) = 'diamond search'
						AND (
								lower(primary_browse_Segment) = 'diamond'
								AND lower(secondary_browse_segment) = 'loose_diamond'
								)
						THEN 1
					ELSE 0
					END AS ld_diamond_catalog_flag
				,CASE 
					WHEN lower(activity_set_code) = 'diamond detail'
						AND	(
								lower(primary_browse_segment) = 'engagement'
								AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
								)
						THEN 1
					ELSE 0
					END AS byo_diamond_detail_flag
				,CASE 
					WHEN lower(activity_set_code) = 'diamond detail'
						AND (
								lower(primary_browse_Segment) = 'diamond'
								AND lower(secondary_browse_segment) = 'loose_diamond'
								)
						THEN 1
					ELSE 0
					END AS ld_diamond_detail_flag
				,CASE 
					WHEN lower(primary_browse_segment) = 'engagement'
						AND lower(secondary_browse_segment) IN ('byor', 'byo3sr', 'general')
						AND lower(PAGE_GROUP_NAME) IN ('byo3sr setting catalog', 'byor setting catalog', 'engagement catalog')
						THEN 1
					ELSE 0
					END AS byo_setting_catalog_flag
				,CASE 
					WHEN lower(primary_browse_segment) = 'engagement'
						AND lower(secondary_browse_segment) IN ('preset')
						AND lower(PAGE_GROUP_NAME) IN ('engagement preset rings catalog')
						THEN 1
					ELSE 0
					END AS preset_setting_catalog_flag
				,CASE 
					WHEN lower(primary_browse_segment) = 'engagement'
						AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
						AND lower(PAGE_GROUP_NAME) IN ('byo3sr setting detail', 'byor setting detail')
						THEN 1
					ELSE 0
					END AS byo_setting_detail_flag
				,CASE 
					WHEN lower(primary_browse_segment) = 'engagement'
						AND lower(secondary_browse_segment) IN ('preset')
						AND PAGE_GROUP_NAME IN ('engagement preset detail')
						THEN 1
					ELSE 0
					END AS preset_detail_flag
				,CASE 
					WHEN lower(primary_browse_segment) = 'engagement'
						AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
						AND lower(PAGE_GROUP_NAME) IN ('byo3sr review your ring', 'byor review your ring')
						THEN 1
					ELSE 0
					END AS byo_build_unit_review_flag
				,min(time_log) OVER (
					PARTITION BY guid_key
					,date_key
					) min_time_log
				,max(time_log) OVER (
					PARTITION BY guid_key
					,date_key
					) max_time_log
				,CASE 
					WHEN page_group_name LIKE '%chat%'
						THEN 1
					ELSE 0
					END AS chat_flag
				,CASE 
					WHEN page_group_name LIKE '%showroom%'
						THEN 1
					ELSE 0
					END AS showroom_flag
				,CASE 
					WHEN lower(primary_browse_segment) = 'education'
						AND lower(secondary_browse_segment) = 'engagement'
						THEN 1
					ELSE 0
					END AS eng_education_flag
				,CASE 
					WHEN lower(primary_browse_segment) = 'education'
						AND lower(secondary_browse_segment) = 'diamond'
						THEN 1
					ELSE 0
					END AS diamond_education_flag
				,CASE 
					WHEN uri LIKE '%/service%'
						THEN 1
					ELSE 0
					END AS brand_promise_page_flag
				,CASE 
					WHEN product_order_guid_key IS NOT NULL
						AND product_order_offer_key IS NOT NULL and ((lower(primary_browse_segment) = 'engagement'
								AND lower(secondary_browse_segment) IN ('byor', 'byo3sr', 'preset', 'general')
								)
							OR (
								lower(primary_browse_Segment) = 'diamond'
								AND lower(secondary_browse_segment) = 'loose_diamond'
								))
						THEN customer_submit_date_key
					END AS order_date
				,CASE 
					WHEN device_type = 'phone'
						THEN 1
					ELSE 0
					END AS phone_flag
				,CASE 
					WHEN device_type = 'desktop'
						THEN 1
					ELSE 0
					END AS desktop_flag
        ,CASE 
				WHEN (lower(primary_browse_segment) = 'engagement'	and lower(secondary_browse_segment) in ('byor','byo3sr','preset') or (lower(primary_browse_Segment) = 'diamond' and lower(secondary_browse_segment) = 'loose_diamond') ) and  lower(page_group_name) in ('byor add to basket','byo3sr add to basket','preset engagement add to basket','loose diamond add to basket')
					THEN 1
				ELSE 0
				END AS eng_cart_flag
			FROM customer_base
			)
	,browse_data AS (
		SELECT *
			,CASE 
				WHEN Engagement_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,Engagement_flag
							)
				ELSE 0
				END AS browse_count_engagement
			,CASE 
				WHEN byo_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_flag
							)
				ELSE 0
				END AS Browse_byo
			,CASE 
				WHEN preset_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,preset_flag
							)
				ELSE 0
				END AS Browse_preset
			,CASE 
				WHEN ld_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_flag
							)
				ELSE 0
				END AS Browse_ld
			,max(byo_cart_flag) OVER (
				PARTITION BY guid_key
				,date_key
				) AS add_to_cart_byo
			,max(preset_cart_flag) OVER (
				PARTITION BY guid_key
				,date_key
				) AS add_to_cart_preset
			,max(ld_cart_flag) OVER (
				PARTITION BY guid_key
				,date_key
				) AS add_to_cart_ld
			,CASE 
				WHEN diamond_search_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,diamond_search_flag
							)
				ELSE 0
				END AS diamond_search_count
			,CASE 
				WHEN byo_diamond_catalog_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_diamond_catalog_flag
							)
				ELSE 0
				END AS byo_diamond_catalog_page_count
			,CASE 
				WHEN ld_diamond_catalog_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_diamond_catalog_flag
							)
				ELSE 0
				END AS ld_diamond_catalog_page_count
			,CASE 
				WHEN byo_diamond_detail_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_diamond_detail_flag
							)
				ELSE 0
				END AS byo_diamond_detail_page_count
			,CASE 
				WHEN ld_diamond_detail_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_diamond_detail_flag
							)
				ELSE 0
				END AS ld_diamond_detail_page_count
			,CASE 
				WHEN byo_setting_catalog_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_setting_catalog_flag
							)
				ELSE 0
				END AS byo_setting_catalog_page_count
			,CASE 
				WHEN preset_setting_catalog_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,preset_setting_catalog_flag
							)
				ELSE 0
				END AS preset_setting_catalog_page_count
			,CASE 
				WHEN byo_setting_detail_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_setting_detail_flag
							)
				ELSE 0
				END AS byo_setting_detail_page_count
			,CASE 
				WHEN preset_detail_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,preset_detail_flag
							)
				ELSE 0
				END AS preset_setting_detail_page_count
			,time_diff(max_time_log, min_time_log, second) dwell_time_seconds_site
			,CASE 
				WHEN byo_flag = 1
					THEN min(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_flag
							)
				END AS byo_min_time_log
			,CASE 
				WHEN byo_flag = 1
					THEN max(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_flag
							)
				END AS byo_max_time_log
			,CASE 
				WHEN preset_flag = 1
					THEN min(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,preset_flag
							)
				END AS preset_min_time_log
			,CASE 
				WHEN preset_flag = 1
					THEN max(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,preset_flag
							)
				END AS preset_max_time_log
			,CASE 
				WHEN ld_flag = 1
					THEN min(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_flag
							)
				END AS ld_min_time_log
			,CASE 
				WHEN ld_flag = 1
					THEN max(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_flag
							)
				END AS ld_max_time_log
			,CASE 
				WHEN Engagement_flag = 1
					THEN min(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_flag
							)
				END AS eng_min_time_log
			,CASE 
				WHEN Engagement_flag = 1
					THEN max(time_log) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_flag
							)
				END AS eng_max_time_log
			,CASE 
				WHEN Engagement_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,Engagement_flag
							)
				END AS browse_month_first_dt_engagement
			,CASE 
				WHEN Engagement_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,Engagement_flag
							)
				END AS browse_month_last_dt_engagement
			,CASE 
				WHEN Engagement_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,Engagement_flag
							)
				END AS browse_year_first_dt_engagement
			,CASE 
				WHEN Engagement_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,Engagement_flag
							)
				END AS browse_year_last_dt_engagement
			,CASE 
				WHEN byo_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,byo_flag
							)
				END AS Browse_byo_first_dt_month
			,CASE 
				WHEN byo_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,byo_flag
							)
				END AS Browse_byo_last_dt_month
			,CASE 
				WHEN byo_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,byo_flag
							)
				END AS Browse_byo_first_dt_year
			,CASE 
				WHEN byo_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,byo_flag
							)
				END AS Browse_byo_last_dt_year
			,CASE 
				WHEN preset_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,preset_flag
							)
				END AS Browse_preset_first_dt_month
			,CASE 
				WHEN preset_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,preset_flag
							)
				END AS Browse_preset_last_dt_month
			,CASE 
				WHEN preset_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,preset_flag
							)
				END AS Browse_preset_first_dt_year
			,CASE 
				WHEN preset_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,preset_flag
							)
				END AS Browse_preset_last_dt_year
			,CASE 
				WHEN ld_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,ld_flag
							)
				END AS Browse_ld_first_dt_month
			,CASE 
				WHEN ld_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,month
							,ld_flag
							)
				END AS Browse_ld_last_dt_month
			,CASE 
				WHEN ld_flag = 1
					THEN min(date_key) OVER (
							PARTITION BY guid_key
							,year
							,ld_flag
							)
				END AS Browse_ld_first_dt_year
			,CASE 
				WHEN ld_flag = 1
					THEN max(date_key) OVER (
							PARTITION BY guid_key
							,year
							,ld_flag
							)
				END AS Browse_ld_last_dt_year
			,count(guid_key) OVER (
				PARTITION BY guid_key
				,date_key
				) AS hit_per_day_site
			,count(DISTINCT page_key) OVER (
				PARTITION BY guid_key
				,date_key
				) AS page_visit_count_distinct
			,CASE 
				WHEN phone_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,phone_flag
							)
				ELSE 0
				END AS phone_visit
			,CASE 
				WHEN desktop_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,desktop_flag
							)
				ELSE 0
				END AS desktop_visit
			,CASE 
				WHEN chat_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,chat_flag
							)
				ELSE 0
				END AS webchats_count
			,CASE 
				WHEN showroom_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,showroom_flag
							)
				ELSE 0
				END AS showroom_page_count
			,CASE 
				WHEN eng_education_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,eng_education_flag
							)
				ELSE 0
				END AS eng_education_page_count
			,CASE 
				WHEN diamond_education_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,diamond_education_flag
							)
				ELSE 0
				END AS diamond_education_page_count
			,CASE 
				WHEN byo_build_unit_review_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_build_unit_review_flag
							)
				ELSE 0
				END AS byo_build_unit_review_count
			,CASE 
				WHEN brand_promise_page_flag = 1
					THEN count(guid_key) OVER (
							PARTITION BY guid_key
							,date_key
							,brand_promise_page_flag
							)
				ELSE 0
				END AS brand_promise_page_count
			,min(order_date) OVER (PARTITION BY guid_key) AS customer_first_order_date
			,max(order_date) OVER (PARTITION BY guid_key) AS customer_last_order_date
			,CASE 
				WHEN byo_flag = 1
					THEN count(DISTINCT offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_flag
							)
				ELSE 0
				END AS byo_product_count_distinct
			,CASE 
				WHEN preset_flag = 1
					THEN count(DISTINCT offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,preset_flag
							)
				ELSE 0
				END AS preset_product_count_distinct
			,CASE 
				WHEN ld_flag = 1
					THEN count(DISTINCT offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_flag
							)
				ELSE 0
				END AS ld_product_count_distinct
			,CASE 
				WHEN engagement_flag = 1
					THEN count(DISTINCT offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,engagement_flag
							)
				ELSE 0
				END AS eng_product_count_distinct
			,CASE 
				WHEN byo_flag = 1
					THEN count(offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,byo_flag
							)
				ELSE 0
				END AS byo_product_count_all
			,CASE 
				WHEN preset_flag = 1
					THEN count(offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,preset_flag
							)
				ELSE 0
				END AS preset_product_count_all
			,CASE 
				WHEN ld_flag = 1
					THEN count(offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,ld_flag
							)
				ELSE 0
				END AS ld_product_count_all
			,CASE 
				WHEN engagement_flag = 1
					THEN count(offer_id) OVER (
							PARTITION BY guid_key
							,date_key
							,engagement_flag
							)
				ELSE 0
				END AS eng_product_count_all
          ,case
            when byo_flag = 1 
              then string_agg(offer_id ) over(partition by guid_key,date_key,byo_flag)
              
            end as byo_product_array_all
          ,case
            when preset_flag = 1
              then string_agg( offer_id ) over(partition by guid_key,date_key,preset_flag)
             
            end as preset_product_array_all
          ,case
            when ld_flag = 1
              then string_agg( offer_id ) over(partition by guid_key,date_key,ld_flag)
             
            end as  ld_product_array_all
          ,case 
            when engagement_flag = 1
              then string_agg( offer_id ) over(partition by guid_key,date_key,engagement_flag)
            end as eng_product_array_all
          ,sum(byo_cart_flag) OVER (PARTITION BY guid_key,date_key) AS total_items_add_to_cart_byo
            ,sum(preset_cart_flag) OVER (PARTITION BY guid_key,date_key) AS total_items_add_to_cart_preset
            ,sum(ld_cart_flag) OVER (PARTITION BY guid_key,date_key) AS total_items_add_to_cart_ld
        ,sum(eng_cart_flag) OVER (PARTITION BY guid_key,date_key) AS total_items_add_to_cart_engagement
            FROM flags )
        SELECT DISTINCT ena.guid_key
        ,ena.date_key
        ,ena.brand_promise_page_view
        ,ena.browse_count_byo
        ,ena.browse_count_engagement
        ,ena.browse_count_ld
        ,ena.browse_count_preset
        ,ena.browse_month_first_dt_engagement
        ,ena.browse_year_first_dt_engagement
        ,ena.browse_month_first_dt_ld
        ,ena.browse_month_last_dt_engagement
        ,ena.browse_year_last_dt_engagement
        ,ena.browse_month_last_dt_ld
        ,ena.browse_month_first_dt_byo
        ,ena.browse_month_first_dt_preset
        ,ena.browse_month_last_dt_byo
        ,ena.browse_month_last_dt_preset
        ,ena.browse_year_first_dt_byo
        ,ena.browse_year_first_dt_ld
        ,ena.browse_year_first_dt_preset
        ,ena.browse_year_last_dt_byo
        ,ena.browse_year_last_dt_ld
        ,ena.browse_year_last_dt_preset
        ,ena.BUR_view_byo
        ,ena.catalog_page_view_diamond_byo
      ,ena.catalog_page_view_diamond_ld
      ,0 as catalog_page_view_diamond_preset
        ,ena.catalog_page_view_setting_byo
        ,ena.catalog_page_view_setting_preset
      ,0 as catalog_page_view_setting_ld
        ,ena.detail_page_view_diamond_byo
      ,ena.detail_page_view_diamond_ld
      ,0 as detail_page_view_diamond_preset
        ,ena.detail_page_view_setting_preset
        ,ena.detail_page_view_setting_byo
        ,first_value(ena.byo_dwell_time_seconds) OVER (
            PARTITION BY guid_key
            ,date_key ORDER BY ena.byo_dwell_time_seconds DESC
            ) AS dwell_time_seconds_byo
        ,first_value(ena.eng_dwell_time_seconds) OVER (
            PARTITION BY ena.guid_key
            ,ena.date_key ORDER BY ena.eng_dwell_time_seconds DESC
            ) AS dwell_time_seconds_engagement
        ,first_value(ena.ld_dwell_time_seconds) OVER (
            PARTITION BY guid_key
            ,date_key ORDER BY ena.ld_dwell_time_seconds DESC
            ) AS dwell_time_seconds_ld
        ,first_value(ena.preset_dwell_time_seconds) OVER (
            PARTITION BY guid_key
            ,date_key ORDER BY ena.preset_dwell_time_seconds DESC
            ) AS dwell_time_seconds_preset
        ,ena.dwell_time_seconds_site
        ,ena.education_page_view_engagement
        ,ena.education_page_view_diamond
        ,bcp.derived_gender as gender
        ,ena.hit_per_day_site
        ,ena.first_order_date
        ,ena.last_order_date
        ,bea.marital_status
        ,ena.number_of_desktop_visits
        ,ena.number_of_diamond_searches
        ,ena.number_of_mobile_visits
        ,ena.number_of_webchats
        ,ena.page_visit_count_distinct
        ,bea.primary_currency_code
      ,ena.product_array_all_byo
      ,ena.product_array_all_preset
      ,ena.product_array_all_ld
      ,ena.product_array_all_engagement
        ,ena.product_browse_distinct_byo
        ,ena.product_browse_distinct_preset
        ,ena.product_browse_distinct_ld
        ,ena.product_browse_distinct_engagement
        ,ena.product_browse_all_byo
        ,ena.product_browse_all_preset
        ,ena.product_browse_all_ld
        ,ena.product_browse_all_engagement
        ,ena.showroom_page_visit
      ,ena.total_items_add_to_cart_byo
      ,ena.total_items_add_to_cart_engagement
      ,ena.total_items_add_to_cart_ld
      ,ena.total_items_add_to_cart_preset
        ,ena.add_to_basket_byo
        ,ena.add_to_basket_ld
        ,ena.add_to_basket_preset 
    FROM (
        SELECT *
            ,first_value(Browse_byo_first_dt_month) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_byo_first_dt_month DESC
                ) AS browse_month_first_dt_byo
            ,first_value(Browse_byo_last_dt_month) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_byo_last_dt_month DESC
                ) AS browse_month_last_dt_byo
            ,first_value(Browse_byo_first_dt_year) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_byo_first_dt_year DESC
                ) AS browse_year_first_dt_byo
            ,first_value(Browse_byo_last_dt_year) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_byo_last_dt_year DESC
                ) AS browse_year_last_dt_byo
            ,first_value(Browse_preset_first_dt_month) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_preset_first_dt_month DESC
                ) AS browse_month_first_dt_preset
            ,first_value(Browse_preset_last_dt_month) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_preset_last_dt_month DESC
                ) AS browse_month_last_dt_preset
            ,first_value(Browse_preset_first_dt_year) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_preset_first_dt_year DESC
                ) AS browse_year_first_dt_preset
            ,first_value(Browse_preset_last_dt_year) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_preset_last_dt_year DESC
                ) AS browse_year_last_dt_preset
            ,first_value(Browse_ld_first_dt_month) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_ld_first_dt_month DESC
                ) AS browse_month_first_dt_ld
            ,first_value(Browse_ld_last_dt_month) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_ld_last_dt_month DESC
                ) AS browse_month_last_dt_ld
            ,first_value(Browse_ld_first_dt_year) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_ld_first_dt_year DESC
                ) AS browse_year_first_dt_ld
            ,first_value(Browse_ld_last_dt_year) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_ld_last_dt_year DESC
                ) AS browse_year_last_dt_ld
            ,first_value(Browse_byo) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_byo DESC
                ) AS browse_count_byo
            ,first_value(Browse_preset) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_preset DESC
                ) AS browse_count_preset
            ,first_value(Browse_ld) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_ld DESC
                ) AS browse_count_ld
            ,first_value(phone_visit) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY phone_visit DESC
                ) AS number_of_mobile_visits
            ,first_value(desktop_visit) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY desktop_visit DESC
                ) AS number_of_desktop_visits
            ,first_value(diamond_search_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY diamond_search_count DESC
                ) AS number_of_diamond_searches
            ,first_value(add_to_cart_byo) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY add_to_cart_byo DESC
                ) AS add_to_basket_byo
            ,first_value(add_to_cart_preset) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY add_to_cart_preset DESC
                ) AS add_to_basket_preset
            ,first_value(add_to_cart_ld) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY add_to_cart_ld DESC
                ) AS add_to_basket_ld
            ,time_diff(byo_max_time_log, byo_min_time_log, second) byo_dwell_time_seconds
            ,time_diff(preset_max_time_log, preset_min_time_log, second) preset_dwell_time_seconds
            ,time_diff(ld_max_time_log, ld_min_time_log, second) ld_dwell_time_seconds
            ,time_diff(eng_max_time_log, eng_min_time_log, second) eng_dwell_time_seconds
            ,first_value(byo_diamond_detail_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY byo_diamond_detail_page_count DESC
                ) AS detail_page_view_diamond_byo
        ,first_value(ld_diamond_detail_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY ld_diamond_detail_page_count DESC
                ) AS detail_page_view_diamond_ld
            ,first_value(byo_diamond_catalog_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY byo_diamond_catalog_page_count DESC
                ) AS catalog_page_view_diamond_byo
        ,first_value(ld_diamond_catalog_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY ld_diamond_catalog_page_count DESC
                ) AS catalog_page_view_diamond_ld
            ,first_value(byo_setting_catalog_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY byo_setting_catalog_page_count DESC
                ) AS catalog_page_view_setting_byo
            ,first_value(preset_setting_catalog_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY preset_setting_catalog_page_count DESC
                ) AS catalog_page_view_setting_preset
            ,first_value(byo_setting_detail_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY byo_setting_detail_page_count DESC
                ) AS detail_page_view_setting_byo
            ,first_value(preset_setting_detail_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY preset_setting_detail_page_count DESC
                ) AS detail_page_view_setting_preset
            ,first_value(byo_build_unit_review_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY byo_build_unit_review_count DESC
                ) AS BUR_view_byo
            ,first_value(webchats_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY webchats_count DESC
                ) AS number_of_webchats
            ,first_value(showroom_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY showroom_page_count DESC
                ) AS showroom_page_visit
            ,first_value(eng_education_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY eng_education_page_count DESC
                ) AS education_page_view_engagement
            ,first_value(diamond_education_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY diamond_education_page_count DESC
                ) AS education_page_view_diamond
            ,first_value(brand_promise_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY brand_promise_page_count DESC
                ) brand_promise_page_view
            ,first_value(customer_first_order_date) OVER (
                PARTITION BY guid_key ORDER BY customer_first_order_date DESC
                ) first_order_date
            ,first_value(customer_last_order_date) OVER (
                PARTITION BY guid_key ORDER BY customer_last_order_date DESC
                ) last_order_date
            ,first_value(byo_product_count_distinct) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY byo_product_count_distinct DESC
                ) AS product_browse_distinct_byo
            ,first_value(preset_product_count_distinct) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY preset_product_count_distinct DESC
                ) AS product_browse_distinct_preset
            ,first_value(ld_product_count_distinct) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY ld_product_count_distinct DESC
                ) AS product_browse_distinct_ld
            ,first_value(eng_product_count_distinct) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY eng_product_count_distinct DESC
                ) AS product_browse_distinct_engagement
            ,first_value(byo_product_count_all) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY byo_product_count_all DESC
                ) AS product_browse_all_byo
            ,first_value(preset_product_count_all) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY preset_product_count_all DESC
                ) AS product_browse_all_preset
            ,first_value(ld_product_count_all) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY ld_product_count_all DESC
                ) AS product_browse_all_ld
            ,first_value(eng_product_count_all) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY eng_product_count_all DESC
                ) AS product_browse_all_engagement
        ,first_value(byo_product_array_all ) over(partition by guid_key,date_key order by byo_product_array_all desc) as product_array_all_byo
      ,first_value(preset_product_array_all ) over(partition by guid_key,date_key order by preset_product_array_all desc) product_array_all_preset
      ,first_value(ld_product_array_all ) over(partition by guid_key,date_key order by ld_product_array_all desc) as product_array_all_ld
      ,first_value(eng_product_array_all ) over(partition by guid_key,date_key order by eng_product_array_all desc) as product_array_all_engagement
        FROM browse_data
        ) ena  LEFT JOIN `{project_name}.{dataset_name3}.bn_customer_profile` bcp
       on ena.guid_key = bcp.bnid
	   left join `{project_name}.{dataset_name3}.bn_explicit_attributes` bea
	   on ena.guid_key = bea.bnid
     WHERE engagement_flag = 1 """.format(
        project_name, dataset_name, dataset_name2, dataset_name3, dataset_name_dssprod, delta_date
    )
    return bql
