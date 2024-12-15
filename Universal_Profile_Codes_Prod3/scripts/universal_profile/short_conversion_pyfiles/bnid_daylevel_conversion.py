def bnid_daylevel_conversion(params):
    project_name=params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name2 = params["dataset_name2"]
    dataset_name3 = params["dataset_name3"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]
    
    bql = f'''WITH customer_base
    AS (
        SELECT distinct
         bn.bnid as guid_key,
        cf.guid_key as guidkey
            ,cf.date_key
            ,cf.PAGE_KEY
        ,cast(cf.offer_id AS string) offer_id
            ,pgd.PRIMARY_BROWSE_SEGMENT
            ,pgd.SECONDARY_BROWSE_SEGMENT
            ,pgd.ACTIVITY_SET_CODE
            ,dtd.DEVICE_TYPE
            ,ud.URI
            ,pgd.PAGE_GROUP_NAME
            ,pof.guid_key AS product_order_guid_key
            ,pof.customer_submit_date_key
            ,parse_time('%T', cast(DISPLAY_TIME_24_HOUR_FORMAT as string)) time_log
            ,extract(year FROM parse_date('%Y%m%d', cast(date_key AS string))) year
            ,extract(month FROM parse_date('%Y%m%d', cast(date_key AS string))) month
        ,parse_date('%Y%m%d', cast(date_key AS string)) as date
        ,parse_date('%Y%m%d', cast(customer_submit_date_key AS string)) as submit_date
        ,pof.customer_submit_date_key as submit_date_key
        ,pof.offer_key as product_offer_key
      ,pof.MERCH_CATEGORY_ROLLUP
        FROM (select * from `{project_name}.{dataset_name_dssprod}.click_fact`  where parse_date('%Y%m%d', cast(date_key AS string)) between DATE_SUB(parse_date('%Y%m%d', cast({delta_date} AS string)),interval 90 day ) and parse_date('%Y%m%d', cast({delta_date} AS string)) ) cf
        join `{project_name}.{dataset_name_dssprod}.email_guid` egl
                ON cf.guid_key = egl.guid_key
         inner  join  `{project_name}.{dataset_name2}.bn_guid_all` bn
          on egl.guid = bn.guid
            LEFT JOIN `{project_name}.{dataset_name_dssprod}.page_group_dim` pgd
                ON cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
            LEFT JOIN `{project_name}.{dataset_name_dssprod}.device_type_dim` dtd
                ON cf.Device_TYPE_KEY = dtd.DEVICE_TYPE_KEY
            LEFT JOIN `{project_name}.{dataset_name_dssprod}.time_dim` td
                ON cf.TIME_KEY = td.TIME_KEY
            LEFT JOIN `{project_name}.{dataset_name_dssprod}.uri_dim` ud
                ON cf.URI_KEY = ud.URI_KEY
            LEFT JOIN 
        (select distinct guid_key,customer_submit_date_key, offer_key,pd.MERCH_CATEGORY_ROLLUP from  `{project_name}.{dataset_name_dssprod}.product_order_fact` po
        INNER JOIN  `{project_name}.{dataset_name_dssprod}.product_dim` pd
        on po.product_key = pd.product_key ) pof
                ON cf.guid_key = pof.guid_key
                    AND cf.date_key= pof.customer_submit_date_key
            and cf.offer_key = pof.offer_key
       where bnid in (Select distinct bnid from `{project_name}.{dataset_name3}.bn_pj_tagging` where Bucket in ('SC','J4L','PPA'))
        ),browse_data
    AS (
        SELECT *
      ,CASE 
                WHEN dj_flag = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,dj_flag
                            )   
                ELSE 0
                END AS browse_dj
          ,CASE 
                 WHEN oj_flag = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,oj_flag
                            )
                ELSE 0
                END AS browse_oj
          ,CASE 
                WHEN wedding_flag = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,wedding_flag
                            )
                ELSE 0
                END AS browse_wedding
          ,CASE 
                WHEN band_matcher_wedding_flag = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,band_matcher_wedding_flag
                            )
                ELSE 0
                END AS Browse_band_matcher_wedding
          ,CASE 
                WHEN band_matcher_catalog_flag = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,band_matcher_catalog_flag
                            )
                ELSE 0
                END AS Browse_band_matcher_catalog
          ,CASE 
                WHEN band_matcher_detail_flag = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,band_matcher_detail_flag
                            )
                ELSE 0
                END AS Browse_band_matcher_detail
            ,time_diff(max_time_log, min_time_log, second) dwell_time_seconds_site
            ,CASE 
                WHEN band_matcher_wedding_flag = 1
                    THEN min(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,band_matcher_wedding_flag
                            )
                END AS band_matcher_wedding_min_time_log
          ,CASE 
                WHEN band_matcher_wedding_flag = 1
                    THEN max(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,band_matcher_wedding_flag
                            )
                END AS band_matcher_wedding_max_time_log
            ,CASE 
                WHEN Wedding_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,Wedding_flag
                            )
                END AS browse_month_first_dt_wedding
            ,CASE 
                WHEN Wedding_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,Wedding_flag
                            )
                END AS browse_month_last_dt_wedding
            ,CASE 
                WHEN Wedding_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,Wedding_flag
                            )
                END AS browse_year_first_dt_wedding
            ,CASE 
                WHEN wedding_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,wedding_flag
                            )
                END AS browse_year_last_dt_wedding
          ,CASE 
                WHEN oj_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,oj_flag
                            )
                END AS browse_month_first_dt_oj
            ,CASE 
                WHEN oj_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,oj_flag
                            )
                END AS browse_month_last_dt_oj
            ,CASE 
                WHEN oj_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,oj_flag
                            )
                END AS browse_year_first_dt_oj
            ,CASE 
                WHEN oj_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,oj_flag
                            )
                END AS browse_year_last_dt_oj
          ,CASE 
                WHEN dj_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,dj_flag
                            )
                END AS browse_month_first_dt_dj
            ,CASE 
                WHEN dj_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,dj_flag
                            )
                END AS browse_month_last_dt_dj
            ,CASE 
                WHEN dj_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,dj_flag
                            )
                END AS browse_year_first_dt_dj
            ,CASE 
                WHEN dj_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,dj_flag
                            )
                END AS browse_year_last_dt_dj
          ,CASE 
                WHEN wedding_flag = 1
                    THEN min(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,wedding_flag
                            )
                END AS wedding_min_time_log
          ,CASE 
                WHEN wedding_flag = 1
                    THEN max(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,wedding_flag
                            )
                END AS wedding_max_time_log
          ,CASE 
                WHEN oj_flag = 1
                    THEN min(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,oj_flag
                            )
                END AS oj_min_time_log
          ,CASE 
                WHEN oj_flag = 1
                    THEN max(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,oj_flag
                            )
                END AS oj_max_time_log
          ,CASE 
                WHEN dj_flag = 1
                    THEN min(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,dj_flag
                            )
                END AS dj_min_time_log
          ,CASE 
                WHEN dj_flag = 1
                    THEN max(time_log) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,dj_flag
                            )
                END AS dj_max_time_log
          ,CASE 
                WHEN band_matcher_wedding_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,band_matcher_wedding_flag
                            )
                END AS Browse_band_matcher_first_dt_month_wedding
          ,CASE 
                WHEN band_matcher_wedding_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,month
                            ,band_matcher_wedding_flag
                            )
                END AS Browse_band_matcher_last_dt_month_wedding
          ,CASE 
                WHEN band_matcher_wedding_flag = 1
                    THEN min(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,band_matcher_wedding_flag
                            )
                END AS Browse_band_matcher_first_dt_year_wedding
          ,CASE 
                WHEN band_matcher_wedding_flag = 1
                    THEN max(date_key) OVER (
                            PARTITION BY guid_key
                            ,year
                            ,band_matcher_wedding_flag
                            )
                END AS Browse_band_matcher_last_dt_year_wedding
            ,count(guid_key) OVER (
                PARTITION BY guid_key
                ,date_key
                ) AS hit_per_day_site
            ,count(DISTINCT page_key) OVER (
                PARTITION BY guid_key
                ,date_key
                ) AS page_visit_count_distinct
            ,CASE 
                WHEN phone_flag_wedding = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,phone_flag_wedding
                            )
                ELSE 0
                END AS phone_visit_wedding
            ,CASE 
                WHEN phone_flag_dj = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,phone_flag_dj
                            )
                ELSE 0
                END AS phone_visit_dj
            ,CASE 
                WHEN phone_flag_oj = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,phone_flag_oj
                            )
                ELSE 0
                END AS phone_visit_oj
            ,CASE 
                WHEN desktop_flag_oj = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,desktop_flag_oj
                            )
                ELSE 0
                END AS desktop_visit_oj
            ,CASE 
                WHEN desktop_flag_dj = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,desktop_flag_dj
                            )
                ELSE 0
                END AS desktop_visit_dj
            ,CASE 
                WHEN desktop_flag_wedding = 1
                    THEN count(guid_key) OVER (
                            PARTITION BY guid_key
                            ,date_key
                            ,desktop_flag_wedding
                            )
                ELSE 0
                END AS desktop_visit_wedding
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
                END AS showroom_page_count,
          CASE 
                    WHEN  wedding_flag = 1
                        THEN count(DISTINCT offer_id) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_flag
                                )
                    ELSE 0
                    END AS wedding_product_count_distinct,
                    
    CASE 
                    WHEN  dj_flag = 1
                        THEN count(DISTINCT offer_id) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,dj_flag
                                )
                    ELSE 0
                    END AS jewllery_product_count_distinct,
                    
    CASE 
                    WHEN  oj_flag = 1
                        THEN count(DISTINCT offer_id) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,oj_flag
                                )
                    ELSE 0
                    END AS other_jewllery_product_count_distinct,
    CASE 
                    WHEN  wedding_flag = 1
                        THEN count(offer_id) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_flag
                                )
                    ELSE 0
                    END AS wedding_product_all_count,				
    CASE 
                    WHEN  dj_flag = 1
                        THEN count(offer_id) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,dj_flag
                                )
                    ELSE 0
                    END AS jewllery_flag_all_count,
    CASE 
                    WHEN  oj_flag = 1
                        THEN count(offer_id) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,oj_flag
                                )
                    ELSE 0
                    END AS other_jewllery_flag_all_count
     ,CASE 
                    WHEN diamond_jewellery_detail_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,diamond_jewellery_detail_flag
                                )
                    ELSE 0
                    END AS diamond_jewellery_detail_page_count    
      ,CASE 
                    WHEN diamond_jewellery_catalog_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,diamond_jewellery_catalog_flag
                                )
                    ELSE 0
                    END AS diamond_jewellery_catalog_page_count 
          ,CASE 
                    WHEN other_jewellery_detail_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,other_jewellery_detail_flag
                                )
                    ELSE 0
                    END AS other_jewellery_detail_page_count 
        ,CASE 
                    WHEN other_jewellery_catalog_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,other_jewellery_catalog_flag
                                )
                    ELSE 0
                    END AS other_jewellery_catalog_page_count 
        ,CASE 
                    WHEN wedding_band_detail_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_band_detail_flag
                                )
                    ELSE 0
                    END AS wedding_band_detail_page_count
            ,CASE 
                    WHEN wedding_band_catalog_flag   = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_band_catalog_flag  
                                )
                    ELSE 0
                    END AS wedding_band_catalog_page_count
            ,CASE 
                    WHEN wedding_cart_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_cart_flag 
                                )
                    ELSE 0
                    END AS wedding_add_to_cart
            ,CASE 
                    WHEN oj_cart_flag = 1
                        THEN count( guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,oj_cart_flag 
                                )
                    ELSE 0
                    END AS oj_add_to_cart
            ,CASE 
                    WHEN dj_cart_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,dj_cart_flag 
                                )
                    ELSE 0
                    END AS dj_add_to_cart
            ,CASE 
                    WHEN wedding_search_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_search_flag
                                )
                    ELSE 0
                    END AS wedding_search_count
            ,CASE 
                    WHEN dj_search_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,dj_search_flag
                                )
                    ELSE 0
                    END AS dj_search_count
            ,CASE 
                    WHEN dj_review_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,dj_review_flag
                                )
                    ELSE 0
                    END AS dj_review_count
            ,CASE 
                    WHEN wedding_review_flag = 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_review_flag
                                )
                    ELSE 0
                    END AS wedding_review_count

            ,CASE 
                    WHEN wedding_education_flag= 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,wedding_education_flag
                                )
                    ELSE 0
                    END AS wedding_education_page_count
            ,CASE 
                    WHEN oj_education_flag= 1
                        THEN count(guid_key) OVER (
                                PARTITION BY guid_key
                                ,date_key
                                ,oj_education_flag
                                )
                    ELSE 0
                    END AS oj_education_page_count   


        ,max(purchase_flag)over(partition by guid_key,date_key)as purchase_flag1
        ,case
            when dj_flag = 1 
              then string_agg(offer_id ) over(partition by guid_key,date_key,dj_flag)
              
            end as dj_product_array_all
          ,case
            when oj_flag = 1
              then string_agg( offer_id ) over(partition by guid_key,date_key,oj_flag)
             
            end as oj_product_array_all
          ,case
            when wedding_flag = 1
              then string_agg( offer_id ) over(partition by guid_key,date_key,wedding_flag)
             
            end as  wedding_product_array_all
        FROM (
            SELECT *
                ,CASE 
                    WHEN (
                            (
                                lower(primary_browse_segment) = 'wedding_band'
                                )
                            )
                        THEN 1
                    ELSE 0
                    END AS wedding_flag
                ,CASE 
                    WHEN (
                            lower(primary_browse_segment) IN ('other_jewelry','misc_jewelry')
                            )
                        THEN 1
                    ELSE 0
                    END AS oj_flag
            ,CASE 
                    WHEN (
                            lower(primary_browse_segment) IN ('diamond_jewelry')
                            )
                        THEN 1
                    ELSE 0
                    END AS dj_flag
            ,CASE
            WHEN (
                            lower(primary_browse_segment) = 'wedding_band'
                            AND lower(secondary_browse_segment) IN ( 'band_matcher')
                            )
                        THEN 1
                    ELSE 0
                    END AS band_matcher_wedding_flag
            ,CASE
            WHEN (
                            lower(primary_browse_segment) = 'wedding_band'
                            AND lower(secondary_browse_segment) IN ( 'band_matcher')
                AND (lower(page_group_name) like '%detail%' or activity_set_code like '%detail%')
                            )
                        THEN 1
                    ELSE 0
                    END AS band_matcher_detail_flag
            ,CASE 
                WHEN (
                            lower(primary_browse_segment) = 'wedding_band'
                            AND lower(secondary_browse_segment) IN ( 'band_matcher')
                AND (lower(page_group_name) like '%catalog%' or activity_set_code like '%catalog%')
                            )
                        THEN 1
                    ELSE 0
                    END AS band_matcher_catalog_flag
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
                    WHEN uri LIKE '%/service%'
                     and lower(primary_browse_segment) = 'wedding_band' 
                        THEN 1
                    ELSE 0
                    END AS brand_promise_page_flag_wedding
                ,CASE 
                    WHEN uri LIKE '%/service%'
                     and lower(primary_browse_segment) = 'diamond_jewelry' 
                        THEN 1
                    ELSE 0
                    END AS brand_promise_page_flag_dj
                ,CASE 
                    WHEN uri LIKE '%/service%'
                     and lower(primary_browse_segment) IN ('other_jewelry','misc_jewelry')
                        THEN 1
                    ELSE 0
                    END AS brand_promise_page_flag_oj
                    

                ,CASE 
                    WHEN device_type = 'phone'
                     and lower(primary_browse_segment) = 'wedding_band' 
                        THEN 1
                    ELSE 0
                    END AS phone_flag_wedding
                ,CASE 
                    WHEN device_type = 'phone'
                     and lower(primary_browse_segment) IN ('other_jewelry','misc_jewelry') 
                        THEN 1
                    ELSE 0
                    END AS phone_flag_oj
                ,CASE 
                    WHEN device_type = 'phone'
                     and lower(primary_browse_segment) IN ('diamond_jewelry')
                        THEN 1
                    ELSE 0
                    END AS phone_flag_dj
                ,CASE 
                    WHEN device_type = 'desktop'
                        THEN 1
                    ELSE 0
                    END AS desktop_flag
                ,CASE 
                    WHEN device_type = 'desktop'
                    and lower(primary_browse_segment) = 'wedding_band' 
                        THEN 1
                    ELSE 0
                    END AS desktop_flag_wedding
                ,CASE 
                    WHEN device_type = 'desktop'
                    and lower(primary_browse_segment) IN ('other_jewelry','misc_jewelry')
                        THEN 1
                    ELSE 0
                    END AS desktop_flag_oj
                ,CASE 
                    WHEN device_type = 'desktop'
                    and lower(primary_browse_segment) IN ('diamond_jewelry')
                        THEN 1
                    ELSE 0
                    END AS desktop_flag_dj
         ,CASE 
                        WHEN lower(activity_set_code) in('diamond jewelry detail','setting detail','diamond detail')
                            AND	lower(primary_browse_segment) = 'diamond_jewelry'
                            THEN 1
                            ELSE 0
                        END AS diamond_jewellery_detail_flag
          ,CASE 
                        WHEN lower(activity_set_code) in('setting catalog','catalog')
                            AND	lower(primary_browse_segment) = 'diamond_jewelry'
                            THEN 1
                            ELSE 0
                        END AS diamond_jewellery_catalog_flag
             ,CASE 
                        WHEN lower(activity_set_code) in('other jewelry detail')
                            AND	lower(primary_browse_segment) IN ('other_jewelry','misc_jewelry')	
                            THEN 1
                            ELSE 0
                        END AS other_jewellery_detail_flag
            ,CASE 
                        WHEN lower(activity_set_code) in('catalog')
                            AND	lower(primary_browse_segment) IN  ('other_jewelry','misc_jewelry')	
                            THEN 1
                            ELSE 0
                        END AS other_jewellery_catalog_flag
              ,CASE 
                        WHEN lower(activity_set_code) in('setting detail','diamond detail','band detail')
                            AND	lower(primary_browse_segment) = 'wedding_band'	
                            THEN 1 
                            ELSE 0
                        END AS wedding_band_detail_flag
              ,CASE 
                        WHEN (lower(activity_set_code) in('catalog','setting catalog')  or lower(page_group_name) ='band matcher catalog')
                            AND	lower(primary_browse_segment) = 'wedding_band'	
                            THEN 1 
                            ELSE 0
                        END AS wedding_band_catalog_flag

       ,CASE 
                        WHEN (lower(primary_browse_segment) = 'education'
                            AND uri like '%wedding%')
                or
                (	lower(primary_browse_segment) IN ('wedding_band') and page_group_name like '%learn%')
                            THEN 1
                ELSE 0
                END wedding_education_flag
        ,CASE 
                        WHEN lower(primary_browse_segment) = 'education'
                            AND lower(secondary_browse_segment) = 'other_jewelry'
                            THEN 1
                        ELSE 0
                        END AS oj_education_flag
              
           ,CASE 
                        WHEN (
                                lower(primary_browse_segment) = 'wedding_band'
                                
                                )
                            AND lower(activity_set_code) IN ('add item to basket', 'add diamond to basket')
                            THEN 1
                        ELSE 0
                        END AS wedding_cart_flag
             ,CASE 
                        WHEN (
                                lower(primary_browse_segment) IN ('other_jewelry','misc_jewelry')
                                
                                )
                            AND lower(activity_set_code) IN ('add item to basket')
                            THEN 1
                        ELSE 0
                        END AS oj_cart_flag
             ,CASE 
                        WHEN (
                                lower(primary_browse_segment) ='diamond_jewelry'
                                
                                )
                            AND lower(activity_set_code) IN ('add item to basket','add diamond to basket')
                            THEN 1
                        ELSE 0
                        END AS dj_cart_flag
              ,CASE 
                        WHEN lower(activity_set_code) = 'diamond search'
                            AND (
                                    lower(primary_browse_Segment) = 'wedding_band'
                                    )
                            THEN 1
                        ELSE 0
                        END AS wedding_search_flag
              ,CASE 
                        WHEN lower(activity_set_code) = 'diamond search'
                            AND (
                                    lower(primary_browse_Segment) = 'diamond_jewelry'
                                    )
                            THEN 1
                        ELSE 0
                        END AS dj_search_flag
              ,CASE 
                        WHEN lower(activity_set_code) = 'review'
                            AND (
                                    lower(primary_browse_Segment) = 'diamond_jewelry'
                                    )
                            THEN 1
                        ELSE 0
                        END AS dj_review_flag
              ,CASE 
                        WHEN lower(activity_set_code) = 'review'
                            AND (
                                    lower(primary_browse_Segment) = 'wedding_band'
                                    )
                            THEN 1
                        ELSE 0
                        END AS wedding_review_flag
            , case 
                        when primary_browse_segment IN ('diamond_jewelry')
                        THEN 1
                        ELSE 0
                        END as dj_order_flag
         ,case when 
         primary_browse_segment='wedding_band'

      THEN 1
                        ELSE 0
                        END as wedding_order_flag
          ,case when
                        primary_browse_segment IN ('other_jewelry','misc_jewelry')

              THEN 1
                        ELSE 0
                        END as oj_order_flag

        ,case when  
            MERCH_CATEGORY_ROLLUP in('Bands','Other Jewelry','Diamond Jewelry')
          and  submit_date_key is not null

                THEN 1
                    ELSE 0
                    END as purchase_flag

         FROM customer_base
            )
        )

    select  distinct a.guid_key, date_key,
     ifnull(browse_count_wedding,0) as browse_count_wedding,
     ifnull(browse_count_dj,0) as browse_count_dj,
    ifnull( browse_count_oj,0) as browse_count_oj
      ,browse_month_first_date_weddding
        ,browse_month_last_date_wedding
        ,browse_year_first_date_Wedding
        ,browse_year_last_date_Wedding
        ,browse_month_first_oj
      ,browse_month_last_oj
      ,browse_year_first_oj
      ,browse_year_last_oj
      ,browse_month_first_dj
      ,browse_month_last_dj
      ,browse_year_first_dj
      ,browse_year_last_dj
      ,ifnull(product_browse_count_distinct_wedding,0) as product_Browse_count_distinct_wedding 
      ,ifnull(product_browse_count_distinct_dj,0) as product_Browse_count_distinct_dj
      ,ifnull(product_browse_count_distinct_oj,0) as product_Browse_count_distinct_oj
      ,ifnull(product_browse_all_count_wedding,0) as product_browse_all_count_wedding
      ,ifnull(product_browse_all_count_dj,0) as product_browse_all_count_dj
      ,ifnull(product_browse_all_count_oj,0) as product_browse_all_count_oj
      ,ifnull(detail_page_view_band_wedding,0) as detail_page_view_wedding
      ,ifnull(detail_page_view_oj,0) as detail_page_view_oj
      ,ifnull(detail_page_view_dj,0) as detail_page_view_dj
      ,ifnull(catalog_page_view_band_wedding,0) as catalog_page_view_wedding
      ,ifnull(catalog_page_view_oj,0) as catalog_page_view_oj
      ,ifnull(catalog_page_view_dj,0) as catalog_page_view_dj
      ,derived_gender
      ,ifnull(dwell_time_seconds_oj,0) as dwell_time_seconds_oj
      ,ifnull(dwell_time_seconds_dj,0) as dwell_time_seconds_dj
      ,ifnull(dwell_time_seconds_band_wedding,0) as dwell_time_seconds_wedding
      ,ifnull(dwell_time_seconds_site,0) as dwell_time_seconds_site
      ,ifnull(hit_per_day_site,0) as hit_per_day_site
      ,MARITAL_STATUS
      ,ifnull(number_of_desktop_visit_dj,0) as number_of_desktop_visit_dj
        ,ifnull(number_of_desktop_visit_oj,0) as number_of_desktop_visit_oj
        , ifnull(number_of_desktop_visit_wedding,0) as number_of_desktop_visit_wedding
        , ifnull(number_of_mobile_visits_dj,0) as number_of_mobile_visits_dj
        ,ifnull(number_of_mobile_visits_oj,0) as number_of_mobile_visits_oj
        ,ifnull(number_of_mobile_visits_wedding,0) as number_of_mobile_visits_wedding
        ,ifnull(number_of_webchats,0) as number_of_webchats
        ,ifnull(page_visit_count_distinct,0) as page_visit_count_distinct
        ,primary_currency_code
        ,ifnull(showroom_page_visit,0) as showroom_page_visit
        ,ifnull(add_to_basket_dj,0) as add_to_basket_dj
      ,ifnull(search_count_number_wedding,0) as search_count_number_wedding
      ,ifnull(search_count_number_dj,0) as search_count_number_dj
      ,ifnull(review_count_number_dj,0) as review_count_number_dj
      ,ifnull(review_count_number_wedding,0) as review_count_number_wedding
      ,ifnull(education_page_count_num_wedding,0) as education_page_count_num_wedding
      ,ifnull(education_page_count_num_oj,0) as education_page_count_num_oj
      ,ifnull(add_to_basket_wedding,0) as add_to_basket_wedding
      ,ifnull(add_to_cart_oj,0) as add_to_basket_oj
    ,product_array_all_dj
    , product_array_all_oj
    ,product_array_all_wedding
    ,ifnull(search_count_number_oj,0) as search_count_number_oj
    ,ifnull(review_count_number_oj,0) as review_count_number_oj 
    ,ifnull(education_page_count_num_dj,0) as education_page_count_num_dj
    , case when date_key >=submit_dt_key  then 1 else  0 end as previous_orders_flag,
     first_value(purchase_flag_oj) OVER (
                PARTITION BY coalesce (a.guid_key)
                ,date_key ORDER BY purchase_flag_oj DESC
                ) AS purchase_flag_oj, 
          first_value(purchase_flag_dj) OVER (
                PARTITION BY a.guid_key
                ,date_key ORDER BY purchase_flag_dj DESC
                ) AS purchase_flag_dj,
          first_value(purchase_flag_wedding) OVER (
                PARTITION BY a.guid_key
                ,date_key ORDER BY purchase_flag_wedding DESC
                ) AS purchase_flag_wedding,

    from

    (
    select distinct
    COALESCE (a.guid_key,purchase_flag.guid_key) as guid_key,
      COALESCE( a.date_key, purchase_flag.submit_date_oj,submit_date_dj,submit_date_wedding) as date_key,
      browse_count_wedding, browse_count_dj, browse_count_oj,
       browse_month_first_date_weddding
        ,browse_month_last_date_wedding
        ,browse_year_first_date_Wedding
        ,browse_year_last_date_Wedding
        ,browse_month_first_oj
      ,browse_month_last_oj
      ,browse_year_first_oj
      ,browse_year_last_oj
      ,browse_month_first_dj
      ,browse_month_last_dj
      ,browse_year_first_dj
      ,browse_year_last_dj
       ,product_browse_count_distinct_wedding
      ,product_browse_count_distinct_dj
      , product_browse_count_distinct_oj
      ,product_browse_all_count_wedding
      ,product_browse_all_count_dj
      ,product_browse_all_count_oj
      ,detail_page_view_band_wedding
      ,detail_page_view_oj
      ,detail_page_view_dj
      ,catalog_page_view_band_wedding
      ,catalog_page_view_oj
      ,catalog_page_view_dj
       ,first_value(derived_gender) OVER (
                PARTITION BY coalesce (a.guid_key) ORDER BY derived_gender ASC
                ) AS derived_gender
      ,dwell_time_seconds_oj
      ,dwell_time_seconds_dj
      ,dwell_time_seconds_band_wedding
       ,dwell_time_seconds_site
      ,hit_per_day_site
      ,first_value(MARITAL_STATUS) OVER (
                PARTITION BY  (a.guid_key) ORDER BY MARITAL_STATUS DESC
                ) AS MARITAL_STATUS
       ,number_of_desktop_visit_dj
        ,number_of_desktop_visit_oj
        , number_of_desktop_visit_wedding
        , number_of_mobile_visits_dj
        ,number_of_mobile_visits_oj
        ,number_of_mobile_visits_wedding
        ,number_of_webchats
        ,page_visit_count_distinct
      ,first_value(primary_currency_code) OVER (
                PARTITION BY  (a.guid_key) ORDER BY primary_currency_code DESC
                ) AS primary_currency_code
        ,showroom_page_visit
        ,add_to_basket_dj
      ,search_count_number_wedding
      ,search_count_number_dj
      ,review_count_number_dj
      ,review_count_number_wedding
      ,education_page_count_num_wedding
      ,education_page_count_num_oj
      ,add_to_basket_wedding
      ,add_to_cart_oj
    ,product_array_all_dj
    , product_array_all_oj
    ,product_array_all_wedding
    ,0 as search_count_number_oj
    ,0 as review_count_number_oj
    ,0 as education_page_count_num_dj
    ,case when purchase_flag.submit_date_oj is null then 0 else 1 end as purchase_flag_oj
    ,case when purchase_flag.submit_date_dj is null then 0 else 1 end as purchase_flag_dj
    ,case when purchase_flag.submit_date_wedding is null then 0 else 1 end as purchase_flag_wedding
    ,min(coalesce (purchase_flag.submit_date_oj,purchase_flag.submit_date_dj,purchase_flag.submit_date_wedding)) OVER (PARTITION BY COALESCE (a.guid_key)) AS submit_dt_key
     ,a.guidkey
     from

    (SELECT DISTINCT 
       ena.guidkey,
       guid_key,
        ena.date_key
      ,ifnull(ena.browse_count_wedding,0) as browse_count_wedding
        ,ifnull(ena.browse_count_dj,0) as browse_count_dj
      ,ifnull(ena.browse_count_oj,0) as browse_count_oj
      ,ena.browse_month_first_date_weddding
        ,ena.browse_month_last_date_wedding
        ,ena.browse_year_first_date_Wedding
        ,ena.browse_year_last_date_Wedding
        ,ena.browse_month_first_oj
      ,ena.browse_month_last_oj
      ,ena.browse_year_first_oj
      ,ena.browse_year_last_oj
      ,ena.browse_month_first_dj
      ,ena.browse_month_last_dj
      ,ena.browse_year_first_dj
      ,ena.browse_year_last_dj
      ,ifnull (ena.product_count_distinct_wedding, 0) as product_browse_count_distinct_wedding
      ,ifnull(ena.product_count_distinct_dj,0) as product_browse_count_distinct_dj
      ,ifnull(ena.product_count_distinct_oj, 0) as product_browse_count_distinct_oj
      ,ifnull(ena.product_browse_all_count_wedding,0) as product_browse_all_count_wedding
      ,ifnull(ena.product_browse_all_count_dj ,0) as product_browse_all_count_dj
      ,ifnull(ena.product_browse_all_count_oj,0) as product_browse_all_count_oj
      ,ifnull(ena.detail_page_view_band_wedding,0) as detail_page_view_band_wedding
      ,ifnull(ena.detail_page_view_oj,0) as detail_page_view_oj
      ,ifnull(ena.detail_page_view_dj,0) as detail_page_view_dj
      ,ifnull(ena.catalog_page_view_band_wedding,0) as catalog_page_view_band_wedding
      ,ifnull(ena.catalog_page_view_oj,0) as catalog_page_view_oj
      ,ifnull(ena.catalog_page_view_dj,0) as catalog_page_view_dj
      ,ifnull(bcp.derived_gender,'U') as derived_gender
        ,ifnull(first_value(ena.oj_dwell_time_seconds_oj) OVER (
            PARTITION BY guid_key
            ,date_key ORDER BY ena.oj_dwell_time_seconds_oj DESC
            ),0) AS dwell_time_seconds_oj
        ,ifnull(first_value(ena.dj_dwell_time_seconds_dj) OVER (
            PARTITION BY guid_key
            ,date_key ORDER BY ena.dj_dwell_time_seconds_dj DESC
            ),0) AS dwell_time_seconds_dj
        ,ifnull(first_value(ena.band_dwell_time_seconds_wedding) OVER (
            PARTITION BY guid_key
            ,date_key ORDER BY ena.band_dwell_time_seconds_wedding DESC
            ),0) AS dwell_time_seconds_band_wedding
        ,ifnull(ena.dwell_time_seconds_site,0) as dwell_time_seconds_site
        ,ifnull(ena.hit_per_day_site,0) as hit_per_day_site
        ,bea.MARITAL_STATUS
        ,ifnull(ena.number_of_desktop_visit_dj,0) as number_of_desktop_visit_dj
        ,ifnull(ena.number_of_desktop_visit_oj,0) as number_of_desktop_visit_oj
        ,ifnull(ena.number_of_desktop_visit_wedding,0) as number_of_desktop_visit_wedding
        ,ifnull(ena.number_of_mobile_visits_dj,0) as number_of_mobile_visits_dj
        ,ifnull(ena.number_of_mobile_visits_oj,0) as number_of_mobile_visits_oj
        ,ifnull(ena.number_of_mobile_visits_wedding,0) as number_of_mobile_visits_wedding
        ,ifnull(ena.number_of_webchats,0) as number_of_webchats
        ,ifnull(ena.page_visit_count_distinct,0) as page_visit_count_distinct
        ,bea.primary_currency_code
        ,ifnull(ena.showroom_page_visit,0) as showroom_page_visit

      ,ifnull(ena.add_to_basket_dj,0) as add_to_basket_dj
      ,ifnull(ena.search_count_number_wedding,0) as search_count_number_wedding
      ,ifnull(ena.search_count_number_dj,0) as search_count_number_dj
      ,ifnull(ena.review_count_number_dj,0) as review_count_number_dj
      ,ifnull(ena.review_count_number_wedding,0) as review_count_number_wedding
      ,ifnull(ena.education_page_count_num_wedding,0) as education_page_count_num_wedding
      ,ifnull(ena.education_page_count_num_oj,0) as education_page_count_num_oj
      ,ifnull(ena.add_to_basket_wedding_count,0) as add_to_basket_wedding
      ,ifnull(ena.add_to_cart_count_oj,0) as  add_to_cart_oj

    , ena.product_array_all_dj
    , ena.product_array_all_oj
    ,ena.product_array_all_wedding
    ,0 as search_count_number_oj
    ,0 as review_count_number_oj
    ,0 as education_page_count_num_dj

    FROM (
        SELECT *,
       
             first_value( browse_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_wedding DESC
                ) AS browse_count_wedding
            ,first_value( browse_oj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_oj DESC
                ) AS browse_count_oj
         ,first_value( browse_dj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_dj DESC
                ) AS browse_count_dj

          ,first_value(Browse_band_matcher_catalog) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_band_matcher_catalog DESC
                ) AS Browse_count_band_matcher_catalogue_wedding
          ,first_value(Browse_band_matcher_detail) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_band_matcher_detail DESC
                ) AS Browse_count_band_matcher_detail_wedding
          ,first_value(browse_month_first_dt_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_month_first_dt_wedding DESC
                ) AS browse_month_first_date_weddding
          ,first_value(browse_month_last_dt_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_month_last_dt_wedding DESC
                ) AS browse_month_last_date_wedding
          ,first_value(browse_year_first_dt_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_year_first_dt_wedding DESC
                ) AS browse_year_first_date_Wedding
          ,first_value(browse_year_last_dt_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_year_last_dt_wedding DESC
                ) AS browse_year_last_date_Wedding
          ,first_value(browse_month_first_dt_oj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_month_first_dt_oj DESC
                ) AS browse_month_first_oj
          ,first_value(browse_month_last_dt_oj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_month_last_dt_oj DESC
                ) AS browse_month_last_oj
          ,first_value(browse_year_first_dt_oj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_year_first_dt_oj DESC
                ) AS browse_year_first_oj
          ,first_value(browse_year_last_dt_oj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_year_last_dt_oj DESC
                ) AS browse_year_last_oj
          ,first_value(browse_month_first_dt_dj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_month_first_dt_dj DESC
                ) AS browse_month_first_dj
          ,first_value(browse_month_last_dt_dj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_month_last_dt_dj DESC
                ) AS browse_month_last_dj
          ,first_value(browse_year_first_dt_dj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_year_first_dt_dj DESC
                ) AS browse_year_first_dj
          ,first_value(browse_year_last_dt_dj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY browse_year_last_dt_dj DESC
                ) AS browse_year_last_dj
          ,time_diff(wedding_max_time_log, wedding_min_time_log, second) band_dwell_time_seconds_wedding
          ,time_diff(oj_max_time_log, oj_min_time_log, second) oj_dwell_time_seconds_oj
          ,time_diff(dj_max_time_log, dj_min_time_log, second) dj_dwell_time_seconds_dj
          ,time_diff(band_matcher_wedding_max_time_log, band_matcher_wedding_min_time_log, second) 
               band_matcher_dwell_time_seconds_wedding
          ,first_value(Browse_band_matcher_first_dt_month_wedding) 
                 OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_band_matcher_first_dt_month_wedding DESC
                ) AS browse_month_first_dt_band_matcher_wedding
         ,first_value(Browse_band_matcher_last_dt_month_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY Browse_band_matcher_last_dt_month_wedding DESC
                ) AS browse_month_last_dt_band_matcher_wedding
          ,first_value(Browse_band_matcher_first_dt_year_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER By Browse_band_matcher_first_dt_year_wedding DESC
                ) AS browse_year_first_dt_band_matcher_wedding
          ,first_value(Browse_band_matcher_last_dt_year_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER By Browse_band_matcher_last_dt_year_wedding DESC
                ) AS browse_year_last_dt_band_matcher_wedding
            ,first_value(phone_visit_dj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY phone_visit_dj DESC
                ) AS number_of_mobile_visits_dj
            ,first_value(phone_visit_oj) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY phone_visit_oj DESC
                ) AS number_of_mobile_visits_oj
            ,first_value(phone_visit_wedding) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY phone_visit_wedding DESC
                ) AS number_of_mobile_visits_wedding
            ,first_value(desktop_visit_dj) OVER (
                PARTITION BY guid_key 
                                 ,date_key ORDER BY desktop_visit_dj DESC
                ) AS number_of_desktop_visit_dj
            ,first_value(desktop_visit_oj) OVER (
                PARTITION BY guid_key 
                                 ,date_key ORDER BY desktop_visit_oj DESC
                ) AS number_of_desktop_visit_oj
            ,first_value(desktop_visit_wedding) OVER (
                PARTITION BY guid_key 
                                 ,date_key ORDER BY desktop_visit_wedding DESC
                ) AS number_of_desktop_visit_wedding
            ,first_value(webchats_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY webchats_count DESC
                ) AS number_of_webchats
            ,first_value(showroom_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY showroom_page_count DESC
                ) AS showroom_page_visit
        , first_value (wedding_product_count_distinct) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_product_count_distinct DESC
                ) AS product_count_distinct_wedding
         , first_value (jewllery_product_count_distinct) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY jewllery_product_count_distinct DESC
                ) AS product_count_distinct_dj
          , first_value (other_jewllery_product_count_distinct) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY other_jewllery_product_count_distinct DESC
                ) AS product_count_distinct_oj
          , first_value (wedding_product_all_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_product_all_count DESC
                ) AS product_browse_all_count_wedding
          , first_value (jewllery_flag_all_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY jewllery_flag_all_count DESC
                ) AS product_browse_all_count_dj
          , first_value (other_jewllery_flag_all_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY other_jewllery_flag_all_count DESC
                ) AS product_browse_all_count_oj
          ,first_value(wedding_band_detail_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_band_detail_page_count DESC
                ) AS detail_page_view_band_wedding
          ,first_value(wedding_band_catalog_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_band_catalog_page_count DESC
                ) AS catalog_page_view_band_wedding
          ,first_value(other_jewellery_detail_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY other_jewellery_detail_page_count DESC
                ) AS detail_page_view_oj
          ,first_value(other_jewellery_catalog_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY other_jewellery_catalog_page_count DESC
                ) AS catalog_page_view_oj
          ,first_value(diamond_jewellery_detail_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY diamond_jewellery_detail_page_count DESC
                ) AS detail_page_view_dj
          ,first_value(diamond_jewellery_catalog_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY diamond_jewellery_catalog_page_count DESC
                ) AS catalog_page_view_dj
          ,first_value(wedding_add_to_cart) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_add_to_cart DESC
                ) AS add_to_basket_wedding_count
            ,first_value(oj_add_to_cart) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY oj_add_to_cart DESC
                ) AS add_to_cart_count_oj
            ,first_value(dj_add_to_cart) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY dj_add_to_cart DESC
                ) AS add_to_basket_dj
         ,first_value(wedding_search_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_search_count DESC
                ) AS search_count_number_wedding
          ,first_value(dj_search_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY dj_search_count DESC
                ) AS search_count_number_dj
          ,first_value(dj_review_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY dj_review_count DESC
                ) AS review_count_number_dj
          ,first_value(wedding_review_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_review_count DESC
                ) AS review_count_number_wedding
          ,first_value(wedding_education_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY wedding_education_page_count DESC
                ) AS education_page_count_num_wedding
          ,first_value(oj_education_page_count) OVER (
                PARTITION BY guid_key
                ,date_key ORDER BY oj_education_page_count DESC
                ) AS education_page_count_num_oj
     ,first_value(dj_product_array_all ) over (partition by guid_key,date_key order by dj_product_array_all desc) as product_array_all_dj
      ,first_value(oj_product_array_all ) over(partition by guid_key,date_key order by oj_product_array_all  desc) product_array_all_oj
      ,first_value(wedding_product_array_all ) over(partition by guid_key,date_key order by wedding_product_array_all desc) as product_array_all_wedding

        FROM browse_data

        )  as ena
    LEFT JOIN `{project_name}.{dataset_name3}.bn_customer_profile` bcp
       on ena.guid_key = bcp.bnid
	   left join `{project_name}.{dataset_name3}.bn_explicit_attributes` bea
	   on ena.guid_key = bea.bnid)a
      full outer join
      (select distinct pof.guid_key as guidkey,
    bn.bnid as guid_key, 
    case when  MERCH_CATEGORY_ROLLUP in ('Other Jewelry') then pof.CUSTOMER_SUBMIT_DATE_KEY else null end as submit_date_oj,
    case when MERCH_CATEGORY_ROLLUP in ('Diamond Jewelry') then pof.CUSTOMER_SUBMIT_DATE_KEY else null end as submit_date_dj,
    case when MERCH_CATEGORY_ROLLUP in ('Bands') then pof.CUSTOMER_SUBMIT_DATE_KEY else null end as submit_date_wedding
    from `{project_name}.{dataset_name_dssprod}.product_order_fact` pof
    join `{project_name}.{dataset_name_dssprod}.product_dim` pd
    on pof.PRODUCT_KEY = pd.PRODUCT_KEY
    INNER JOIN `{project_name}.{dataset_name_dssprod}.email_guid` gd
        on pof.GUID_KEY = gd.GUID_KEY
    inner  join  `{project_name}.{dataset_name2}.bn_guid_all` bn
    on gd.guid = bn.guid
    where  pd.MERCH_CATEGORY_ROLLUP in ('Other Jewelry','Diamond Jewelry','Bands') and
           pof.CANCEL_DATE_KEY is null and pof.RETURN_ACCEPTED_DATE_KEY is null and parse_date('%Y%m%d', cast(pof.CUSTOMER_SUBMIT_DATE_KEY AS string)) = DATE_SUB(parse_date('%Y%m%d', cast({delta_date} AS string)),interval 1 day )
            and bn.bnid in (Select distinct bnid from `{project_name}.{dataset_name3}.bn_pj_tagging` where Bucket in ('SC','PPA','J4L')))purchase_flag
        on a.guid_key =purchase_flag.guid_key and a.date_key = coalesce(purchase_flag.submit_date_oj,purchase_flag.submit_date_dj,purchase_flag.submit_date_wedding)
      )a

     where (browse_count_wedding >0 or  browse_count_dj >0 or  browse_count_oj>0 or purchase_flag_dj <>0 or purchase_flag_oj <>0 or purchase_flag_oj <>0 or purchase_flag_wedding <>0)
    '''.format(project_name, dataset_name, dataset_name2, dataset_name3, dataset_name_dssprod, delta_date)
    return bql