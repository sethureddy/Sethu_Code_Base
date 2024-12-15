def bn_lc_pj_attributes():
    bql = '''
    ---- Query for bn_lc_pj_attributes ----
    drop table if exists `bnile-cdw-prod.o_customer.bn_lc_pj_attributes`;
    
    CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.o_customer.bn_lc_pj_attributes` as
    
     (
    --- Logic for customer_data it has complete data of customer coming from different sources ---
    
    WITH 
      customer_data AS (
      SELECT distinct
      ead.bnid
      ,cf.date_key,
        LOWER(coalesce(cf.sku1,
            cf.sku2,
            cf.sku3,
            cf.sku4,
            cf.sku5)) AS cf_sku,
        LOWER(ds.sku) AS ds_sku
          ,cf.REQUEST_TYPE 
        ,pgd.PRODUCT_INTENT
        ,CAST(cf.offer_id AS string) offer_id,
         pgd.PRIMARY_BROWSE_SEGMENT,
        pgd.SECONDARY_BROWSE_SEGMENT,
        pgd.ACTIVITY_SET_CODE,
        ud.URI,
        qsd.QUERYSTRING,
        pgd.PAGE_GROUP_NAME,
        parse_time('%T',DISPLAY_TIME_24_HOUR_FORMAT) time_log,
        prd.PRIMARY_METAL_NAME,
        prd.merch_category_rollup,
        lower (Replace(PRIMARY_SETTING_TYPE, 'é','e')) PRIMARY_SETTING_TYPE,
        ds.SHAPE
        ,ds.CARAT
        ,ds.CUT
             FROM
      `bnile-cdw-prod.dssprod_o_warehouse.click_fact`  cf
      INNER JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.email_guid`   egl
      ON
        cf.GUID_KEY = egl.GUID_KEY
      LEFT JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`  pgd
      ON
        cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
      LEFT JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.time_dim` td
      ON
        cf.TIME_KEY = td.TIME_KEY
      LEFT JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.uri_dim`  ud
      ON
        cf.URI_KEY = ud.URI_KEY      	
      LEFT JOIN	
        `bnile-cdw-prod.dssprod_o_warehouse.querystring_dim` qsd
      ON
        cf.QUERYSTRING_KEY=qsd.QUERYSTRING_KEY
       LEFT JOIN
       `bnile-cdw-prod.o_product.offer_attributes_dim`  prd
      ON
        cf.OFFER_KEY = prd.offer_id
        left join
        ( select *except(mode) from (
    Select PRODUCT_OFFER_KEY, PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE
    ,PRODUCT_CLASS_NAME,PRODUCT_SUB_CLASS_NAME,PRODUCT_CATEGORY_TYPE,MERCH_PRODUCT_CATEGORY,MERCH_CATEGORY_ROLLUP,
    row_number() over(partition by product_offer_key  order by count(*) desc) mode
    from `bnile-cdw-prod.dssprod_o_warehouse.product_dim` 
    group by PRODUCT_OFFER_KEY, PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,
    PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,product_class_name,MERCH_CATEGORY_ROLLUP,MERCH_PRODUCT_CATEGORY,PRODUCT_SUB_CLASS_NAME,PRODUCT_CLASS_NAME,PRODUCT_CATEGORY_TYPE )
    where mode = 1)p
    on cf.offer_key = p.PRODUCT_OFFER_KEY
      LEFT JOIN
        `bnile-cdw-prod.o_diamond.diamond_attributes_dim`  ds
      ON
        LOWER(coalesce(cf.sku1,
            cf.sku2,
            cf.sku3,
            cf.sku4,
            cf.sku5))=LOWER(ds.sku) 
         join `bnile-cdw-prod.up_stg_customer.bn_guid_all`  ead
            on egl.guid =ead.guid    
      ),
    
    --- cut_off_date----
      
    cut_off as 
    
    ( select bnid,min (date_key) as cut_off_date
           from customer_data
        where    ( ( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor',
              'byo3sr',
              'preset',
              'general') )
          OR ( LOWER(primary_browse_Segment) = 'diamond'
            AND LOWER(secondary_browse_segment) = 'loose_diamond' ) )  
            and (PARSE_DATE('%Y%m%d',CAST(date_key AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
            group by bnid
           ),
     
      --- Logic for LC base query----
    --- ***Rules***
    --- 1) Last 90 days active
    --- 2) views ENGAGEMENT pages
    --- 3) has not purchased an engagement ring in last 18 months
    
      --- Logic for customer_base who are the customers in purchase journey satisfied the rules ---
      
      customer_base as
      (
          select a.*, c.cut_off_date from customer_data  a
          inner join
          (select distinct * from 
    (select  bnid,min((PARSE_DATE('%Y%m%d',CAST(DATE_KEY AS STRING)))) OVER(PARTITION BY bnid) as pj_qualify_date
    from customer_data  
    where 
          
          --- Rule 1 - Logic for past 90 days active ---
          
         DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y%m%d',
              CAST(DATE_KEY AS STRING))),DAY) <=90
              
        --- Rule 2 - Logic for views engagement pages --  
              
        AND ( ( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor',
              'byo3sr',
              'preset',
              'general') )
          OR ( LOWER(primary_browse_Segment) = 'diamond'
            AND LOWER(secondary_browse_segment) = 'loose_diamond' ) )
            
            --- Rule 3 - Logic for has not purchased engagement ring in past 18 months
            
            AND  bnid  not in
       (
       select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes`
    where parse_DATE('%Y-%m-%d',
          CAST(order_engagement_last_dt AS string)) >= DATE_SUB(CURRENT_DATE (), INTERVAL 18 month)
          and order_engagement_cnt >0
     )
    
     )
    )b
    on a.bnid=b.bnid
    left join
    ---- cut_off date-----
    (
    select  distinct bnid, cut_off_date from cut_off
    )c
    on a.bnid = c.bnid
    where a.date_key >= c.cut_off_date
    ) 
    
    ---  Logic for browsing pages with respective flags and counts ---
          
    ,browse_data AS (
            SELECT *
          ,countif(diamond_search_flag=1) over(partition by bnid) as diamond_search_count
           ,countif( showroom_flag=1) over(partition by bnid) as showroom_page_count
           ,countif( brand_promise_page_flag=1) over(partition by bnid) as brand_promise_page_count
           ,countif( diamond_detail_flag=1) over(partition by bnid) as diamonds_viewed_count
           ,countif( diamonds_selected_for_setting_flag=1) over(partition by bnid) as diamonds_selected_for_setting_count
           ,countif(count_settings_selected_flag=1) over(partition by bnid) as count_settings_selected
               ,countif(count_settings_viewed_flag=1) over(partition by bnid) as count_settings_viewed
               ,countif(byo_build_unit_review_flag=1) over(partition by bnid) as count_built_rings
           ,countif( education_flag=1) over(partition by bnid) as count_education_flag
           ,countif(cart_adds_flag=1) over(partition by bnid) as count_adds_flag
           ,countif(engagement_flag =1) over (partition by bnid) as count_page_views
           ,count(distinct date_key) over (partition by bnid) as count_site_visits
            FROM (
                SELECT *
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
                        WHEN page_group_name LIKE '%showroom%'
                            THEN 1
                        ELSE 0
                        END AS showroom_flag
              
             ,CASE 
                        WHEN uri LIKE '%/service%' and lower(REQUEST_TYPE) = 'page'
                            THEN 1
                        ELSE 0
                        END AS brand_promise_page_flag
    
            ,CASE 
                        WHEN 
             ((lower(activity_set_code) = 'diamond detail'
                            AND	(
                                    lower(primary_browse_segment) = 'engagement'
                                    AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
                                    )) 
                                    or 
                            (lower(activity_set_code) = 'diamond detail'
                            AND (
                                    lower(primary_browse_Segment) = 'diamond'
                                    AND lower(secondary_browse_segment) = 'loose_diamond'
                                    )))
                            THEN 1
                        ELSE 0
                        END AS diamond_detail_flag		
              
            ,CASE 
                        WHEN lower(activity_set_code) IN ('diamond search', 'diamond detail')
                            AND 
                                (
                                    lower(primary_browse_segment) = 'engagement'
                                    AND lower(secondary_browse_segment) IN ('byor', 'byo3sr' )
                                    )	
                                
                            THEN 1
                        ELSE 0
                        END AS diamonds_selected_for_setting_flag 
              
            ,CASE 
                        WHEN lower(primary_browse_segment) = 'engagement'
                            AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
                            AND lower(PAGE_GROUP_NAME) IN ('byor add setting to byo', 'byo3sr add setting to byo')
                            THEN 1
                        ELSE 0
                        END AS count_settings_selected_flag,
            CASE 
                        WHEN lower(primary_browse_segment) = 'engagement'
                            AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
                            AND lower(PAGE_GROUP_NAME) IN ('byo3sr setting detail', 'byor setting detail')
                            THEN 1
                        ELSE 0
                        END AS count_settings_viewed_flag
              
            ,CASE 
                        WHEN starts_with(uri, '/build-your-own-ring/') 
                             AND 
                             (starts_with(querystring, 'offerid') OR starts_with(querystring, 'diamondsku'))
                            THEN 1
                        ELSE 0
                        END AS byo_build_unit_review_flag
              
            ,CASE 
                        WHEN lower(primary_browse_segment) = 'education' and 
                    ( lower(product_intent) = 'engagement')
                            THEN 1
                        ELSE 0
                        END AS education_flag
              
            ,CASE 
                    when( (
                                lower(primary_browse_segment) = 'engagement'
                                AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
                                )
                            AND lower(page_group_name) IN ('byor add to basket', 'byo3sr add to basket')  
                            or 
                            (
                                lower(primary_browse_segment) = 'engagement'
                                AND lower(secondary_browse_segment) IN ('preset')
                                )
                            AND lower(page_group_name) IN ('preset engagement add to basket') 
                            or 
                            (
                                lower(primary_browse_Segment) = 'diamond'
                                AND lower(secondary_browse_segment) = 'loose_diamond'
                                )
                            AND lower(page_group_name) IN ('loose diamond add to basket')) 
                and lower(request_type) = 'service'
                    THEN 1
                        ELSE 0
                        END AS cart_adds_flag,
              
            CASE 
                    when  
              ( ( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor',
              'byo3sr',
              'preset',
              'general') )
          OR ( LOWER(primary_browse_Segment) = 'diamond'
            AND LOWER(secondary_browse_segment) = 'loose_diamond' ) ) 
                then 1
          else 0
          end engagement_flag
          
                FROM customer_base
                )
            )    
        
     select distinct 
     a.bnid
    , e_lc_gender
    ,ifnull(e_lc_count_diamond_searches,0) as e_lc_count_diamond_searches
    ,ifnull(e_lc_count_showroom_page_visits,0) as e_lc_count_showroom_page_visits
    ,ifnull(e_lc_count_brand_promise_page_visits,0) as e_lc_count_brand_promise_page_visits
    ,ifnull(e_lc_count_diamonds_viewed,0) as e_lc_count_diamonds_viewed
    ,ifnull(e_lc_count_diamonds_selected_for_setting,0) as e_lc_count_diamonds_selected_for_setting
    ,ifnull(e_lc_count_settings_selected,0) as e_lc_count_settings_selected
    ,ifnull(e_lc_count_settings_viewed,0) as e_lc_count_settings_viewed
    ,ifnull(e_lc_count_built_rings,0) as e_lc_count_built_rings
    ,ifnull(e_lc_count_education_pages_visits,0) as e_lc_count_education_pages_visits
    ,ifnull(e_lc_count_eng_cart_adds,0) as e_lc_count_eng_cart_adds
    ,e_lc_most_viewed_diamond_OID_1 as e_lc_most_viewed_diamond_sku_1
    ,e_lc_most_viewed_diamond_OID_2 as e_lc_most_viewed_diamond_sku_2
    ,e_lc_most_viewed_setting_OID_1 as e_lc_most_viewed_setting_oid_1
    ,e_lc_most_viewed_setting_OID_2 as e_lc_most_viewed_setting_oid_2
    ,ifnull(e_lc_count_call_center_contacts,0) as e_lc_count_call_center_contacts
    ,e_lc_proposal_date
    ,ifnull(e_lc_free_ring_sizer_order_flag, 'N') as e_lc_free_ring_sizer_order_flag
    , e_lc_free_ring_sizer_order_date
    ,ifnull (e_lc_paid_ring_sizer_order_flag, 'N') as e_lc_paid_ring_sizer_order_flag
    , e_lc_paid_ring_sizer_order_date
    ,ifnull(e_lc_count_items_in_cart,0) as e_lc_count_items_in_cart
    ,e_lc_byo_most_recent_review_setting_OID as e_lc_byo_most_recent_review_setting_oid
    ,e_lc_byo_most_recent_review_LD_OID as e_lc_byo_most_recent_review_ld_sku
    ,e_lc_most_recent_viewed_LD_OID as e_lc_most_recent_viewed_ld_sku
    ,e_lc_most_recent_viewed_OID as e_lc_most_recent_viewed_oid
    ,i_lc_most_viewed_metal_1
    ,i_lc_most_viewed_metal_2
    ,i_lc_most_viewed_diamond_shape_1
    ,i_lc_most_viewed_diamond_shape_2
    ,i_lc_min_diamond_carat
    ,i_lc_max_diamond_carat
    ,i_lc_median_diamond_carat
    ,i_lc_most_viewed_setting_style_1
    ,i_lc_most_viewed_setting_style_2
    , i_lc_most_viewed_diamond_quality
    ,ifnull(e_lc_count_page_views,0) as e_lc_count_page_views
    ,ifnull(e_lc_count_site_visits,0) as e_lc_count_site_visits
    , e_lc_last_site_activity_date
    , i_lc_min_budget
    , i_lc_median_budget
    ,i_lc_max_budget
    ,e_lc_pj_qualify_date
    ,e_lc_last_email_send_date
    ,i_lc_propensity_overall_phase 
    ,i_lc_propensity_run_date 
    ,i_lc_propensity_overall_score
    ,i_lc_previous_pj_phase  
    ,null as e_lc_budget
    ,null as e_lc_wishlisted_setting_oid_1
    ,null as e_lc_wishlist_setting_date_1
    ,null as e_lc_wishlist_setting_oid_2
    ,null as e_lc_wishlist_setting_date_2
    ,null as e_lc_price_drop_flag
    
    
    from
    
     (  
     
     --- Logic for e_lc_count_diamond_searches,e_lc_count_showroom_page_visits,e_lc_count_brand_promise_page_visits,
     --- e_lc_count_diamonds_viewed,e_lc_count_diamonds_selected_for_setting,e_lc_count_settings_selected,
     --- e_lc_count_settings_viewed,e_lc_count_built_rings,e_lc_count_education_pages_visits,e_lc_count_eng_cart_adds ---
     
     (
        SELECT DISTINCT 
          bnid
       ,first_value(diamond_search_count) OVER (PARTITION BY bnid order by diamond_search_count desc )  as e_lc_count_diamond_searches
       ,first_value(showroom_page_count) OVER (PARTITION BY bnid order by showroom_page_count desc )  as e_lc_count_showroom_page_visits
       ,first_value(brand_promise_page_count) OVER (PARTITION BY bnid order by brand_promise_page_count desc )  as e_lc_count_brand_promise_page_visits
       ,first_value(diamonds_viewed_count) OVER (PARTITION BY bnid order by diamonds_viewed_count desc )  as e_lc_count_diamonds_viewed
       ,first_value(diamonds_selected_for_setting_count) OVER (PARTITION BY bnid order by diamonds_selected_for_setting_count desc )  as             e_lc_count_diamonds_selected_for_setting
       ,first_value(count_settings_selected) OVER (PARTITION BY bnid order by count_settings_selected desc )  as e_lc_count_settings_selected
       ,first_value(count_settings_viewed) OVER (PARTITION BY bnid order by count_settings_viewed desc )  as e_lc_count_settings_viewed
     ,first_value(count_built_rings) OVER (PARTITION BY bnid order by count_built_rings desc )  as e_lc_count_built_rings
       ,first_value(count_education_flag) OVER (PARTITION BY bnid order by count_education_flag desc )  as e_lc_count_education_pages_visits
       ,first_value(count_adds_flag) OVER (PARTITION BY bnid order by count_adds_flag desc )  as e_lc_count_eng_cart_adds
       ,first_value(count_page_views) OVER (PARTITION BY bnid order by count_page_views desc )  as e_lc_count_page_views
       ,first_value(count_site_visits) OVER (PARTITION BY bnid order by count_site_visits desc )  as e_lc_count_site_visits
       
         FROM 
       (
        SELECT *	
        FROM browse_data
        ) 
       )a
     
     left join
     
       --- Logic for e_lc_pj_qualify_date ---
       
     (select distinct bnid,min((PARSE_DATE('%Y%m%d',CAST(DATE_KEY AS STRING)))) OVER(PARTITION BY bnid) as e_lc_pj_qualify_date
     from customer_data
      where ( ( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor',
              'byo3sr',
              'preset',
              'general') )
          OR ( LOWER(primary_browse_Segment) = 'diamond'
            AND LOWER(secondary_browse_segment) = 'loose_diamond' ) 
          OR ( LOWER(primary_browse_Segment) = 'education'
            AND LOWER(secondary_browse_segment) IN ( 'engagement' , 'diamond' ) )
                   ) 
     )qualify_date
     on a.bnid = qualify_date.bnid
     
     left join
     
      --- Logic for e_lc_most_viewed_diamond_OID_1,e_lc_most_viewed_diamond_OID_2 ---
      
     (select distinct bnid,max(e_lc_most_viewed_diamond_OID_1)  over(partition by bnid) as e_lc_most_viewed_diamond_OID_1, 
    max(e_lc_most_viewed_diamond_OID_2)  over(partition by bnid) as e_lc_most_viewed_diamond_OID_2  from
     (
      select bnid, ds_sku, count_offer_id ,row_num,  case when row_num=1 then first_value(ds_sku) over(partition by bnid,row_num order by count_offer_id desc) end e_lc_most_viewed_diamond_OID_1
      ,case when row_num=2 then first_value(ds_sku) over(partition by bnid,row_num order by count_offer_id  desc) end e_lc_most_viewed_diamond_OID_2
     from 
     (  select bnid, ds_sku, count_offer_id ,ROW_NUMBER() over(partition by bnid order by count_offer_id desc, max_date desc , latest_time desc) as row_num
       from 
      
      (  select  distinct ds_sku, count(ds_sku) over (partition by bnid, ds_sku) as count_offer_id, 
      max(date_key) over (partition by bnid,ds_sku) as max_date,
     first_value(time_log) over(partition by bnid, ds_sku order by date_key desc,time_log desc ) as latest_time,
        bnid from browse_data 
      where diamond_detail_flag = 1 and ds_sku is not null        
      )   
     ) 
    ) 
    )   diamond_viewed 
      on a.bnid = diamond_viewed.bnid
    
      
    left join
    
    --- Logic for e_lc_most_viewed_setting_OID_1, e_lc_most_viewed_setting_OID_2 ---
    
     (select distinct bnid,max(SAFE_CAST(REGEXP_REPLACE(e_lc_most_viewed_setting_OID_1, "[.][0-9]+$","") AS NUMERIC)) over(partition by bnid) as e_lc_most_viewed_setting_OID_1
     , max(SAFE_CAST(REGEXP_REPLACE(e_lc_most_viewed_setting_OID_2, "[.][0-9]+$","") AS NUMERIC)) over(partition by bnid) as e_lc_most_viewed_setting_OID_2  from
     (
      select bnid, offer_id, count_offer_id , row_num, 
      case when row_num=1 then first_value(offer_id) over(partition by bnid,row_num order by count_offer_id  desc) end e_lc_most_viewed_setting_OID_1
      ,case when row_num =2 then first_value(offer_id) over(partition by bnid,row_num order by count_offer_id  desc) end e_lc_most_viewed_setting_OID_2
      from 
      (  select bnid, offer_id, count_offer_id ,ROW_NUMBER() over(partition by bnid order by count_offer_id desc, max_date desc, latest_time desc) as row_num
      from 
      (  select  distinct offer_id, count(offer_id) over(partition by bnid,offer_id ) count_offer_id,
      max(date_key) over (partition by bnid, offer_id) as max_date,
       first_value(time_log) over(partition by bnid, offer_id order by date_key desc,time_log desc ) as latest_time,
      bnid from browse_data
      where  ifnull( merch_category_rollup,'A') not in ('Diamond Jewelry','Other Jewelry','Bands') 
      and  lower(primary_browse_segment) = 'engagement'
                            AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
                            AND lower(PAGE_GROUP_NAME) IN ('byo3sr setting detail', 'byor setting detail') and offer_id is not null
                
      )
      ) 
       ) 
       )   setting_viewed
    on a.bnid = setting_viewed.bnid
    
    left join
    
    --- Logic for e_lc_count_call_center_contacts ---
    
    
    (select  ead.bnid , count(ead.bnid) as e_lc_count_call_center_contacts
    from `bnile-cdw-prod.dssprod_o_customer.freshdesk_ticket` fr
    left join `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d`  ead
    on ead.email_address =fr.contact_id 
    inner join
    cut_off co 
    on co.bnid = ead.bnid
    where contact_id is not null and contact_id not like '-'
    and cast (SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', fr.CREATED_TIME) as date) >=  cast (PARSE_DATE('%Y%m%d',CAST(co.cut_off_date AS STRING)) as date)
    group by bnid
    ) call_center_contacts
    on 
    a.bnid = call_center_contacts.bnid
    
    
    left join
    
    -- Logic for e_lc_proposal_date ---
    
    (SELECT distinct bnid, max(proposal_date) as e_lc_proposal_date from `bnile-cdw-prod.o_customer.up_explicit_attributes` 
    group by bnid
    ) proposal
    on 
    a.bnid = proposal.bnid
    
    left join
    
    --- Logic for e_lc_free_ring_sizer_order_flag, e_lc_free_ring_sizer_order_date ---
    
    (
    select  bnid,
    max(e_lc_free_ring_sizer_order_flag) as e_lc_free_ring_sizer_order_flag,
    CAST (max(e_lc_free_ring_sizer_order_date) AS DATE) as e_lc_free_ring_sizer_order_date from 
    (select  ead.bnid,
    event_name,case when lower(event_name) like '%ringsize%' then 'Y' else 'N' end as e_lc_free_ring_sizer_order_flag , 
    case when lower(event_name) like '%ringsize%' then event_date else null end as e_lc_free_ring_sizer_order_date
    from `bnile-cdw-prod.nileprod_o_warehouse.site_event` se
    left join `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d` ead
    on se.email_address=ead.email_address 
    where cast (se.event_date as date) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
     
    where e_lc_free_ring_sizer_order_flag = 'Y' group by bnid
    )
    free_ring
    on a.bnid = free_ring.bnid
    
    left join
     
     --- Logic for e_lc_paid_ring_sizer_order_flag, e_lc_paid_ring_sizer_order_date --- 
    
    (SELECT
      bnid,
       max(e_lc_paid_ring_sizer_order_flag) as e_lc_paid_ring_sizer_order_flag,
       max(PARSE_DATE('%Y%m%d',CAST(e_lc_paid_ring_sizer_order_date AS STRING))) as e_lc_paid_ring_sizer_order_date
    FROM (
      SELECT
         ed.bnid,
        CASE
          WHEN prd.PRODUCT_DESCRIPTION = 'Ring Sizer' THEN 'Y' ELSE 'N'
      END
        AS e_lc_paid_ring_sizer_order_flag,
        CASE
          WHEN prd.PRODUCT_DESCRIPTION = 'Ring Sizer' THEN pof.CUSTOMER_SUBMIT_DATE_KEY
        ELSE
        NULL
      END
        AS e_lc_paid_ring_sizer_order_date
      FROM
        `bnile-cdw-prod.dssprod_o_warehouse.product_order_fact`  pof
      LEFT JOIN
       `bnile-cdw-prod.dssprod_o_warehouse.product_dim`  prd
      ON
        pof.product_key = prd.product_key
      LEFT JOIN
      `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim`  ead
      ON
        ead.EMAIL_ADDRESS_KEY = pof.EMAIL_ADDRESS_KEY
         left join 
         `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d` ed
    on ead.email_address=ed.email_address 
      
     where (PARSE_DATE('%Y%m%d',CAST(CUSTOMER_SUBMIT_DATE_KEY AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
     
          
        )
       where e_lc_paid_ring_sizer_order_flag = 'Y'
      GROUP BY
        bnid) paid_ring
      
    on a.bnid = paid_ring.bnid
    
    left join
    
    --- Logic for e_lc_byo_most_recent_review_setting_OID, e_lc_byo_most_recent_review_LD_OID ---
    
    (select bnid,
    SAFE_CAST(REGEXP_REPLACE(e_lc_byo_most_recent_review_setting_OID, "[.][0-9]+$","") AS NUMERIC) as e_lc_byo_most_recent_review_setting_OID,
    e_lc_byo_most_recent_review_LD_OID 
    
    from (select *,row_number() over(partition by bnid order by  date_key desc, time_log desc ) rn 
    from 
    (Select 
    c.bnid, 
    c.offer_id as e_lc_byo_most_recent_review_setting_OID,
    case when coalesce(SKU1, SKU2, SKU3, SKU4, SKU5) like 'ld%' then coalesce(SKU1, SKU2, SKU3, SKU4, SKU5) else null end as e_lc_byo_most_recent_review_LD_OID,
    c.date_key,
    max(parse_time('%T', DISPLAY_TIME_24_HOUR_FORMAT)) time_log
    From
    ( Select  ead.bnid,uri_key, page_group_key, offer_key, offer_id,
    SKU1, SKU2, SKU3, SKU4, SKU5,date_key, time_key
    From `bnile-cdw-prod.dssprod_o_warehouse.click_fact` a
    inner join `bnile-cdw-prod.dssprod_o_warehouse.email_guid`   b
    on a.guid_key = b.guid_key
    left join
    `bnile-cdw-prod.up_stg_customer.bn_guid_all` ead
    on ead.guid =b.guid 
    ) as c
    inner join
    cut_off co
    on co.bnid = c.bnid
    left join `bnile-cdw-prod.dssprod_o_warehouse.uri_dim`  as d
    on c.uri_key= d.uri_key
    left join `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`  as e
    on c.page_group_key= e.page_group_key
    LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.time_dim`  td
                ON c.TIME_KEY = td.TIME_KEY  
          LEFT JOIN
       `bnile-cdw-prod.o_product.offer_attributes_dim`  prd
      ON
        c.OFFER_KEY = prd.offer_id
    where  ifnull( merch_category_rollup,'A') not in ('Diamond Jewelry','Other Jewelry','Bands') 
    and lower(primary_browse_segment) = 'engagement'
                            AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
                            AND lower(PAGE_GROUP_NAME) IN ('byo3sr review your ring', 'byor review your ring') 
                and c.offer_id is not null and date_key >=cut_off_date  
                group by bnid,c.offer_id, sku1, sku2, SKU3, SKU4, SKU5, date_key 
               order by date_key desc, time_log desc )	)
               where  rn=1	) review_setting       
               on a.bnid = review_setting.bnid
    
    -- Logic  e_lc_most_recent_viewed_LD_OID ---
      
    left join
    
    (
      select bnid, ld_oid as e_lc_most_recent_viewed_LD_OID
      from 
      (  select distinct bnid, ld_oid,
      ROW_NUMBER() over(partition by bnid order by date_key desc, time_log desc) as row_num
      from 
     ( select distinct bnid, coalesce(sku1,sku2,sku3,sku4,sku5) as ld_oid
      , date_key,parse_time('%T', cast(DISPLAY_TIME_24_HOUR_FORMAT as string)) time_log
    from `bnile-cdw-prod.dssprod_o_warehouse.click_fact`   cf
    INNER JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  egl
      ON
        cf.GUID_KEY = egl.GUID_KEY
        left join
        `bnile-cdw-prod.up_stg_customer.bn_guid_all`  ead
       on ead.guid=egl.guid 
      left join `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`   pgd
     on cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
    
     LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.time_dim`  td
                ON cf.TIME_KEY = td.TIME_KEY
    
      where  
       coalesce(sku1,sku2,sku3,sku4,sku5) is not null 
        and (PARSE_DATE('%Y%m%d',CAST(cf.date_key AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
      and ((lower(activity_set_code) = 'diamond detail'
                            AND	(
                                    lower(primary_browse_segment) = 'engagement'
                                    AND lower(secondary_browse_segment) IN ('byor', 'byo3sr')
                                    )) 
                                    or 
                            (lower(activity_set_code) = 'diamond detail'
                            AND (
                                    lower(primary_browse_Segment) = 'diamond'
                                    AND lower(secondary_browse_segment) = 'loose_diamond'
                                    )))
                    
                    )
                    )
              where row_num =1      
                    ) most_recent_ld
    on a.bnid = most_recent_ld.bnid
      
    --- e_lc_most_recent_viewed_OID ---
    
    left join
    (select bnid, offer_id as e_lc_most_recent_viewed_OID,
      from 
      (  select distinct bnid,  cast (offer_id as numeric) as offer_id,
      ROW_NUMBER() over(partition by bnid order by date_key desc, time_log desc) as row_num
      from 
    ( select distinct bnid, cf.offer_id,
       date_key,parse_time('%T', cast(DISPLAY_TIME_24_HOUR_FORMAT as string)) time_log
    from `bnile-cdw-prod.dssprod_o_warehouse.click_fact`   cf
    INNER JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  egl
      ON
        cf.GUID_KEY = egl.GUID_KEY
        left join
        `bnile-cdw-prod.up_stg_customer.bn_guid_all`  ead
       on ead.guid=egl.guid 
      left join `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`   pgd
     on cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
    
     LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.time_dim`  td
                ON cf.TIME_KEY = td.TIME_KEY
          LEFT JOIN
       `bnile-cdw-prod.o_product.offer_attributes_dim`  prd
      ON
        cf.OFFER_KEY = prd.offer_id
      where  
     ifnull( merch_category_rollup,'A') not in ('Diamond Jewelry','Other Jewelry','Bands') 
       and cf.offer_id is not null
    and (PARSE_DATE('%Y%m%d',CAST(cf.date_key AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
    and lower(primary_browse_segment) = 'engagement'
    AND lower(secondary_browse_segment) IN ('byor', 'byo3sr','preset')
    and lower(ACTIVITY_SET_CODE) IN ('setting detail', 'engagement detail'))
    
    )
    where row_num =1) most_recent_offerid
    on a.bnid = most_recent_offerid.bnid  
    
    
      left join 
      
      --- Logic for e_lc_count_items_in_cart --- 
      
    (select coalesce(a.bnid,b.bnid) as bnid, sum(ifnull(count_rings,0)+ifnull(count_sku,0)) as e_lc_count_items_in_cart
    from
    (select bnid, count(sku)  as count_rings from
    (SELECT a.basket_id,ead.bnid,c.sku,c.BASKET_ITEM_ID
    FROM `bnile-cdw-prod.nileprod_o_warehouse.basket`  a
    inner join 
    `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  b
    on a.CUSTOMER_GUID = b.GUID
    inner join 
    `bnile-cdw-prod.nileprod_o_warehouse.basket_item`  c
    on a.basket_id = c.basket_id
    inner join 
    `bnile-cdw-prod.up_stg_customer.bn_guid_all`  ead
    on ead.guid=b.guid  
    where b.email_address is not null
    and lower(a.basket_status)  in ('open')
    and ifnull(lower(a.order_type),'a') not in ('exchange','replacement','repair','resize')
    and SKU IN ('BN3STN_MTD','BNRING_MTD')
    )
    group by bnid
    having count_rings >0)a
    full outer join
    (select bnid, count(sku) as count_sku
    from
    (SELECT distinct a.BASKET_ID,ead.bnid,c.sku,c.BASKET_ITEM_ID
    FROM `bnile-cdw-prod.nileprod_o_warehouse.basket`   a
    inner join 
    `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  b
    on a.CUSTOMER_GUID = b.GUID
    inner join 
    `bnile-cdw-prod.nileprod_o_warehouse.basket_item`  c
    on a.basket_id = c.basket_id
    inner join
    `bnile-cdw-prod.up_stg_customer.bn_guid_all`  ead
    on ead.guid=b.guid
    and c.SKU  is not null
    and lower(a.basket_status)  in ('open')
    and ifnull(lower(a.order_type),'a') not in ('exchange','replacement','repair','resize')
    and lower(c.sku) like '%ld%')
    group by bnid
    having count_sku>0
    )b 
    on  a.bnid = b.bnid
    group by bnid) cart
    on a.bnid = cart.bnid
    
    left join
    
    --- Logic for e_lc_gender ---
    
    (select bnid,e_lc_gender from
    (
    select bnid,coalesce(gender,derived_gender) as e_lc_gender from `bnile-cdw-prod.o_customer.bn_customer_profile`  where derived_gender <> 'N/A' )
    )gender
    on a.bnid= gender.bnid
    
     left join
    
    --- Logic for i_lc_most_viewed_metal_1, i_lc_most_viewed_metal_2 ---
    
    (select distinct bnid,max(i_lc_most_viewed_metal_1) over(partition by bnid) as i_lc_most_viewed_metal_1
    , max(i_lc_most_viewed_metal_2) over(partition by bnid) as i_lc_most_viewed_metal_2
     from
    (select bnid, count_metal_name , row_num,case when row_num=1 then first_value(PRIMARY_METAL_NAME) over(partition by bnid,row_num order by count_metal_name  desc) end i_lc_most_viewed_metal_1
    ,case when row_num=2 then first_value(PRIMARY_METAL_NAME) over(partition by bnid,row_num order by count_metal_name  desc) end i_lc_most_viewed_metal_2
     from
    
      (  select bnid, count_metal_name ,PRIMARY_METAL_NAME,
       ROW_NUMBER() over(partition by bnid order by count_metal_name desc, max_date desc , latest_time desc) as row_num
      from 
      (  select  distinct bnid,PRIMARY_METAL_NAME,
      count ( PRIMARY_METAL_NAME ) over(partition by bnid,PRIMARY_METAL_NAME ) as count_metal_name
      ,max(date_key) over (partition by bnid, PRIMARY_METAL_NAME) as max_date
      ,first_value(time_log) over(partition by bnid, PRIMARY_METAL_NAME order by date_key desc,time_log desc ) as latest_time
      from browse_data
        where (lower(primary_browse_segment) = 'engagement'
                            AND lower(secondary_browse_segment) IN ('byor', 'byo3sr','preset'))
                  and PRIMARY_METAL_NAME is not null and PRIMARY_METAL_NAME not like 'nan'
      ) 
      ) 
     )
     
     )   metal_name
    on 
    a.bnid=metal_name.bnid
    
    left join
    
    --- Logic for i_lc_most_viewed_diamond_shape_1, i_lc_most_viewed_diamond_shape_2 ---
    
    
     (select distinct bnid,max(i_lc_most_viewed_diamond_shape_1) over(partition by bnid) as i_lc_most_viewed_diamond_shape_1
    , max(i_lc_most_viewed_diamond_shape_2) over(partition by bnid) as i_lc_most_viewed_diamond_shape_2
     from
    
    (select bnid, count_diamond_shape , row_num,case when row_num=1 then first_value(SHAPE) over(partition by bnid,row_num order by count_diamond_shape  desc) end i_lc_most_viewed_diamond_shape_1
    ,case when row_num=2 then first_value(SHAPE) over(partition by bnid,row_num order by count_diamond_shape  desc) end i_lc_most_viewed_diamond_shape_2
     from
    
      (  select bnid, count_diamond_shape ,SHAPE,ROW_NUMBER() over(partition by bnid order by count_diamond_shape desc, max_date desc , latest_time desc ) as row_num
      from 
    
      (  select  distinct  bnid,SHAPE,count(SHAPE) over(partition by bnid,SHAPE ) as count_diamond_shape,
      max(date_key) over (partition by bnid, SHAPE) max_date
      , first_value(time_log) over(partition by bnid, SHAPE order by date_key desc,time_log desc ) as latest_time
      from browse_data
        where diamond_detail_flag = 1 and  SHAPE is not null and SHAPE not like 'nan' 
                          
      ) 
    ) 
    ) )   dia_shape
    on a.bnid =dia_shape.bnid
    
    left join
    
    --- Logic for i_lc_min_diamond_carat, i_lc_median_diamond_carat, i_lc_max_diamond_carat ---
    
    (
    select distinct bnid,i_lc_min_diamond_carat,i_lc_max_diamond_carat, round(i_lc_median_diamond_carat,2)  i_lc_median_diamond_carat  from
    (select bnid,PERCENTILE_CONT(CARAT,0) OVER(PARTITION BY bnid) AS i_lc_min_diamond_carat
    ,PERCENTILE_CONT(CARAT,0.5) OVER(PARTITION BY bnid) AS i_lc_median_diamond_carat
    ,PERCENTILE_CONT(CARAT,1) OVER(PARTITION BY bnid) AS i_lc_max_diamond_carat
     from
    
    (  select   bnid,CARAT from browse_data
        where diamond_detail_flag = 1 and   CARAT is not null  
    order by CARAT asc
      ) 
     )
    ) dia_carat_weight
    on a.bnid = dia_carat_weight.bnid 
    
    --- i_lc_most_viewed_setting_style_1, i_lc_most_viewed_setting_style_2 --
    
     left join
    (select distinct bnid,max(i_lc_most_viewed_setting_style_1) over(partition by bnid) as i_lc_most_viewed_setting_style_1
    , max(i_lc_most_viewed_setting_style_2) over(partition by bnid) as i_lc_most_viewed_setting_style_2
    from
    (select bnid, count_setting_type , row_num,case when row_num=1 then first_value(PRIMARY_SETTING_TYPE) over(partition by bnid,row_num order by count_setting_type  desc) end i_lc_most_viewed_setting_style_1
    ,case when row_num=2 then first_value(PRIMARY_SETTING_TYPE) over(partition by bnid,row_num order by count_setting_type  desc) end i_lc_most_viewed_setting_style_2
    from
      (  select bnid, count_setting_type ,PRIMARY_SETTING_TYPE,
      ROW_NUMBER() over(partition by bnid order by count_setting_type desc,max_date desc, latest_time desc ) as row_num
      from 
    
      (  select  distinct bnid,PRIMARY_SETTING_TYPE,
      count(PRIMARY_SETTING_TYPE) over (partition by bnid,PRIMARY_SETTING_TYPE ) as count_setting_type,  
      max(date_key) over (partition by bnid,PRIMARY_SETTING_TYPE ) as max_date,
      first_value(time_log) over(partition by bnid, PRIMARY_SETTING_TYPE order by date_key desc,time_log desc ) latest_time
      from browse_data
        where 
              (lower(primary_browse_segment) = 'engagement'
                 AND lower(secondary_browse_segment) IN ('byor', 'byo3sr','preset'))
                 and PRIMARY_SETTING_TYPE is not null and PRIMARY_SETTING_TYPE not like 'nan'
               )  
       ) 
     ) 
      )setting_type
    
    
      on a.bnid = setting_type.bnid
      
      left join
      
      --- Logic for i_lc_most_viewed_diamond_quality ---
      
      (select distinct bnid,max(i_lc_most_viewed_diamond_quality) over(partition by bnid) as i_lc_most_viewed_diamond_quality
     from
    (select bnid, count_diamond_quality , row_num,case when row_num=1 then first_value(diamond_quality) over(partition by bnid,row_num order by count_diamond_quality  desc) end i_lc_most_viewed_diamond_quality
     from
      (  select bnid, count_diamond_quality ,diamond_quality,ROW_NUMBER() over(partition by bnid order by count_diamond_quality desc, 
    max_date desc, latest_time desc) as row_num
      from 
      (  select   distinct bnid,CUT as diamond_quality,
      count(CUT) over(partition by bnid, CUT ) count_diamond_quality,
      max(date_key) over(partition by bnid, CUT ) max_date,
     first_value(time_log) over(partition by bnid, CUT order by date_key desc,time_log desc ) as latest_time
      from browse_data 
        where 
              diamond_detail_flag =1
                                    and  CUT is not null and CUT not like 'nan' 
      ) 
      )
      )
      ) dia_quality
      on a.bnid=dia_quality.bnid
      
      
    
    --- Logic for e_lc_last_site_activity_date ---
    
     left join
      (
     select distinct bnid, max((PARSE_DATE('%Y%m%d',CAST(DATE_KEY AS STRING)))) as e_lc_last_site_activity_date  from customer_data  
    group by bnid  )site_date
    on a.bnid=site_date.bnid   
    
    
    left join 
    
    --- Logic for i_lc_min_budget, i_lc_median_budget, i_lc_max_budget ---
    
    (select distinct bnid,concat(i_lc_min_budget,' USD') i_lc_min_budget, 
    concat(i_lc_median_budget,' USD') i_lc_median_budget ,concat(i_lc_max_budget,' USD') i_lc_max_budget from
    
    (select bnid, round(i_lc_min_budget,2) as i_lc_min_budget, round (i_lc_median_budget,2) as i_lc_median_budget, 
    round(i_lc_max_budget,2)  as i_lc_max_budget
    
    from
    (select 
    distinct bnid,PERCENTILE_CONT(price,0) OVER(PARTITION BY bnid) AS i_lc_min_budget 
    ,PERCENTILE_CONT(price,0.5) OVER(PARTITION BY bnid) AS i_lc_median_budget
     ,PERCENTILE_CONT(price,1) OVER(PARTITION BY bnid) AS i_lc_max_budget 
     from
    
    (
    (select ead.bnid, ROUND(AVG(dpf.PRODUCT_LIST_PRICE),2)  as price
    from `bnile-cdw-prod.dssprod_o_warehouse.click_fact`  cf
    INNER JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  egl
      ON
        cf.GUID_KEY = egl.GUID_KEY
        join
        `bnile-cdw-prod.up_stg_customer.bn_guid_all`   ead
        on ead.guid =egl.guid
        join
        cut_off co
       on co.bnid = ead.bnid
        left join
        `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`   pgd
        on cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
         JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.daily_product_fact` dpf
      ON
      cf.offer_key = dpf.offer_key
     and  cf.DATE_KEY =dpf.DATE_KEY
         where dpf.PRODUCT_LIST_PRICE  is not null
               AND  ( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor',
              'byo3sr',
              'preset',
              'general' ) )
              and cf.date_key >= co.cut_off_date 
           group by bnid,cf.DATE_KEY ,cf.OFFER_KEY
           
           )
           
    UNION ALL
    (select ead.bnid, avg(USD_price) price
    from (select * from `bnile-cdw-prod.dssprod_o_warehouse.click_fact` )  cf
    INNER JOIN
        `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  egl
      ON
        cf.GUID_KEY = egl.GUID_KEY
        join
        `bnile-cdw-prod.up_stg_customer.bn_guid_all`   ead
        on ead.guid =egl.guid
       join
        cut_off co
       on co.bnid = ead.bnid
        left join
        `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`   pgd
        on cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
         JOIN
        `bnile-cdw-prod.dssprod_o_diamond.diamond_daily_data` ddd
      ON
      lower(coalesce(cf.sku1, cf.sku2,cf.sku3,cf.sku4,cf.sku5)) = lower(ddd.sku)
      and   cf.DATE_KEY =ddd.capture_date_key
         where (( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor',
              'byo3sr',
              'preset',
              'general' ) ) 
              or 
               ( LOWER(primary_browse_Segment) = 'diamond'
            AND LOWER(secondary_browse_segment) = 'loose_diamond' )
            )
            and cf.date_key >= co.cut_off_date 
         group by bnid,cf.DATE_KEY ,cf.sku1, cf.sku2, cf.sku3, cf.sku4, cf.sku5
           )
           
           )
    
    
    )
    )
    
    ) pricing_data
    
    on a.bnid=pricing_data.bnid
    
    --- Logic for e_lc_last_email_send_date ---
    
    left join
    (
    SELECT
      distinct bnid, max(last_email_send_date) as e_lc_last_email_send_date
    FROM
    `bnile-cdw-prod.o_customer.up_explicit_attributes` 
    group by bnid
     ) email_send
    
    on a.bnid = email_send.bnid
    
    
    -----i_lc_propensity_overall_phase----
    
    left join
    
    
    ( select distinct bnid, case when date_diff(current_date(), max_date, Day) <=30 
                                     then i_lc_pj_phase
                                     else 'Low' end  as  i_lc_propensity_overall_phase, 
      from
      (SELECT bnid,max_date, max(case when row_num =1 then propensity_phase else null end ) as i_lc_pj_phase
        from
        (select distinct bnid, propensity_phase,max_date,
          row_number() over (partition by bnid order by max_date desc ) row_num
          from
          (SELECT distinct bnid,propensity_phase , max(model_run_date ) over (partition by bnid,propensity_phase) max_date,
            FROM `bnile-cdw-prod.o_customer.bn_lc_propensity` 
            )
            )
           group by bnid,max_date )  where i_lc_pj_phase is not null 
            ) pj_phase
    on a.bnid = pj_phase.bnid
    
    
    -----i_lc_previous_pj_phase -----
    
    left join
    
     ( select distinct bnid, max(i_lc_previous_pj_phase) over (partition by bnid ) i_lc_previous_pj_phase
      from
      (SELECT bnid, 
        case when row_num =2 then propensity_phase else null end as i_lc_previous_pj_phase
        from
        (select distinct bnid, propensity_phase,
          row_number() over (partition by bnid order by max_date desc ) row_num
          from
          (SELECT distinct bnid,propensity_phase , max(model_run_date ) over (partition by bnid,propensity_phase) max_date,
            FROM `bnile-cdw-prod.o_customer.bn_lc_propensity` 
            
            )))) phase
    on a.bnid = phase.bnid
    
    ----i_lc_propensity_overall_score ---
    
    left join
    
    (select bnid, max(case when model_run_date = max_date then propensity_score end) as i_lc_propensity_overall_score
    from
    (SELECT
      BNID,
      round(propensity_score ,3) propensity_score,
      model_run_date ,
      max(model_run_date ) over (partition by bnid) max_date
    FROM
       `bnile-cdw-prod.o_customer.bn_lc_propensity` )
      group by bnid) prediction
    
    on a.bnid =  prediction.bnid
    
    ---- i_lc_propensity_run_date ---
    
    left join
    
    
    (SELECT
      BNID,
       max(model_run_date) as i_lc_propensity_run_date
    FROM
      `bnile-cdw-prod.o_customer.bn_lc_propensity` 
      group by bnid) phase_date
    
    on a.bnid =  phase_date.bnid
    
     
        )
     )'''
    return bql