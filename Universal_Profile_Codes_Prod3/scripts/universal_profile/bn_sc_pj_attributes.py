def bn_sc_pj_attributes():
    bql = '''
    ----- Query for bn_sc_pj_attributes ----
    drop table if exists `bnile-cdw-prod.o_customer.bn_sc_pj_attributes`;
    
    CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.o_customer.bn_sc_pj_attributes` as
    
    (
    
    --- Logic for customer_data it has complete data of customer coming from different sources ---
    
    WITH customer_data AS (
            SELECT distinct 
        ead.bnid
          ,cf.date_key
            ,cf.REQUEST_TYPE 
          ,lower(coalesce(cf.sku1,cf.sku2,cf.sku3,cf.sku4,cf.sku5)) as cf_sku
            ,cast(cf.offer_id AS string) offer_id
          ,cf.offer_key 
          ,pgd.PRIMARY_BROWSE_SEGMENT
                ,pgd.SECONDARY_BROWSE_SEGMENT
                ,pgd.ACTIVITY_SET_CODE
                ,ud.URI
                ,pgd.PAGE_GROUP_NAME
          ,pgd.product_intent
          ,prd.MERCH_CATEGORY_ROLLUP 
          ,prd.class_name as product_class_name
          ,prd.merch_product_category
                ,parse_time('%T', cast(DISPLAY_TIME_24_HOUR_FORMAT as string)) time_log     
            FROM `bnile-cdw-prod.dssprod_o_warehouse.click_fact`  cf
            INNER JOIN `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  egl
                ON cf.GUID_KEY = egl.GUID_KEY
            LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`  pgd
                ON cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
            LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.time_dim`  td
                ON cf.TIME_KEY = td.TIME_KEY
            LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.uri_dim`  ud
                ON cf.URI_KEY = ud.URI_KEY
           left join `bnile-cdw-prod.o_product.offer_attributes_dim`  prd
           on cf.OFFER_KEY  = prd.offer_id
             join  `bnile-cdw-prod.up_stg_customer.bn_guid_all` ead
            on egl.guid =ead.guid 
            
           ),
     
    --- cut_off_date 
     cut_off as 
    (select distinct bnid, min(date_key) as cut_off_date
    from customer_data  
    where 
          ( LOWER(primary_browse_segment) = 'other_jewelry'
         OR LOWER(primary_browse_segment) = 'diamond_jewelry'
         OR  LOWER(primary_browse_segment) = 'wedding_band' 
         or LOWER(primary_browse_segment) = 'misc_jewelry'
		 or ( lower(primary_browse_segment) = 'education'  
        and lower(product_intent)  in ('jewelry','bands') )
		 )
       and PARSE_DATE('%Y%m%d',CAST(date_key AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY)
       group by bnid),
    
    --- Logic for customer_base , the customers who are in short conversion --- 
    
    --- *** SC Rules***
    --- 1) Last 90 days active.
    --- 2) views wedding_band,other_jewelry,diamond_jewelry pages.
    --- 3) no prior purchases.
    
    Short_consideration as
     
    (select distinct bnid, 1 as e_sc from 
        ( select bnid
from customer_data  
where 
     
     --- Rule 1 - Last 90 days active ---

     DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y%m%d',
          CAST(DATE_KEY AS STRING))),DAY) <=90
		  
	  --- Rule 2 - Logic for views wedding_bang,diamond_jewelry,other_jewelry pages ----	 
		  
     AND ( LOWER(primary_browse_segment) = 'other_jewelry'
     OR LOWER(primary_browse_segment) = 'diamond_jewelry'
     OR LOWER(primary_browse_segment) = 'wedding_band' 
	 OR LOWER(primary_browse_segment) = 'misc_jewelry'
	 OR ( lower(primary_browse_segment) = 'education'  
        and lower(product_intent)  in ('jewelry','bands') )
	   )
	 
	 --- Rule 3 - Logic for no prior purchases
	 
    and  bnid not in (select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes` where order_cnt > 0)
 )  
    ),
    
    --- *** PPA Rules***
    --- 1) Purchased an engagement ring in last 18 months
     
    post_purchase as  
     
    (select distinct bnid,ppa_min_date_key, 1 as e_ppa from  
     ( select bnid,min(date_key) as ppa_min_date_key from customer_data
    where
    
     ( LOWER(primary_browse_segment) = 'other_jewelry'
         OR LOWER(primary_browse_segment) = 'diamond_jewelry'
         OR LOWER(primary_browse_segment) = 'wedding_band' 
           or LOWER(primary_browse_segment) = 'misc_jewelry'
		 OR  ( lower(primary_browse_segment) = 'education'  
        and lower(product_intent)  in ('jewelry','bands') )
		   )
        
        and (PARSE_DATE('%Y%m%d',CAST(date_key AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
         
    --- Rule 1 - Logic for Purchased an engagement ring in last 18 months
    and	 bnid  in
       (
       select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes`
     where
    parse_DATE('%Y-%m-%d', CAST(order_engagement_last_dt AS string)) >= DATE_SUB(CURRENT_DATE (), INTERVAL 18 month) and order_engagement_cnt>0)
        group by bnid 
        
          
     )) ,
     
     
    --- *** J4L Rules***
    --- 1) Purchased with us in past
    
    jeweler_for_life as
    (select distinct bnid,j4l_min_date_key, 1 as e_j4l  from 
        ( select bnid,min(date_key) as j4l_min_date_key from customer_data  
     where 
       
       ( LOWER(primary_browse_segment) = 'other_jewelry'
         OR LOWER(primary_browse_segment) = 'diamond_jewelry'
         OR LOWER(primary_browse_segment) = 'wedding_band' 
           or LOWER(primary_browse_segment) = 'misc_jewelry')
        
        and (PARSE_DATE('%Y%m%d',CAST(date_key AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
    
    --- Rule 1 - Logic for last prior purchases
        and 
          bnid  in (select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes` where order_cnt > 0)
          group by bnid
     ) ) ,
    
       customer_base as
        (
        select c.*, d.cut_off_date from
       (select a.* from customer_data a
          inner join
         ( 
         select distinct bnid
         from 
         (
         (select distinct bnid from  Short_consideration)    
           UNION DISTINCT
         (select distinct bnid from  post_purchase )
         UNION DISTINCT 
       (select distinct bnid from jeweler_for_life) )
        ) b 
       on a.bnid=b.bnid  
        )c
        
     -- Cut_off_date For the records---
     left join
    (
    select bnid,  cut_off_date from cut_off  )d
    on c.bnid = d.bnid  
    where date_key >=cut_off_date
    and cut_off_date is not null
          )
       
    
     ---  Logic for browsing pages with respective flags and counts ---
     
     ,browse_data AS (
            SELECT *    
            ,CASE 
                    WHEN showroom_flag = 1
                        THEN count(bnid) OVER (
                                PARTITION BY bnid,showroom_flag
                                
                                )
                    ELSE 0
                    END AS showroom_page_count
            
            ,CASE 
                    WHEN brand_promise_page_flag = 1
                        THEN count(bnid) OVER (
                                PARTITION BY bnid,brand_promise_page_flag
                                
                                )
                    ELSE 0
                    END AS brand_promise_page_count
                ,CASE 
                    WHEN education_flag = 1 
                        THEN count(bnid) OVER (
                                PARTITION BY bnid,education_flag
                                
                                )
                    ELSE 0
                    END AS count_education_flag		
            ,CASE 
                    WHEN cart_adds_flag = 1
                        THEN count(bnid) OVER (
                                PARTITION BY bnid,cart_adds_flag
                                
                                )
                    ELSE 0
                    END AS count_adds_flag
            ,CASE
                    WHEN wedding_education_flag= 1
                        THEN count(bnid) OVER (
                                PARTITION BY bnid ,wedding_education_flag
                                )
                    ELSE 0
                    END AS wedding_education_page_count
            , case when page_views_flag =1
                     then count(bnid) over (PARTITION by bnid, page_views_flag)
                     else 0
                     end as count_page_views_flag
             ,count(distinct date_key) over (partition by bnid) as count_site_visits
    
                    
            FROM (
                SELECT *
                     ,CASE 
                        WHEN page_group_name LIKE '%showroom%'
                THEN 1
                        ELSE 0
                        END AS showroom_flag
              
             ,CASE 
                        WHEN uri LIKE '%/service%' and lower(REQUEST_TYPE)  = 'page'
              THEN 1
                        ELSE 0
                        END AS brand_promise_page_flag
                    
            ,CASE 
                        WHEN lower(primary_browse_segment) = 'education'  
                    and lower(product_intent)  in ('jewelry','bands')
                            THEN 1
                        ELSE 0
                        END AS education_flag
            
            ,CASE 
                    when ((
                                lower(primary_browse_segment) = 'wedding_band'
                            AND lower(secondary_browse_segment) IN ('general', 'byo5sr')
                                AND lower(page_group_name) IN ('band add to basket', 'byo5sr add to basket') ) 
                            or 
                            (
                                lower(primary_browse_segment) = 'other_jewelry'
                                AND lower(secondary_browse_segment) IN ('uncategorized','general')
                                AND lower(page_group_name) IN ('uncategorized add to basket','other jewelry add to basket') )
                            or 
                            (
                                lower(primary_browse_Segment) = 'diamond_jewelry'
                                AND lower(secondary_browse_segment) IN  ('byoe','byop','general')
                                AND lower(page_group_name) IN ('byoe add to basket','byop add to basket','diamond jewelry add to basket') )) 
                  and
                  lower(REQUEST_TYPE) ='service'
                  
                    THEN 1
                        ELSE 0
                        END AS cart_adds_flag		
             , CASE
                        WHEN ((lower(primary_browse_segment) = 'education'
                        AND uri like '%wedding%')
                or
                (   lower(primary_browse_segment) IN ('wedding_band') and page_group_name like '%learn%'))
                            THEN 1
                ELSE 0
                END wedding_education_flag
                
                , case when ( LOWER(primary_browse_segment) = 'other_jewelry'
                               OR LOWER(primary_browse_segment) = 'diamond_jewelry'
                               OR  LOWER(primary_browse_segment) = 'wedding_band' 
                                 or LOWER(primary_browse_segment) = 'misc_jewelry')
                               then 1
                               else 0
                               end page_views_flag
                
                FROM customer_base
                )
            )   
        
    
     
    (select distinct 
     a.bnid
     ,sc_pj_qualify_date
    ,ifnull(e_sc_count_showroom_page_visits,0) as e_sc_count_showroom_page_visits
    ,ifnull(e_sc_count_brand_promise_page_visits,0) as e_sc_count_brand_promise_page_visits
    ,ifnull(e_sc_count_education_pages_visits,0) as e_sc_count_education_pages_visits
    ,ifnull(e_sc_count_cart_adds,0) as e_sc_count_cart_adds
    ,ifnull(e_sc_count_wedding_band_education_pages_visits,0) as e_sc_count_wedding_band_education_pages_visits
    , e_sc_most_viewed_offer_id_1
    , e_sc_most_viewed_offer_id_2
    , e_sc_most_viewed_offer_id_3
    , e_sc_most_viewed_offer_id_4
    ,e_sc_most_recent_viewed_offer_id
    ,e_sc_most_recent_viewed_merch_product_category
    ,ifnull(e_sc_count_call_center_contacts,0) as e_sc_count_call_center_contacts
    ,ifnull(e_sc_count_site_visits,0) as e_sc_count_site_visits
    ,ifnull(e_sc_count_page_views,0) as e_sc_count_page_views
    ,e_sc_last_site_activity_date
    ,i_sc_min_budget
    ,i_sc_median_budget
    , i_sc_max_budget
    ,e_sc_gender
    , e_sc_proposal_date
    , e_sc_wedding_date
    , e_sc_most_viewed_product_class_name_1
    , e_sc_most_viewed_product_class_name_2
    ,e_sc_most_viewed_product_class_name_3
    , e_sc_most_viewed_merch_product_category_1
    , e_sc_most_viewed_merch_product_category_2
    , e_sc_most_viewed_merch_product_category_3
    ,e_sc_partner_email_address
    ,e_sc_enagegement_bought_offer_id
    ,e_sc_enagegement_bought_sku
    ,ifnull(e_sc_wedding_band_bought_count,0) as e_sc_wedding_band_bought_count 
    ,e_sc_enagegement_ring_bought_date 
    ,e_sc_last_email_send_date
    ,ifnull(e_sc,0) as e_sc
    ,ifnull(e_ppa,0) as e_ppa
    ,ifnull(e_j4l,0) as e_j4l
    ,i_sc_propensity_overall_phase 
    ,i_sc_previous_pj_phase
    ,i_sc_propensity_overall_score   
    ,i_sc_propensity_run_date
    ,null as e_sc_PROPOSAL_READINESS
    ,null as e_sc_PROPOSAL_STATUS
    ,null as e_sc_wishlisted_offer_id_1
    ,null as e_sc_wishlisted_offer_id_date_1
    ,null as e_sc_wishlisted_offer_id_2
    ,null as e_sc_wishlisted_offer_id_date_2  
    ,null as e_sc_budget
    ,null as e_engagement_shipping_or_delivery_date
    from
    
     (  
     
     --- Logic for e_sc_count_showroom_page_visits,e_sc_count_brand_promise_page_visits,e_sc_count_education_pages_visits,e_sc_count_cart_adds ----
     
     (
        SELECT DISTINCT 
          bnid
        ,first_value(showroom_page_count) OVER (PARTITION BY bnid order by showroom_page_count desc )  as e_sc_count_showroom_page_visits
       ,first_value(brand_promise_page_count)  OVER (PARTITION BY bnid order by brand_promise_page_count desc )  
         as e_sc_count_brand_promise_page_visits
        ,first_value(count_education_flag) OVER (PARTITION BY bnid order by count_education_flag desc )  as e_sc_count_education_pages_visits
       ,first_value(count_adds_flag) OVER (PARTITION BY bnid order by count_adds_flag desc )  as e_sc_count_cart_adds
       ,first_value(wedding_education_page_count) OVER (PARTITION BY bnid order by wedding_education_page_count desc )  as e_sc_count_wedding_band_education_pages_visits
        ,first_value(count_page_views_flag) OVER (PARTITION BY bnid order by count_page_views_flag desc )  as e_sc_count_page_views
       ,first_value(count_site_visits) OVER (PARTITION BY bnid order by count_site_visits desc )  as e_sc_count_site_visits
      
         
       FROM 
       (
        SELECT *	
        FROM browse_data
        ) 
       )a
       
      left join
      
      --- Logic for sc_pj_qualify_date ---
    ( select distinct  coalesce(a.bnid,b.bnid,c.bnid) as bnid,coalesce(sconsideration_qualify_date,ppa_qualify_date,j4l_qualify_date ) as sc_pj_qualify_date from
     (
    (select distinct bnid,
    min((PARSE_DATE('%Y%m%d',CAST(DATE_KEY AS STRING)))) OVER(PARTITION BY bnid) as sconsideration_qualify_date from customer_base 
    where DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y%m%d',CAST(DATE_KEY AS STRING))),DAY) <= 90) a
    full outer join
    (select distinct bnid,PARSE_DATE ('%Y%m%d',CAST(ppa_min_date_key as string)) as ppa_qualify_date from post_purchase ) b
        on a.bnid=b.bnid 
     full outer join
        (  select distinct bnid, PARSE_DATE ('%Y%m%d',CAST(j4l_min_date_key as string))  as j4l_qualify_date  from jeweler_for_life    
    ) c
     on a.bnid=c.bnid
    )
    )
    qualify_date
    on a.bnid=qualify_date.bnid
    
    left join
    
    (select * from short_consideration  ) sc
    
    on sc.bnid = a.bnid
    
    left join
    
    (select * from post_purchase) ppa
    on ppa.bnid = a. bnid
    
    left join
    
    (select * from jeweler_for_life ) jeweler
    on jeweler.bnid = a.bnid
    
     left join
    
    --- Logic for  e_sc_most_viewed_offer_id_1,e_sc_most_viewed_offer_id_2,e_sc_most_viewed_offer_id_3,e_sc_most_viewed_offer_id_4 ---
    
      (select 
       distinct bnid
      ,max(SAFE_CAST(REGEXP_REPLACE(e_most_viewed_offer_id_1, "[.][0-9]+$","") AS NUMERIC)) over(partition by bnid) as e_sc_most_viewed_offer_id_1
      ,max(SAFE_CAST(REGEXP_REPLACE(e_most_viewed_offer_id_2, "[.][0-9]+$","") AS NUMERIC)) over(partition by bnid) as e_sc_most_viewed_offer_id_2
      ,max(SAFE_CAST(REGEXP_REPLACE(e_most_viewed_offer_id_3, "[.][0-9]+$","") AS NUMERIC)) over(partition by bnid) as e_sc_most_viewed_offer_id_3
      ,max(SAFE_CAST(REGEXP_REPLACE(e_most_viewed_offer_id_4, "[.][0-9]+$","") AS NUMERIC)) over(partition by bnid) as e_sc_most_viewed_offer_id_4
      
      from
     (
      select bnid, offer_id, count_offer_id , row_num, 
      case when row_num=1 then first_value(offer_id) over(partition by bnid,row_num order by count_offer_id   desc) end e_most_viewed_offer_id_1
      ,case when row_num=2 then first_value(offer_id) over(partition by bnid,row_num order by count_offer_id  desc) end e_most_viewed_offer_id_2
      ,case when row_num=3 then first_value(offer_id) over(partition by bnid,row_num order by count_offer_id  desc) end e_most_viewed_offer_id_3
      ,case when row_num=4 then first_value(offer_id) over(partition by bnid,row_num order by count_offer_id  desc) end e_most_viewed_offer_id_4
      from 
      (  select bnid, offer_id, count_offer_id,max_date, 
      ROW_NUMBER() over(partition by bnid order by count_offer_id desc,max_date desc,latest_time desc ) as row_num
      from 
      (  select distinct offer_id, count(offer_id) over(partition by bnid,offer_id ) as count_offer_id, 
      max(date_key) over(partition by bnid, offer_id) as max_date, 
     first_value(time_log) over(partition by bnid, offer_id order by date_key desc,time_log desc ) as latest_time,
      bnid 
      from customer_base
      where merch_category_rollup  not in ('Engagement')  
      and offer_id is not null and merch_category_rollup is not null
     )
     ) 
      ) 
         )  viewed_offer_id 
      on a.bnid = viewed_offer_id.bnid
      
      left join
      
      -- Logic for e_sc_most_recent_viewed_offer_id,e_sc_most_recent_viewed_merch_product_category--
     
     (
      select bnid, offer_id as e_sc_most_recent_viewed_offer_id, merch_product_category as e_sc_most_recent_viewed_merch_product_category
      from 
      (  select distinct bnid, offer_id, merch_product_category,
      ROW_NUMBER() over(partition by bnid order by date_key desc, time_log desc) as row_num
      from 
      (  select distinct ead.bnid, cast (cf.offer_id as numeric) as offer_id, merch_product_category, date_key,parse_time('%T', cast(DISPLAY_TIME_24_HOUR_FORMAT as string)) time_log
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
     left join `bnile-cdw-prod.o_product.offer_attributes_dim`    prd
           on cast(cf.offer_id as numeric)  = prd.offer_id
     LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.time_dim`  td
                ON cf.TIME_KEY = td.TIME_KEY
    inner join
    cut_off co 
    on co.bnid = ead.bnid
        where merch_category_rollup  not in ('Engagement') 
        and lower(merch_product_category) not in ('watches')
      and cf.offer_id is not null and merch_category_rollup is not null
      and (PARSE_DATE('%Y%m%d',CAST(cf.date_key AS STRING)) >= DATE_SUB(current_date(), INTERVAL 365 DAY))
         ) 
     
    ) where row_num =1 ) most_recent
      
     on a.bnid = most_recent.bnid
    
      
      left join
      
      --- Logic for e_sc_count_call_center_contacts ---
    
    (select distinct ead.bnid , count(ead.bnid) as e_sc_count_call_center_contacts
    from `bnile-cdw-prod.dssprod_o_customer.freshdesk_ticket` fr
    left join `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d`  ead
    on lower(trim(ead.email_address)) =lower(trim(fr.contact_id)) 
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
      (
      
      --- Logic for e_sc_last_site_activity_date ---
      
     select distinct bnid, max((PARSE_DATE('%Y%m%d',CAST(DATE_KEY AS STRING)))) as e_sc_last_site_activity_date  from customer_data  
    group by bnid  )site_date
    on a.bnid=site_date.bnid            
                
                
     left join
     
     --- Logic for i_sc_min_budget,i_sc_median_budget,i_sc_max_budget ---
     (select distinct bnid, concat(i_min_budget,' USD') i_sc_min_budget, 
    concat(i_median_budget,' USD') i_sc_median_budget ,concat(i_max_budget,' USD') i_sc_max_budget from
    
    (select bnid, i_min_budget, round (i_median_budget,2) as i_median_budget, i_max_budget
    from
    (select 
    distinct bnid,PERCENTILE_CONT(price,0) OVER(PARTITION BY bnid) AS i_min_budget 
    ,PERCENTILE_CONT(price,0.5) OVER(PARTITION BY bnid) AS i_median_budget
     ,PERCENTILE_CONT(price,1) OVER(PARTITION BY bnid) AS i_max_budget 
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
         and dpf.PRODUCT_LIST_PRICE <> 0
               AND  ( LOWER(primary_browse_segment) = 'other_jewelry'
         OR LOWER(primary_browse_segment) = 'diamond_jewelry'
         OR  LOWER(primary_browse_segment) = 'wedding_band' 
         or LOWER(primary_browse_segment) = 'misc_jewelry'
         ) and cf.date_key >= co.cut_off_date 
           group by bnid,cf.DATE_KEY ,cf.OFFER_KEY
           )
           
     union all
     
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
      and  USD_price <> 0 and 
      cf.DATE_KEY =ddd.capture_date_key
         where ( LOWER(primary_browse_segment) = 'other_jewelry'
         OR LOWER(primary_browse_segment) = 'diamond_jewelry'
         OR  LOWER(primary_browse_segment) = 'wedding_band'
         or LOWER(primary_browse_segment) = 'misc_jewelry'
         )
           and cf.date_key >= co.cut_off_date 
         group by bnid,cf.DATE_KEY ,cf.sku1, cf.sku2, cf.sku3, cf.sku4, cf.sku5
           ))
     ))) budget
    on a.bnid=budget.bnid
    
     
     left join
    
    --- Logic for e_sc_gender ---
    
    (select bnid,e_sc_gender from
    (
    select bnid,coalesce(gender,derived_gender) as e_sc_gender from `bnile-cdw-prod.o_customer.bn_customer_profile`  where derived_gender <> 'N/A' )
    
    )gender
    on a.bnid= gender.bnid
     
     
     left join
    
    --- Logic for e_sc_proposal_date ---
    
    (SELECT distinct bnid, max(proposal_date) as e_sc_proposal_date from `bnile-cdw-prod.o_customer.up_explicit_attributes` 
    group by bnid
    ) proposal
    on 
    a.bnid = proposal.bnid
    
    left join
    
    --- Logic for e_sc_wedding_date ---
    
    
    (select bnid ,max(wedding_date) as e_sc_wedding_date from `bnile-cdw-prod.up_stg_customer.stg_pii_attributes` where wedding_date is not null 
    group by bnid
    ) wed_date
    on a.bnid=wed_date.bnid    
     
     left join
     
     --- Logic for e_sc_most_viewed_product_class_name_1,e_sc_most_viewed_product_class_name_2,e_sc_most_viewed_product_class_name_3 ---
     
     
    (select distinct bnid
      ,max(e_most_viewed_product_class_name_1) over(partition by bnid) as e_sc_most_viewed_product_class_name_1
      , max(e_most_viewed_product_class_name_2) over(partition by bnid) as e_sc_most_viewed_product_class_name_2
      ,  max(e_most_viewed_product_class_name_3) over(partition by bnid) as e_sc_most_viewed_product_class_name_3
      
      from
     (
      select bnid, product_class_name, count_product_class_name , row_num
      , case when row_num=1 then first_value(product_class_name) over(partition by bnid,row_num order by count_product_class_name  desc) end e_most_viewed_product_class_name_1
      ,case when row_num=2 then first_value(product_class_name) over(partition by bnid,row_num order by count_product_class_name  desc) end e_most_viewed_product_class_name_2
      ,case when row_num=3 then first_value(product_class_name) over(partition by bnid,row_num order by count_product_class_name  desc) end e_most_viewed_product_class_name_3
      
      from 
      (  select bnid, product_class_name, count_product_class_name ,
      ROW_NUMBER() over(partition by bnid order by count_product_class_name desc, max_date desc,latest_time desc ) as row_num
      from 
      (  select distinct product_class_name, 
      count(product_class_name) over (partition by bnid,product_class_name) as count_product_class_name, 
      max(date_key) over (partition by bnid,product_class_name) as max_date,
     first_value(time_log) over(partition by bnid, product_class_name order by date_key desc,time_log desc ) as latest_time,
      bnid from customer_base
      where merch_category_rollup  not in ('Engagement')  and product_class_name is not null and merch_category_rollup is not null
      )
     ) 
      ) 
         )   viewed_product_class_name 
      on a.bnid = viewed_product_class_name.bnid
    
     
      left join
     
     --- Logic for e_sc_most_viewed_merch_product_category_1,e_sc_most_viewed_merch_product_category_2,e_sc_most_viewed_merch_product_category_3 ---
     
     
    (select distinct bnid
      ,max(e_most_viewed_merch_product_category_1) over(partition by bnid) as e_sc_most_viewed_merch_product_category_1
      , max(e_most_viewed_merch_product_category_2) over(partition by bnid) as e_sc_most_viewed_merch_product_category_2
      ,  max(e_most_viewed_merch_product_category_3) over(partition by bnid) as e_sc_most_viewed_merch_product_category_3
      
      from
     (
      select bnid, merch_product_category, count_merch_product_category , row_num
      , case when row_num=1 then first_value(merch_product_category) over(partition by bnid,row_num order by count_merch_product_category  desc) end e_most_viewed_merch_product_category_1
      ,case when row_num=2 then first_value(merch_product_category) over(partition by bnid,row_num order by count_merch_product_category  desc) end e_most_viewed_merch_product_category_2
      ,case when row_num=3 then first_value(merch_product_category) over(partition by bnid,row_num order by count_merch_product_category  desc) end e_most_viewed_merch_product_category_3
      
      from 
      (  select bnid, merch_product_category, count_merch_product_category ,
      ROW_NUMBER() over(partition by bnid order by count_merch_product_category desc, max_date desc, latest_time desc) as row_num
      from 
      ( select  distinct bnid,merch_product_category, 
      count(merch_product_category) over(partition by bnid,merch_product_category) as count_merch_product_category,
      max(date_key) over(partition by bnid,merch_product_category) as max_date,
        first_value(time_log) over(partition by bnid, merch_product_category order by date_key desc,time_log desc ) as latest_time
       from 
      customer_base
      where merch_category_rollup  not in ('Engagement')  and merch_product_category is not null and merch_category_rollup is not null
     )
     ) 
      ) 
         )   viewed_merch_product_category
      on a.bnid = viewed_merch_product_category.bnid
     
    left join
    
    --- Logic for e_sc_partner_email_address ---
    
    (select bnid,e_sc_partner_email_address from
     (select distinct ead.bnid,FIANCE_EMAIL_ADDRESS as e_sc_partner_email_address , ROW_NUMBER() over ( partition by bnid order by e.FIANCE_CREATE_DATE desc)
     as row_num
      from `bnile-cdw-prod.dssprod_o_warehouse.email_address` e
      left join `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d`  ead
      on ead.email_address=e.email_address
      where FIANCE_EMAIL_ADDRESS is not null )
     where row_num=1
     )fiance_email
     on a.bnid = fiance_email.bnid
     
     --- Logic for e_sc_enagegement_ring_bought_date, e_sc_wedding_band_bought_count ---
     
    left join
    
    (SELECT distinct bnid, max(order_engagement_ring_last_dt) as e_sc_enagegement_ring_bought_date,
    sum(order_wedding_band_cnt) as e_sc_wedding_band_bought_count,
    FROM `bnile-cdw-prod.up_stg_customer.stg_order_attributes` 
    group by bnid
     ) en
     
    on a.bnid = en.bnid
    
    --- Logic for e_sc_enagegement_bought_offer_id ---
    
     left join
     
    (with orders_data as
    (select
    distinct
               bnid,
               trim(lower(ead.email_address)) email_address ,
                prd.product_department_name as activity_name,
                prd.PRODUCT_OFFER_KEY,
                case when lower(sku) like  'ld%' then sku else null end sku,
                case
                    when otd.order_category = 'R' and otd.order_type <> 'REPLACEMENT' and cast(otd.new_order_flag as Numeric) = 1 then 'ORDER'
                    when otd.order_category = 'RMA' and otd.order_type not in ('RETURN FOR MOUNTING') and cast(otd.return_for_work_flag as Numeric)= 0 then 'RETURN'
                end activity_type,
                case
                    when otd.order_category = 'R' and otd.order_type <> 'REPLACEMENT' and cast(otd.new_order_flag as Numeric)= 1 
                      then ifnull(pof.customer_submit_date_key,pof.order_date_key)
                    --               
                    when otd.order_category = 'RMA' and otd.order_type not in ('RETURN FOR MOUNTING') and cast(otd.return_for_work_flag as Numeric)= 0 
                      then pof.order_date_key 
    
                end activity_date,
                pof.basket_id activity_count,
                case when prd.product_department_name = 'Engagement' then 1 else 0 end engagement_flag, 
                case when prd.product_department_name = 'Engagement' and prd.product_category_name like '%Ring%' then 1 else 0 end engagement_ring_flag,
                case when prd.product_category_name = 'Loose Diamonds' then 1 else 0 end loose_diamond_flag,
                from
                `bnile-cdw-prod.dssprod_o_warehouse.product_order_fact` pof,
                `bnile-cdw-prod.dssprod_o_warehouse.order_type_dim` otd,
                `bnile-cdw-prod.dssprod_o_warehouse.product_dim` prd,
                `bnile-cdw-prod.dssprod_o_warehouse.host_dim` hsd,
                `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead,
                `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d` ed
                where 1=1
                and pof.order_type_key = otd.order_type_key
                and pof.product_key = prd.product_key
                and pof.host_key = hsd.host_key
                and pof.email_address_key= ead.email_address_key
                and ead.EMAIL_ADDRESS =ed.EMAIL_ADDRESS 
                and prd.sku not like 'GIFTCERT%'
                and prd.product_category_name not in ('Misc')
                and case
                when otd.order_category = 'R' and otd.order_type <> 'REPLACEMENT' and cast(otd.new_order_flag as Numeric) = 1 then 1
                when otd.order_category = 'RMA' and otd.order_type not in ('RETURN FOR MOUNTING') and cast(otd.return_for_work_flag as Numeric) = 0 then 1
                else 0
                end = 1
                and pof.cancel_date_key is null
                and cast(pof.order_amount_net as Numeric) <> 0
                    )
    
    
    select bnid, product_offer_key as e_sc_enagegement_bought_offer_id, sku as e_sc_enagegement_bought_sku
    from
    
    (select bnid, product_offer_key, sku, activity_date, row_number() over (partition by bnid order by activity_date desc) rank from
    (
    select distinct bnid, 
    first_value(PRODUCT_OFFER_KEY) over (partition by bnid, activity_date order by PRODUCT_OFFER_KEY desc) product_offer_key,
    first_value(sku) over (partition by bnid, activity_date order by sku desc) sku,
    activity_date
    from
    (SELECT
      bnid, 
      case when orders_product >0 then PRODUCT_OFFER_KEY else null end as PRODUCT_OFFER_KEY,
      case when orders_diamonds>0 then sku else null end as sku,
      activity_date,
    from
     ( 
      SELECT
      distinct 
      bnid,
      PRODUCT_OFFER_KEY,
      sku ,
      ((first_value(orders_product) OVER (PARTITION BY bnid, PRODUCT_OFFER_KEY order by  orders_product desc)) -  (first_value(returns_product) OVER (PARTITION BY bnid, PRODUCT_OFFER_KEY order by  returns_product desc))) orders_product,
      ((first_value(orders_diamonds) OVER (PARTITION BY bnid, sku order by  orders_diamonds desc)) -  (first_value(returns_diamonds) OVER (PARTITION BY bnid, sku order by  returns_diamonds desc))) orders_diamonds,
      activity_date,
      activity_type
      from
       
        
        (
      SELECT
        DISTINCT bnid,
        PRODUCT_OFFER_KEY,
        sku,
        activity_type,
        activity_date,
        countif (PRODUCT_OFFER_KEY IS NOT NULL
          AND activity_type = 'ORDER') OVER(PARTITION BY bnid, PRODUCT_OFFER_KEY, activity_type) AS orders_product,
        countif (PRODUCT_OFFER_KEY IS NOT NULL
          AND activity_type = 'RETURN' ) OVER(PARTITION BY bnid, PRODUCT_OFFER_KEY, activity_type) AS returns_product,
         countif (sku IS NOT NULL
          AND activity_type = 'ORDER') OVER(PARTITION BY bnid, sku, activity_type) AS orders_diamonds,
          countif (sku IS NOT NULL
          AND activity_type = 'RETURN' ) OVER(PARTITION BY bnid, sku, activity_type) AS returns_diamonds
      FROM (
        SELECT
          *
        FROM
         orders_data
        WHERE
         ( engagement_flag =1 or loose_diamond_flag =1) 
          ) )
    
          ) where activity_type <> 'RETURN'
    )
          )
         where (product_offer_key is not null  or sku is not null))
         where rank =1) engagement_id
    on a.bnid = engagement_id.bnid
    
    --- Logic for e_sc_last_email_send_date ---
    
    left join 
    (
    SELECT
      distinct bnid, max(last_email_send_date) as e_sc_last_email_send_date
    FROM `bnile-cdw-prod.o_customer.up_explicit_attributes` 
    group by bnid
      ) email_send
    
    on a.bnid = email_send.bnid
    
    --- i_sc_propensity_overall_phase--
    
    left join
    
    ( select distinct bnid, case when date_diff(current_date(), max_date, Day) <=30 
                                     then i_sc_pj_phase
                                     else 'Low' end  as  i_sc_propensity_overall_phase, 
      from
      (SELECT bnid,max_date, max(case when row_num =1 then propensity_phase else null end ) as i_sc_pj_phase
        from
        (select distinct bnid, propensity_phase,max_date,
          row_number() over (partition by bnid order by max_date desc ) row_num
          from
          (SELECT distinct bnid,propensity_phase , max(model_run_date ) over (partition by bnid,propensity_phase) max_date,
            FROM `bnile-cdw-prod.o_customer.bn_sc_propensity`
            )
            )
           group by bnid,max_date )  where i_sc_pj_phase is not null 
            ) pj_phase
            
    on  a.bnid = pj_phase.bnid
    
    
    ---i_sc_previous_pj_phase ---
    
    left join
    
    
    ( select distinct bnid, 
      max(i_sc_previous_pj_phase) over (partition by bnid ) i_sc_previous_pj_phase
      from
      (SELECT bnid,
        case when row_num =2 then propensity_phase else null end as i_sc_previous_pj_phase
        from
        (select distinct bnid, propensity_phase,
          row_number() over (partition by bnid order by max_date desc ) row_num
          from
          (SELECT distinct bnid,propensity_phase , max(model_run_date) over (partition by bnid,propensity_phase) max_date,
            FROM `bnile-cdw-prod.o_customer.bn_sc_propensity`   
            )))) phase
    
    on  a.bnid = phase.bnid
    
    left join
    
    --- i_sc_propensity_overall_score ---
    
    (select bnid, max(case when model_run_date = max_date then propensity_score end) as 	i_sc_propensity_overall_score   
    from
    (SELECT
      BNID,
      round(propensity_score ,3) propensity_score,
      model_run_date ,
      max(model_run_date ) over (partition by bnid) max_date
    FROM
      `bnile-cdw-prod.o_customer.bn_sc_propensity` )
       group by bnid )  prediction
       
       on a.bnid = prediction.bnid
    
    
    left join
    
    --- i_sc_propensity_run_date ---
    
    (SELECT
      BNID,
       max(model_run_date) as i_sc_propensity_run_date 
    FROM
      `bnile-cdw-prod.o_customer.bn_sc_propensity` 
      group by bnid) phase_date
    
    on a.bnid =  phase_date.bnid
    
    
    )
     )
           
    )'''
    return bql