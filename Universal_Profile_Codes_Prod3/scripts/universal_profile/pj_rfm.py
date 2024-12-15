def pj_rfm():
    bql = '''
    --Pool of people falling in LC and SC --------
    
    drop table if exists `bnile-cdw-prod.o_customer.bn_pj_tagging`;
    
    CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.o_customer.bn_pj_tagging` as
    
    
    (
    
    WITH customer_data AS (
            SELECT distinct 
        ead.bnid
        ,cf.guid_key
                ,cf.date_key
                ,pgd.PRIMARY_BROWSE_SEGMENT
                ,pgd.SECONDARY_BROWSE_SEGMENT
        ,pgd.product_intent
            FROM `bnile-cdw-prod.dssprod_o_warehouse.click_fact`  cf
            INNER JOIN `bnile-cdw-prod.dssprod_o_warehouse.email_guid`  egl
                ON cf.GUID_KEY = egl.GUID_KEY
            LEFT JOIN `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim`  pgd
                ON cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
            join `bnile-cdw-prod.up_stg_customer.bn_guid_all` ead
            on egl.GUID = ead.guid         
        ),
    
    
    --- Logic for customer_base , the customers who are in short conversion --- 
    
    --- *** SC Rules***
    --- 1) Last 90 days active.
    --- 2) views wedding_band,other_jewelry,diamond_jewelry pages.
    --- 3) no prior purchases.
    
    Short_consideration as
    
    (select distinct bnid, 'SC' as Bucket from 
        ( select bnid
    from customer_data  
    where 
        
        --- Rule 1 - Last 90 days active ---
    
        DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y%m%d',
            CAST(DATE_KEY AS STRING))),DAY) <=90
            
        --- Rule 2 - Logic for views wedding_bang,diamond_jewelry,other_jewelry pages ----	 
            
        AND (LOWER(primary_browse_segment) = 'other_jewelry'
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
    
    
    ( select distinct bnid, 'PPA' as Bucket from customer_data
    where
    
    ( LOWER(primary_browse_segment) = 'other_jewelry'
        OR LOWER(primary_browse_segment) = 'diamond_jewelry'
        OR LOWER(primary_browse_segment) = 'wedding_band' 
        OR LOWER(primary_browse_segment) = 'misc_jewelry'
        OR ( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor', 'byo3sr','preset','general') )
        OR ( LOWER(primary_browse_Segment) = 'diamond'
            AND LOWER(secondary_browse_segment) = 'loose_diamond' ) 
        OR ( lower(primary_browse_segment) = 'education'  
            and lower(product_intent)  in ('jewelry','bands','engagement') ) )
            
        --- Rule 2 - Last 90 days active ---
        and DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y%m%d',
            CAST(DATE_KEY AS STRING))),DAY) <=90 
        
    --- Rule 3 - Logic for Purchased an engagement ring in last 18 months
    and	 bnid  in
    (
    select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes`
    where
    parse_DATE('%Y-%m-%d', CAST(order_engagement_last_dt AS string)) >= DATE_SUB(CURRENT_DATE (), INTERVAL 18 month) 
    and order_engagement_cnt >0  and order_cnt >0)
        
        
    ) ,
    
    
    --- *** J4L Rules***
    --- 1) Purchased with us in past
    
    jeweler_for_life as
    
        ( select bnid, 'J4L' as Bucket from customer_data  
    where 
    
    ( LOWER(primary_browse_segment) = 'other_jewelry'
        OR LOWER(primary_browse_segment) = 'diamond_jewelry'
        OR LOWER(primary_browse_segment) = 'wedding_band' 
        OR LOWER(primary_browse_segment) = 'misc_jewelry'
        OR ( LOWER(primary_browse_segment) = 'engagement'
            AND LOWER(secondary_browse_segment) IN ('byor', 'byo3sr','preset','general') )
        OR ( LOWER(primary_browse_Segment) = 'diamond'
            AND LOWER(secondary_browse_segment) = 'loose_diamond' ) 
        OR ( lower(primary_browse_segment) = 'education'  
            and lower(product_intent)  in ('jewelry','bands','engagement') ) )
            
            
        --- Rule 2 - Last 90 days active ---
    and 
        DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y%m%d', CAST(DATE_KEY AS STRING))),DAY) <=90
    
    --- Rule 1 - Logic for last prior purchases
        and 
        bnid  in (select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes` where order_cnt > 0 )
        
        and bnid not in (select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes` where
    parse_DATE('%Y-%m-%d', CAST(order_engagement_last_dt AS string)) >= DATE_SUB(CURRENT_DATE (), INTERVAL 18 month) 
    and order_engagement_cnt >0  and order_cnt >0 )
    
    ),
    
    LC as 
    (select bnid , 'LC' as Bucket from
    (select  bnid
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
            AND LOWER(secondary_browse_segment) = 'loose_diamond' ) 
        OR ( lower(primary_browse_segment) = 'education' and 
                        ( lower(product_intent) = 'engagement') ) )
            
            --- Rule 3 - Logic for has not purchased engagement ring in past 18 months
            
            AND  bnid  not in
    (
    select bnid from `bnile-cdw-prod.up_stg_customer.stg_order_attributes`
    where parse_DATE('%Y-%m-%d',
        CAST(order_engagement_last_dt AS string)) >= DATE_SUB(CURRENT_DATE (), INTERVAL 18 month)
        and order_engagement_cnt >0)))
    
    Select *, Current_date() as run_date from Short_consideration
    UNION Distinct
    Select *,Current_date() as run_date from post_purchase
    UNION Distinct
    select *, Current_date() as run_date from jeweler_for_life
    union distinct
    select *, Current_date() as run_date from LC
    );
        
    ---creating history_bn_pj_tagging 
    
    CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.up_stg_customer.history_bn_pj_tagging`
    ( bnid	STRING
    ,Bucket STRING		
    ,run_date Date ) ;
    
    insert into `bnile-cdw-prod.up_stg_customer.history_bn_pj_tagging`
    
    select * from `bnile-cdw-prod.o_customer.bn_pj_tagging`
    where run_date = current_date() and run_date not in 
    ( select max(run_date) from `bnile-cdw-prod.up_stg_customer.history_bn_pj_tagging` );
    
    
    --drop table if exists `bnile-cdw-prod.up_stg_customer.bn_pj_tag`;
    create or replace table `bnile-cdw-prod.up_stg_customer.bn_pj_tag` as
    
    select bnid,
    max(pj_ppa) as pj_ppa,
    max(pj_lc) as pj_lc,
    max(pj_sc) as pj_sc,
    max(pj_j4l) as pj_j4l
    from 
    (SELECT bnid, 
    case when bucket='PPA' then "Y" else "N" end pj_ppa, 
    case when bucket='LC' then "Y" else "N" end pj_lc,
    case when bucket='SC' then "Y" else "N" end pj_sc,
    case when bucket='J4L' then "Y" else "N" end pj_j4l
    from  `bnile-cdw-prod.o_customer.bn_pj_tagging`
    )
    group by bnid
    ;
        
    -------------RFM------------------------------
    
    -- RFM Calculation - Prod ----------
    
    
    -- Engagement source data 
    --drop table if exists `bnile-cdw-prod.up_stg_customer.rfm_eng_src`;
    create or replace table `bnile-cdw-prod.up_stg_customer.rfm_eng_src`
    AS
    SELECT
    bnid, 
    sum(order_engagement_cnt) as order_engagement_cnt,
    max(order_engagement_last_dt) as order_engagement_last_dt,
    avg(order_engagement_avg_amt) as order_engagement_avg_amt,
    FROM
    `bnile-cdw-prod.o_customer.up_explicit_attributes`
    WHERE
    order_engagement_cnt>0 and order_engagement_avg_amt>=0
    group by bnid
    ;
    
    
    -- NTiles - Engagement
    
    --DROP TABLE IF EXISTS `bnile-cdw-prod.up_stg_customer.rfm_eng`;
    CREATE or replace TABLE `bnile-cdw-prod.up_stg_customer.rfm_eng` AS
    SELECT
    bnid,
    order_engagement_cnt,
    order_engagement_last_dt,
    order_engagement_avg_amt,
    NTILE(3) OVER (ORDER BY order_engagement_last_dt) AS rfm_e_r,
    NTILE(3) OVER (ORDER BY order_engagement_cnt) AS rfm_e_f,
    NTILE(3) OVER (ORDER BY order_engagement_avg_amt) AS rfm_e_m
    FROM
    `bnile-cdw-prod.up_stg_customer.rfm_eng_src`
    ;
    
    -- Jewelry source data 
    --drop table if exists `bnile-cdw-prod.up_stg_customer.rfm_jewelry_src`;
    create or replace table `bnile-cdw-prod.up_stg_customer.rfm_jewelry_src`
    AS
    SELECT
    bnid, 
    sum(order_jewelry_cnt) as order_jewelry_cnt,
    max(order_jewelry_last_dt) as order_jewelry_last_dt,
    avg(order_jewelry_avg_amt) as order_jewelry_avg_amt,
    FROM
    `bnile-cdw-prod.o_customer.up_explicit_attributes`
    WHERE
    order_jewelry_cnt>0 and order_jewelry_avg_amt>=0
    GROUP BY
    bnid
    ;
    --Jewelry Ntiles
    
    --DROP TABLE IF EXISTS  `bnile-cdw-prod.up_stg_customer.rfm_jewelry`;
    CREATE or replace TABLE 
    `bnile-cdw-prod.up_stg_customer.rfm_jewelry` AS
    SELECT
    bnid,
    order_jewelry_cnt,
    order_jewelry_last_dt,
    order_jewelry_avg_amt,
    NTILE(3) OVER (ORDER BY order_jewelry_last_dt) AS rfm_j_r,
    NTILE(3) OVER (ORDER BY order_jewelry_cnt) AS rfm_j_f,
    NTILE(3) OVER (ORDER BY order_jewelry_avg_amt) AS rfm_j_m
    FROM
    `bnile-cdw-prod.up_stg_customer.rfm_jewelry_src` 
    ;'''
    return bql