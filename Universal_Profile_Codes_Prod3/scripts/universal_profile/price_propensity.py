def price_propensity(params):
    project_name = params["project_name"]
    start_date = 20170101
    _query = query(project_name, start_date)
    main_query = _query
    return main_query


def query(project_name, start_date):
    bql = f'''        
    with browse as 
    (Select total.*,product_list_price
    From(
    Select distinct bgm.bnid, cf.offer_key, pof.offer_key as is_ordered_offerid,
    cf.guid_key, cf.date_key, activity_set_code
    From
    (Select *, 
    From  `{project_name}.dssprod_o_warehouse.click_fact`
    where date_key>={start_date}) cf
    inner join `{project_name}.up_tgt_ora_tables.guid_dim_cf` gd
    on cf.guid_key= gd.guid_key
    inner join( Select distinct bnid, customer_guid from  `{project_name}.up_stg_customer.bnid_guid_mapping`)bgm
    on gd.guid= bgm.customer_guid
    inner join `{project_name}.dssprod_o_warehouse.offer_dim` od
    on cf.offer_key= od.offer_key
    Left join (select page_group_key, activity_set_code
    From `{project_name}.dssprod_o_warehouse.page_group_dim`) pgd
    on cf.page_group_key= pgd.page_group_key
    Left join (Select guid_key,  offer_key, customer_submit_date_key
    from `{project_name}.dssprod_o_warehouse.product_order_fact`
    where return_accepted_date_key is null and CANCEL_DATE_KEY is null) pof
    on cf.offer_key= pof.offer_key and cf.guid_key= pof.guid_key 
    order by bnid,offer_key, date_key) total
    inner join (Select distinct offer_key, avg(product_list_price) as Product_list_price
    from `{project_name}.dssprod_o_warehouse.daily_product_fact` 
    group by offer_key)dpf
    on total.offer_key = dpf.offer_key),
    type_browsing as (
    Select distinct bnid,offer_key, guid_key, date_key,product_list_price
    From browse
    ), 
    browsing_cat as (
    Select ori.bnid, 
        min(product_list_price) as min_browse_price,
        max(lower_quartile) as percentile_25_browse,
        avg(product_list_price) as average_browse_price,
        max(median) as median_browse_price,
        max(upper_quartile) as percentile_75_browse,
        max(product_list_price) as max_browse_price,
        max(std) as standard_deviation_browse, 
        count(distinct offer_key) as distinct_offer_key_browse_count
    From type_browsing ori
    Left join ( 
    Select distinct bnid,PERCENTILE_CONT( product_list_price,0.5) over (partition by bnid) as median
        ,PERCENTILE_CONT( product_list_price,0.75) over (partition by bnid) as upper_quartile
        ,PERCENTILE_CONT( product_list_price,0.25) over (partition by bnid ) as lower_quartile
        ,STDDEV_POP(product_list_price ) over (partition by bnid) as std
    from type_browsing) calculate
    on ori.bnid= calculate.bnid
    group by bnid),Mastertable as (
    Select type_browse.bnid as bnid,
    type_browse.* except(bnid,percentile_25_browse,percentile_75_browse) ,
    Case when (type_browse.average_browse_price=0 or type_browse.average_browse_price is null) then 0
         else type_browse.standard_deviation_browse/type_browse.average_browse_price 
         end as cv_browse,
         concat(type_browse.percentile_25_browse, " " , "-"," ",type_browse.percentile_75_browse) as price_range_browse,
    type_order.* except(bnid),
    Case when (type_order.average_order_price=0 or type_order.average_order_price is null) then 0
         else type_order.standard_deviation_order/type_order.average_order_price
         end as cv_order,
         concat(type_order.min_order_price , " " , "-"," ",type_order.max_order_price) as price_range_order,
    type_basket.*except(bnid),
    Case when (type_basket.average_price_atb=0 or type_basket.average_price_atb is null) then 0
         else type_basket.standard_deviation_atb/type_basket.average_price_atb 
         end as cv_atb,
         concat(type_basket.min_price_atb, " " , "-"," ",type_basket.max_price_atb) as price_range_atb,
    From browsing_cat type_browse
    Left join (
    with main as (
    Select distinct bnid,offer_key, guid_key,
    product_list_price
    From browse
    where is_ordered_offerid is not null )
    Select ori.bnid, 
        min(product_list_price) as min_order_price,
        avg(product_list_price) as average_order_price,
        max(median) as median_order_price,
        max(product_list_price) as max_order_price,
        max(std) as standard_deviation_order,
        count(distinct offer_key) as distinct_offer_key_order_count
    From main ori
    Left join ( 
    Select distinct bnid, PERCENTILE_CONT( product_list_price,0.5) over (partition by bnid) as median
        ,STDDEV_POP(product_list_price ) over (partition by bnid) as std
    from main) calculate
    on ori.bnid= calculate.bnid 
    group by bnid 
    )  type_order
    on type_browse.bnid= type_order.bnid
    Left join (
    with main as (
    Select distinct bnid,offer_key, guid_key, date_key,product_list_price
    From browse
    where activity_set_code like '%basket%')
    Select ori.bnid, 
        min(product_list_price) as min_price_atb,
        avg(product_list_price) as average_price_atb,
        max(median) as median_price_atb,
        max(product_list_price) as max_price_atb,
        max(std) as standard_deviation_atb,
        count(distinct offer_key) as distinct_offer_key_atb_basket
    From main ori
    Left join ( Select distinct bnid, PERCENTILE_CONT(product_list_price,0.5) over (partition by bnid) as median
    ,STDDEV_POP(product_list_price ) over (partition by bnid) as std
    from main) calculate
    on ori.bnid= calculate.bnid 
    group by bnid )  type_basket
    on type_browse.bnid= type_basket.bnid)
    , Tier_phase as(
    Select *,case when Tier='Tier 1' then PERCENTILE_CONT(cv_browse,0.60) over (partition by Tier) end as percentile_60_cv_browse_Tier1
            ,case when Tier='Tier 1' then PERCENTILE_CONT(average_order_price,0.80) over (partition by Tier) end  as percentile_80_order_tier1
            ,case when Tier='Tier 1' then PERCENTILE_CONT(average_order_price,0.50) over (partition by Tier) end as percentile_50_order_tier1
            ,case when Tier='Tier 2' then PERCENTILE_CONT(price_ratio,0.90) over (partition by Tier) end as percentile_90_priceratio_tier2
            ,case when CV_order_0_flag=1 then PERCENTILE_CONT(cv_order,0.60) over (partition by CV_order_0_flag ) end as percentile_60_cv_order_tier1
            ,case when Tier='Tier 3' then PERCENTILE_CONT(cv_browse,0.60) over (partition by Tier) end as percentile_60_cv_browse_tier3 
            ,case when Tier='Tier 3' then PERCENTILE_CONT(cv_browse,0.76) over (partition by Tier) end as percentile_75_cv_browse_tier3 
                   
    From (               
    Select *,
    case when average_order_price is not null then 'Tier 1'
         when average_order_price is null and  average_price_atb is not null then 'Tier 2'
         when average_browse_price is not null and average_order_price is null and  average_price_atb is null then 'Tier 3'
         end as tier,
    case when  (average_order_price is null and average_price_atb=0) then 0
         when  ( average_order_price is null and average_price_atb>0)
         then average_browse_price/average_price_atb end as price_ratio, 
    case when (cv_order<> 0 and cv_order is not null) then  1
         when (cv_order=0  and average_order_price is not null) then 0
         end CV_order_0_flag
    From  mastertable
    where average_browse_price is not null
    ))Select *except(CV_order_0_flag,percentile_60_cv_browse_Tier1,	
                      percentile_80_order_tier1,	percentile_50_order_tier1,	
                      percentile_90_priceratio_tier2,	percentile_60_cv_order_tier1,	
                      percentile_60_cv_browse_tier3,	percentile_75_cv_browse_tier3), 
                      case when (Tier='Tier 1'and  average_order_price<=percentile_50_order_tier1) then 'Low'
                   when  (Tier='Tier 1'and average_order_price>percentile_50_order_tier1 and average_order_price<=percentile_80_order_tier1) then 'Medium'
                   when (Tier='Tier 1'and average_order_price>percentile_80_order_tier1)  then 'High'
                   end tier_1_phase_price_based
                  ,case when Tier='Tier 1' and cv_order=0 and distinct_offer_key_order_count=1 then 'One time Shoppers'
                   when Tier='Tier 1' and cv_order=0 and distinct_offer_key_order_count>1 then 'Rational Shoppers'
                   when Tier='Tier 1' and (cv_order<=percentile_60_cv_order_tier1 and cv_browse<=percentile_60_cv_browse_tier1) and cv_order>0 then 'Rational Shoppers'
                   when Tier='Tier 1' and  cv_order>0 then 'Irrational Shoppers'
                   end as tier_1_shopper_type
                   , Case when Tier='Tier 2' and Price_ratio<=percentile_90_priceratio_tier2 then 'Rational A2B'
                          when Tier='Tier 2' and Price_ratio> percentile_90_priceratio_tier2 then 'Irrational A2B'
                          end as tier_2_shopper_type
                ,case when Tier='Tier 3' and  cv_browse<=percentile_60_cv_browse_tier3 then 'Fixed Orbited Shoppers'  
                      when Tier='Tier 3' and  cv_browse>percentile_60_cv_browse_tier3 and cv_browse<=percentile_75_cv_browse_tier3 then 'Agile Shoppers'
                      when Tier='Tier 3' and  cv_browse>percentile_75_cv_browse_tier3  then 'Volatile Shoppers'
                      end as tier_3_shopper_type
                
    From Tier_phase
    '''.format(project_name, start_date)
    return bql