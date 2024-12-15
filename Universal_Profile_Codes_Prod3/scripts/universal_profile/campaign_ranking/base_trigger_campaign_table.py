def base_trigger_campaign_table(params):
    project_name=params["project_name"]
    dataset_name1 = params["dataset_name1"]
    dataset_name2 = params["dataset_name2"]
    
    bql = f'''With email_metrics as (
    select
        ems.placement_key,
          placement_category_type_name, -- email_type
          pd.placement_category_name,   -- email_campaign     
          pd.placement_name,
        full_date
        , sum(email_send_count) as sends
        , sum(email_unique_click_count) as clicks
    from `{project_name}.{dataset_name1}.email_metrics_summary`  ems
    inner join `{project_name}.{dataset_name1}.placement_dim` pd
        on pd.placement_key = ems.placement_key
    inner join `{project_name}.{dataset_name1}.date_dim` dd
        on dd.date_key = ems.date_key
    where 1=1
        and dd.full_date between 
        date_sub(date_trunc(current_date(), month), interval 1 year) and date_sub(date_trunc(current_date(), month), interval 1 day) 
        -- 1 year duration till last month
    group by  ems.placement_key,   placement_category_type_name, -- email_type
                 pd.placement_category_name, -- email_campaign        
                 pd.placement_name, full_date
)
,orders_by_placement as (
SELECT
        il.placement_key,
         os.initial_email_addr_key as email_address_key,
         os.initial_basket_id,
         os.initial_sale_amount,
         os.initial_cost_amount,
         os.initial_order_date_key,
         placement_name,
         Placement_category_name,
         placement_category_type_name
    FROM `{project_name}.{dataset_name1}.order_summary`  os,
         `{project_name}.{dataset_name1}.date_dim` dd,
         (SELECT ifnull (placement_category_type_name, '-')
                     placement_category_type_name, -- email_type
                 pd.placement_category_name, -- email_campaign       
                 pd.placement_name,
                 iloe.*
            FROM `{project_name}.{dataset_name1}.placement_dim` pd,
                 (SELECT oe.placement_key,
                         oe.order_id,
                                    os.initial_submit_date_key,
                                 date_diff(parse_date('%Y%m%d', cast(( os.INITIAL_SUBMIT_DATE_KEY) AS string) ), oe.event_date, day)
                             days_before_purchase,
                         ROW_NUMBER () OVER (PARTITION BY oe.order_id ORDER BY oe.event_date DESC) row_id
                    FROM `{project_name}.{dataset_name1}.order_summary` os
                      join `{project_name}.{dataset_name1}.order_event_summary` oe
                      on os.initial_basket_id = oe.order_id
                       join `{project_name}.{dataset_name1}.date_dim` dd
                       on oe.order_date_key = dd.date_key 
                       and dd.full_date between date_sub(date_trunc(current_date(), month), interval 1 year) and date_sub(date_trunc(current_date(), month), interval 1 day) 
                       -- date duration 1 year
                    AND oe.event_type_key NOT IN (23,42,45,63,64,65,81,82,83,84,85,101,102)
                   ) iloe
           WHERE     1 = 1
                 AND pd.placement_key = iloe.placement_key -- inner join to include placement only
                 AND iloe.row_id = 1 -- last click only - (excluding source codes) see inner partition statemenst
                 AND iloe.days_before_purchase >= 0 -- only include if click before order submit
                 AND iloe.days_before_purchase <= 30 -- only include if within 30 days of purchase
                 AND ifnull (pd.placement_category_type_name, '-') !=
                 '-') il
    WHERE     1 = 1
         AND os.initial_basket_id = il.order_id
         and dd.date_key = os.initial_order_date_key
         and dd.full_date between 
         date_sub(date_trunc(current_date(), month), interval 1 year) and date_sub(date_trunc(current_date(), month), interval 1 day)  --now_date
         )
   , orders_agg as (  
   -- Aggregating orders  info at placement, date level
        Select
        obp.Placement_category_type_name,
        obp.Placement_category_name,
        obp.placement_name,
        obp.placement_key,
        dd.full_date
        , sum(obp.initial_sale_amount) as revenue
        , sum(obp.initial_sale_amount - obp.initial_cost_amount) as margin
        , count(distinct initial_basket_id) as orders
    from orders_by_placement obp
    inner join `{project_name}.{dataset_name1}.date_dim` dd
        on dd.date_key = obp.initial_order_date_key
    where 1=1
         and dd.full_date between  date_sub(date_trunc(current_date(), month), interval 1 year) and  date_sub(date_trunc(current_date(), month), interval 1 day) -- now_date
    group by
        placement_name, Placement_category_type_name, Placement_category_name,placement_key, full_date
        )
        ,
        fin_agg_table as (
        select * from (
        Select 
        coalesce(em.placement_key, obp.placement_key) as placement_key,
        coalesce(em.placement_name, obp.placement_name) as placement_name,
        coalesce( em.placement_category_name, obp.placement_category_name) as placement_category_name,
        coalesce( em.placement_category_type_name, obp.placement_category_type_name) as placement_category_type_name,
        coalesce(em.full_date,obp.full_date) as full_date,
        revenue, 
        margin,
        orders,
        sends,
        clicks,
        From orders_agg obp
        full outer join email_metrics em
       on obp.placement_key = em.placement_key and obp.full_date=em.full_date
       ) where sends>0 or clicks>0 or revenue is not null or margin is not null
       )
      
,
derived_table as  (
 -- Aggregating Orders Info and Email metrics info at a month level
select placement_key,
        placement_name, 
        placement_category_name, 
        placement_category_type_name,
        year, month_no, month,
        count(distinct full_date) as days_run, -- frequency of days the campaign has been run
        sum(revenue) as revenue, 
        sum(margin) as margin, 
        sum(orders) as orders,
        sum(sends) as sends, 
        sum(clicks) as clicks, 
        sum(clicks)/nullif(sum(sends),0) as click_ratio,
From (
Select *
From (
Select *, extract(year from full_date) as year, extract(month from full_date) as month_no, FORMAT_DATETIME("%B", DATETIME(full_date)) as month
From fin_agg_table
))
where placement_category_type_name='Ongoing' and placement_key in (
138917,138916,138915,138934,139914,139915,139916,148197,148198,148160,148200,148199,148161,147261,140854,127561,130380,127543,128049,127541,127571,128044,127560,127559,127558,128074,128075,128045,128076,127570,128078,127572,130436,127816,140554,127542,64744,83575,112160,71844,146185,127815,148358,140140,140136,140143,140145,140141,140175,141316,141314,141315,141317,140234,140151,140149,140147,140146,140235,140157,140156,140154,140153,145767,145769,145766,145765,145764,145774,145775,145773,145772,145771
) -- 73 required campaigns based on list. Any new campaigns to be added here
group by  placement_key, placement_name, placement_category_name, placement_category_type_name, year,month_no,month
),
last_month as (
-- Last run month for every campaign
select placement_key, extract(month from  max(full_date)) as last_month, extract(year from  max(full_date)) as last_year
From fin_agg_table
where placement_key in (Select distinct placement_key from  derived_table)
group by placement_key
),
camp_name as (
-- Finding Campaign ID for the respective placement key
SELECT distinct placement_key,esf.campaign_id
FROM `{project_name}.{dataset_name2}.email_send_fact` esf -- Taking campaign ID
where cast(placement_key as int64) in (Select distinct placement_key from  derived_table) 
)
,
base as (
Select distinct dt.*,campaign_id
from derived_table dt
join last_month lmr
on dt.placement_key=lmr.placement_key and dt.month_no=lmr.last_month and dt.year=lmr.last_year
left join camp_name cmp
on cast(dt.placement_key as string)=cmp.placement_key
)

Select distinct *
From base
'''.format(project_name, dataset_name1, dataset_name2)
    return bql