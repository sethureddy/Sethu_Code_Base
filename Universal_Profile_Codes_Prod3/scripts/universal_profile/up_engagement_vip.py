def up_engagement_vip():
    bql="""
    create or replace table `bnile-cdw-prod.o_customer.vip_segmentation_engagement` as  

    -- taking 2 years of engagement data rfm buckets
    with engagement_base as
    (
    select distinct
    email_address,
    email_address_key,
    basket_id, 
    order_date,
    case when (offer_id is null or offer_id=0) then LD_sku else cast(offer_id as string) end as product, 
    purchase_category,
    price
    from
    (
    select *
    from
    (
    SELECT distinct
    ed.EMAIL_ADDRESS,
    uph.email_address_key,
    uph.basket_id,
    coalesce(offer_id, setting_offer_id) as offer_id,
    case when uph.merch_category_rollup in ('Loose Diamond') then sku else null end as LD_sku,
    uph.order_date,
    case when uph.merch_category_rollup in ('Engagement') then
    max(uph.usd_after_disc_sale_amount) over (partition by uph.email_address_key,order_date, basket_id, coalesce(offer_id, setting_offer_id))
    when uph.merch_category_rollup in ('Loose Diamond')
    then max(uph.usd_after_disc_sale_amount) over (partition by uph.email_address_key,order_date, basket_id, sku) end as price
    , --USD sale amount after discount taken as 'order value' for a transaction(offer_id)-- 2 broad overall category for purchases
    case when uph.merch_category_rollup in ('Diamond Jewelry','Other Jewelry','Bands') then 'Jewelry'
    when uph.merch_category_rollup in ('Engagement','Loose Diamond') then 'Engagement'
    else 'None' -- for some rows merch_category_rollup is null
    end as purchase_category
    FROM `bnile-cdw-prod.o_customer.up_purchase_history` uph --base table
    left join `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ed --to get email_ids against email_address_key
    on uph.email_address_key = ed.EMAIL_ADDRESS_KEY
    where return_date is null --only consider orders which have not been returned
    and ed.email_address is not null --remove rows where email_id is not available(~250 distinct email_address_key do not have email_address)
    and ed.email_address not in ('none') --removes rows where email address is none
    )
    where purchase_category = 'Engagement' and price > 0 and order_date is not null--filter engagement purchases and orders which have have value>0
    )
    )
    -- table for rfm analysis with calculated fields
    ,
    base_rfm as
    (
    select * from
    (
    select distinct
    email_address,
    sum(price)/count(*) as AOV, /*average order value*/
    count(*) as transactions, /*total purchases(products bought) till date*/
    max(order_date) as last_purchase_date,
    date_diff(current_date(), max(order_date), month) as recency_in_months,
    date_diff(current_date(), max(order_date), day) as recency_in_days, /*recency*/
    from engagement_base
    group by EMAIL_ADDRESS, email_address_key /*aggregated over email_address level*/
    )
    where (email_address is not null 
            and AOV is not null
            and transactions is not null
            and recency_in_days is not null)		
    )
    --###### aov rank ######
    ,rank_aov as
    (
    select 
    email_address,
    AOV,
    -- ntile(3) over(order by AOV) as aov_rank
    case when AOV <= 3500 then 1
    when AOV > 3500 and AOV <= 7500 then 2
    when AOV > 7500 and AOV <= 10500 then 3
    when AOV > 10500 and AOV <= 17000 then 4
    when AOV > 17000 and AOV <= 25000 then 5
    else 3
    end as aov_rank
    from base_rfm
    )

    ###### transactions rank ######
    ,rank_trans as 
    (
    select 
    email_address,
    transactions,
    case when transactions = 1 then 1
    when transactions >1 then 2
    end as transactions_rank
    from base_rfm
    )

    --##### recency rank ######
    ,rank_recency as 
    (
    select 
    email_address,
    recency_in_days,
    case when recency_in_days <=90 then 3
    when recency_in_days > 90 and recency_in_days <= 365 then 2
    else 1
    end as recency_rank
    from base_rfm
    )

    select *,
    case when AOV > 25000 then 'VIP Customers'
    when RFM_rank in ('322','422','323','423','522','523') then 'High Value'
    when RFM_rank in ('413','513','313','312','412','512') then 'Potential High Value'
    when RFM_rank in ('212','213','112','113') then 'Potential Promising'
    when RFM_rank in ('122','123','222','223') then 'Promising'
    when RFM_rank in ('221','311','411','511','521','421','321','211') then 'Winback'
    when RFM_rank in ('121','111') then 'Churned'
    end as segments
    from (
        select 
        a.email_address,
        aov,
        transactions,
        recency_in_days,
        aov_rank,
        transactions_rank,
        recency_rank,
        concat(aov_rank,transactions_rank,recency_rank) as RFM_rank
        from rank_aov a
        join rank_trans t
        on a.email_address = t.email_address
        join rank_recency r
        on t.email_address = r.email_address
        )
    """

    return bql
