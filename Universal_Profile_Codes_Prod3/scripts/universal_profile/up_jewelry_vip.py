def up_jewelry_vip():
    bql="""
    create or replace table `bnile-cdw-prod.o_customer.vip_segmentation_jewelry` as  

    -- taking 2 years of jewelry data rfm buckets
    with jewelry_base as
    (
    select *
    from
    (
    SELECT distinct
    ed.EMAIL_ADDRESS,
    embd.bnid,
    uph.email_address_key,
    uph.basket_id,
    uph.order_date,
    coalesce(uph.offer_id,uph.setting_offer_id) as offer_id,
    max(uph.usd_after_disc_sale_amount) over (partition by uph.email_address_key,order_date, basket_id, coalesce(offer_id, setting_offer_id)) as price,
    /*USD sale amount after discount taken as 'order value' for a transaction(offer_id)*/

    case when uph.merch_category_rollup in ('Diamond Jewelry','Other Jewelry','Bands') then 'Jewelry'
    when uph.merch_category_rollup in ('Engagement', 'Loose Diamond') then 'Engagement'
    else 'None' /* for some rows merch_category_rollup is not available */
    end as purchase_category
    from
    (select distinct * from `bnile-cdw-prod.o_customer.up_purchase_history`) uph
    left join `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ed on uph.email_address_key = ed.EMAIL_ADDRESS_KEY /*to get email_ids against email_address_key*/
    left join
    (select distinct email_address, bnid from `bnile-cdw-prod.o_customer.email_customer_profile`) embd on embd.email_address = ed.email_address /*email_address to bnid mapping from ecp table*/
    where return_date is null /*only consider orders which have not been returned*/
    and ed.email_address is not null /*remove rows where email_id is not available*/
    and ed.email_address not in ('none') /*removes rows where email address is none*/
    and uph.order_date between date_add(current_date(), INTERVAL - 2 year) and current_date() --taking 2 years of data
    )
    where purchase_category = 'Jewelry' and price > 0
    /*filter jewelry purchases and orders which have have value > 0*/
    )


    -- rfm analysis
    ,base_rfm as
    (
    select distinct
    email_address,
    sum(price)/count(*) as AOV, /*average order value*/
    count(*) as transactions, /*total purchases(products bought) till date*/
    max(order_date) as last_purchase_date,
    date_diff(current_date(), max(order_date), month) as recency_in_months,
    date_diff(current_date(), max(order_date), day) as recency_in_days, /*recency*/
    from jewelry_base
    group by EMAIL_ADDRESS, email_address_key /*aggregated over email_address level*/
    )


    ###### aov rank ######
    ,rank_aov as
    (
    select 
    email_address,
    AOV,
    -- ntile(3) over(order by AOV) as aov_rank
    case when AOV <= 300 then 1
    when AOV > 300 and AOV <= 1000 then 2
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
    when transactions >1 and  transactions <= 4 then 2
    else 3
    end as transactions_rank
    from base_rfm
    )

    ##### recency rank ######
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
    case when AOV > 3000 then 'VIP Customers'
    when RFM_rank in ('333','323','332','322') then 'High Value'
    when RFM_rank in ('313','312') then 'Potential High Value'
    when RFM_rank in ('213','212','133','112','113') then 'Potential Promising'
    when RFM_rank in ('222','133','123','122','233','223','232','132') then 'Promising'
    when RFM_rank in ('321','331','231','221','131','311','211') then 'Winback'
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
