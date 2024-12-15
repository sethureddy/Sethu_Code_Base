def up_seans_new_list():
    bql="""/* Sean's New List */

    create or replace table bnile-cdw-prod.o_customer.engagement_buyers_newlist as
    With base as 
    (
    SELECT distinct 
    ed.email_address,
    uph.email_address_key,
    order_date,
    basket_id,
    merch_category_rollup,
    coalesce(cast((case when offer_id=0 then null else offer_id end) as string), cast(setting_offer_id as string)) as offer_id,
    case when uph.merch_category_rollup in ('Loose Diamond') then sku end as ld_sku,
    subscribe_flag,
    last_email_open_date,
    case when uph.merch_category_rollup in ('Engagement') 
         then max(uph.usd_after_disc_sale_amount) over (partition by uph.email_address_key,order_date, basket_id, coalesce(offer_id, setting_offer_id))
    when uph.merch_category_rollup in ('Loose Diamond')
         then max(uph.usd_after_disc_sale_amount) over (partition by uph.email_address_key,order_date, basket_id, sku)
    when uph.merch_category_rollup not in ('Engagement','Loose Diamond')
         then max(uph.usd_after_disc_sale_amount) over (partition by uph.email_address_key,order_date, basket_id, offer_id)
    end as price, /* USD sale amount after discount is considered for order value */
    case when merch_category_rollup in ('Engagement','Loose Diamond') and lower(merch_product_category) like '%byo%' then 'BYO'
    when merch_category_rollup in ('Engagement','Loose Diamond') and lower(merch_product_category) like '%preset%' then 'Preset'
    when merch_category_rollup in ('Engagement','Loose Diamond') and lower(merch_product_category) is null and lower(sku) like '%ld%' then 'LD'
    end as eng_product_category, /* Engagement categories - BYO, Preset & LD */
    case when merch_category_rollup in ('Engagement','Loose Diamond') then 'Engagement'
    when merch_category_rollup in ('Diamond Jewelry','Bands','Other Jewelry') then 'Jewellery'
    end as Broad_product_category /* 2 broad overall categories for purchases - Engagement & Jewellery */

    FROM (Select * from `bnile-cdw-prod.o_customer.up_purchase_history` where return_date is null and usd_after_disc_sale_amount >0 )uph
    left join `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ed /* to get email_address against email_address_key */
    on uph.email_address_key = ed.EMAIL_ADDRESS_KEY
    left join
    (select distinct email_address, subscribe_flag from `bnile-cdw-prod.o_customer.email_customer_profile`) ecf /* email_address to bnid mapping from ecp table */
    on ecf.email_address = ed.email_address
    left join
    (
      SELECT distinct email_address, max(activity_date) as last_email_open_date /* last email open date */
      FROM `bnile-cdw-prod.o_warehouse.email_open_fact` eof
      Left join `bnile-cdw-prod.up_stg_customer.up_email_address_dim` upd
      on eof.up_email_address_key= cast(upd.up_email_address_key as string)
      group by email_address
    ) eof
    on ed.email_address= eof.email_address
    where uph.email_address_key in (
                                Select distinct email_address_key
                                From `bnile-cdw-prod.o_customer.up_purchase_history`
                                where order_date<=date_sub(current_date(), interval 18 month) and Merch_category_rollup in ('Engagement','Loose Diamond')
                                and return_date is null and usd_after_disc_sale_amount>0 
                                   ) /* Considering customers who made engagement purchase prior to 18 months */
    and ed.email_address not in ('none') and merch_category_rollup is not null
    )

    --- --#################### Engagement KPI's #####################

    , trans_cte as 
    (
    Select total_trans.*, eng_trans.eng_trans
    From 
    (
      Select email_address,email_address_key, count(*) as total_trans /* total number of purchases made */
      From 
      (
        Select distinct email_address, email_address_key, order_date, basket_id, offer_id, ld_sku
        From base
        where (basket_id is not null and (offer_id is not null or ld_sku is not null))
      )
      group by email_address,email_address_key
    ) total_trans
    left join 
    (
      Select email_address,email_address_key, count(*) as eng_trans /* total number of engagement purchases made */
      From 
      (
      Select distinct email_address, email_address_key, order_date, basket_id, offer_id, ld_sku
      From base
      where (basket_id is not null and (offer_id is not null or ld_sku is not null)) and Merch_category_rollup in ('Engagement','Loose Diamond')
      )
      group by email_address, email_address_key
    ) eng_trans
    on total_trans.email_address= eng_trans.email_address
    )

    , pj_cat_base as
    (
    Select distinct b.email_address, order_date , Broad_product_category as Initial_purchase_category /* to get category of initial purchased item */
    From base b
    join
    (
    Select distinct email_address,min(order_date) as min_order_dt
    From base
    group by email_address
    ) o_min
    on b.email_address=o_min.email_address and b.order_date=o_min.min_order_dt
    )

    , pj_cat as 
    (
    Select distinct main.*, 
    case when double.email_address is not null then 1 else 0 end as initial_purchase_eng_jwl /* customers whose initial purchase is both engagement & jewelry category item */
    From 
    (
    Select distinct email_address, order_date,
    first_value(Initial_purchase_category) over (partition by email_address, order_date order by Initial_purchase_category) as Initial_purchase_category
    From pj_cat_base
    Where Initial_purchase_category is not null
    ) main
    Left join (
    Select email_address
    from pj_cat_base
    group by email_address, order_date
    having count(distinct Initial_purchase_category)>1
    ) double
    on main.email_address=double.email_address
    )


    , init_eng_info as 
    (
    Select distinct * except(Initial_eng_cat_LD,Initial_eng_cat_BYO,Initial_eng_cat_Preset),
    first_value(Initial_eng_cat_LD) over(partition by email_address, email_address_key, first_eng_purchase_dt order by Initial_eng_cat_LD desc) as Initial_eng_cat_LD,
    first_value(Initial_eng_cat_BYO) over(partition by email_address, email_address_key, first_eng_purchase_dt order by Initial_eng_cat_BYO desc) as Initial_eng_cat_BYO,
    first_value(Initial_eng_cat_Preset) over(partition by email_address, email_address_key, first_eng_purchase_dt order by Initial_eng_cat_Preset desc) as Initial_eng_cat_Preset
    From 
    (
      Select distinct b.email_address, email_address_key, 
      order_date as first_eng_purchase_dt , /* first engagement purchase date */
      /* Initital Engagement purchase category flags - wheather it is BYO/LD/Preset */
      case when eng_product_category='LD' then 1 else 0 end as Initial_eng_cat_LD,
      case when eng_product_category='BYO' then 1 else 0 end as Initial_eng_cat_BYO,
      case when eng_product_category='Preset' then 1 else 0 end as Initial_eng_cat_Preset,
      sum(price) over (partition by b.email_address, email_address_key, order_date) as Initial_eng_price /* price of initial engagement item */
      From base b
      join 
      (
        Select distinct email_address,min(order_date) as min_order_dt_eng 
        FROM base
        where merch_category_rollup in ('Engagement','Loose Diamond') /* initial purchase must be engagement */
        group by email_address
      ) o_min_eng
      on b.email_address=o_min_eng.email_address and b.order_date=o_min_eng.min_order_dt_eng
      where merch_category_rollup in ('Engagement','Loose Diamond') 
    )
    )

    , fin as 
    /* Contains all the engagement KPI's */
    (
    Select distinct b.*, cat.* except (email_address), trn.* except (email_address,email_address_key), eng.* except (email_address,email_address_key)
    From
    (Select distinct email_address, email_address_key, subscribe_flag, last_email_open_date from base) b
    Left join
    (Select distinct email_address, Initial_purchase_category,initial_purchase_eng_jwl from pj_cat) cat
    on b.email_address=cat.email_address
    Left join trans_cte trn
    on trn.email_address=b.email_address
    Left join init_eng_info eng
    on b.email_address=eng.email_address
    )


    --- ---  ########## Band KPI's##########

    ,band_base as 
    /* Contains band purchases data after initial engagement purchase */
    (
    Select b.*,o_min_eng.min_ord_dt_eng 
    From base b
    join 
    (
      select distinct email_address,min(order_date) as min_ord_dt_eng
      from base
      where merch_category_rollup in ('Engagement','Loose Diamond') /* initial purchase must be engagement */
      group by email_address
    ) o_min_eng
    on b.email_address = o_min_eng.email_address and b.order_date >= o_min_eng.min_ord_dt_eng
    where merch_category_rollup in ('Bands') /* Band purchases after initial engagement purchase */
    )

    ,band_fin as
    (
    select purchase_dates.*,purchase_cnts.* except(email_address,email_address_key) from 
    (
      select 
      email_address,
      email_address_key,
      min(order_date) as first_band_purchase_dt,   /* first band purchase date */
      max(order_date) as last_band_purchase_dt,    /* last band purchase date */
      from band_base
      group by email_address,email_address_key
    ) purchase_dates /* first and last band purchase dates after initial engagement purchase */
    left join 
    (
      select email_address,email_address_key,count(*) as band_purchase_count,sum(price)/count(*) as AOV_band 
      from 
      (
        select email_address,email_address_key,order_date,basket_id,offer_id,price
        from band_base
        where basket_id is not null and offer_id is not null
      )
      group by email_address,email_address_key
    ) purchase_cnts /* band purchase counts and AOV of band purchases after initial engagement purchase*/
    on purchase_dates.email_address = purchase_cnts.email_address
    )

    -- ######### Non Band KPI's ###########


    ,non_band_base1 as /* contains non band category flags created on base data */
    (
    select *,
    case when merch_category_rollup = 'Diamond Jewelry' and defined_category = 'Bracelets' then 1 else 0 end DJ_Bracelets,
    case when merch_category_rollup = 'Diamond Jewelry' and defined_category = 'Earrings' then 1 else 0 end DJ_Earrings,
    case when merch_category_rollup = 'Diamond Jewelry' and defined_category = 'Necklaces' then 1 else 0 end DJ_Necklaces,
    case when merch_category_rollup = 'Diamond Jewelry' and defined_category = 'NonEngagement Ring' then 1 else 0 end DJ_NonEngagement_Ring,
    case when merch_category_rollup = 'Diamond Jewelry' and defined_category = 'Others' then 1 else 0 end DJ_Others,
    case when merch_category_rollup = 'Other Jewelry' and defined_category = 'Bracelets' then 1 else 0 end OJ_Bracelets,
    case when merch_category_rollup = 'Other Jewelry' and defined_category = 'Earrings' then 1 else 0 end OJ_Earrings,
    case when merch_category_rollup = 'Other Jewelry' and defined_category = 'Necklaces' then 1 else 0 end OJ_Necklaces,
    case when merch_category_rollup = 'Other Jewelry' and defined_category = 'NonEngagement Ring' then 1 else 0 end OJ_NonEngagement_Ring,
    case when merch_category_rollup = 'Other Jewelry' and defined_category = 'Others' then 1 else 0 end OJ_Others,
    from 
    (
    select *, 
    case when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='Stud Earrings' then 'Earrings'
    when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='Fashion Earrings' then 'Earrings'
    when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='Earrings' then 'Earrings'
    when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='Pendants' then 'Necklaces'
    when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='Necklaces' then 'Necklaces'
    when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='SolitairePendant' then 'Necklaces'
    when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='Fash Ring' then 'NonEngagement Ring'
    when merch_category_rollup in ('Diamond Jewelry') and product_class_name ='Bracelets' then 'Bracelets'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Pendants' then 'Necklaces'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Necklaces' then 'Necklaces'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='SolitairePendant' then 'Necklaces'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Fashion Pendants' then 'Necklaces'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Earrings' then 'Earrings'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Fashion Earrings' then 'Earrings'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Stud Earrings' then 'Earrings'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Bracelets' then 'Bracelets'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Fash Ring' then 'NonEngagement Ring'
    when merch_category_rollup in ('Other Jewelry') and product_class_name ='Rings' then 'NonEngagement Ring'
    else 'Others'
    end as defined_category

    from base bse
    left join /* to get product categories  */
    (
      select *except(mode) 
      from 
      (
      Select product_offer_key,product_class_name,
      row_number() over(partition by product_offer_key order by count(*) desc) mode
      from `bnile-cdw-prod.dssprod_o_warehouse.product_dim`
      group by product_offer_key,product_class_name
      )
      where mode = 1
      ) pd
    on bse.offer_id=cast(pd.product_offer_key as string)
    )
    )

    ,non_band_base2 as
    (
    /* contains data of all the non band purchases after first initial engagement purchase, including non band prod category flags  */
    select b.*,min_ord_dt_eng 
    from non_band_base1 b
    join 
    (
      select distinct email_address,min(order_date) as min_ord_dt_eng
      from base
      where merch_category_rollup in ('Engagement','Loose Diamond')     /* initial purchase must be engagement */
      group by email_address
    ) o_min_eng
    on b.email_address = o_min_eng.email_address and b.order_date >= o_min_eng.min_ord_dt_eng /* considering data after initial engagement purchase */
    where merch_category_rollup in ('Diamond Jewelry','Other Jewelry')   /* Non Band purchases after initial engagement purchase */
    )

    ,nonband_purchase_dts as 
    (
    /* first and last non band purchase dates after initial engagement purchase*/
    select 
    distinct email_address, email_address_key,
    first_value(first_order_dt_dj_bracelets) over (partition by email_address, email_address_key order by first_order_dt_dj_bracelets desc ) as first_order_dt_dj_bracelets,
    first_value(last_order_dt_dj_bracelets) over (partition by email_address, email_address_key order by last_order_dt_dj_bracelets desc ) as last_order_dt_dj_bracelets,
    first_value(first_order_dt_dj_earrings) over (partition by email_address, email_address_key order by first_order_dt_dj_earrings desc ) as first_order_dt_dj_earrings,
    first_value(last_order_dt_dj_earrings) over (partition by email_address, email_address_key order by last_order_dt_dj_earrings desc ) as last_order_dt_dj_earrings,
    first_value(first_order_dt_dj_necklace) over (partition by email_address, email_address_key order by first_order_dt_dj_necklace desc ) as first_order_dt_dj_necklace,
    first_value(last_order_dt_dj_necklace) over (partition by email_address, email_address_key order by last_order_dt_dj_necklace desc ) as last_order_dt_dj_necklace,
    first_value(first_order_dt_DJ_NonEngagement_Ring) over (partition by email_address, email_address_key order by first_order_dt_DJ_NonEngagement_Ring desc ) as first_order_dt_DJ_NonEngagement_Ring,
    first_value(last_order_dt_DJ_NonEngagement_Ring) over (partition by email_address, email_address_key order by last_order_dt_DJ_NonEngagement_Ring desc ) as last_order_dt_DJ_NonEngagement_Ring,
    first_value(first_order_dt_DJ_Others) over (partition by email_address, email_address_key order by first_order_dt_DJ_Others desc ) as first_order_dt_DJ_Others,
    first_value(last_order_dt_DJ_Others) over (partition by email_address, email_address_key order by last_order_dt_DJ_Others desc ) as last_order_dt_DJ_Others,

    first_value(first_order_dt_oj_bracelets) over (partition by email_address, email_address_key order by first_order_dt_oj_bracelets desc ) as first_order_dt_oj_bracelets,
    first_value(last_order_dt_oj_bracelets) over (partition by email_address, email_address_key order by last_order_dt_oj_bracelets desc ) as last_order_dt_oj_bracelets,
    first_value(first_order_dt_oj_earrings) over (partition by email_address, email_address_key order by first_order_dt_oj_earrings desc ) as first_order_dt_oj_earrings,
    first_value(last_order_dt_oj_earrings) over (partition by email_address, email_address_key order by last_order_dt_oj_earrings desc ) as last_order_dt_oj_earrings,
    first_value(first_order_dt_oj_necklace) over (partition by email_address, email_address_key order by first_order_dt_oj_necklace desc ) as first_order_dt_oj_necklace,
    first_value(last_order_dt_oj_necklace) over (partition by email_address, email_address_key order by last_order_dt_oj_necklace desc ) as last_order_dt_oj_necklace,
    first_value(first_order_dt_OJ_NonEngagement_Ring) over (partition by email_address, email_address_key order by first_order_dt_OJ_NonEngagement_Ring desc ) as first_order_dt_OJ_NonEngagement_Ring,
    first_value(last_order_dt_OJ_NonEngagement_Ring) over (partition by email_address, email_address_key order by last_order_dt_OJ_NonEngagement_Ring desc ) as last_order_dt_OJ_NonEngagement_Ring,
    first_value(first_order_dt_OJ_Others) over (partition by email_address, email_address_key order by first_order_dt_OJ_Others desc ) as first_order_dt_OJ_Others,
    first_value(last_order_dt_OJ_Others) over (partition by email_address, email_address_key order by last_order_dt_OJ_Others desc ) as last_order_dt_OJ_Others,

    From 
    (
    /* first non band purchase date after initial engagement purchase */
    select distinct email_address, email_address_key ,
    case when dj_bracelets=1 then min(order_date) over (partition by email_address, email_address_key, dj_bracelets) end as first_order_dt_dj_bracelets,
    case when DJ_Earrings=1 then min(order_date) over (partition by email_address, email_address_key, DJ_Earrings) end as first_order_dt_dj_earrings,
    case when dj_necklaces=1 then min(order_date) over (partition by email_address, email_address_key, dj_necklaces) end as first_order_dt_dj_necklace,
    case when DJ_NonEngagement_Ring=1 then min(order_date) over (partition by email_address, email_address_key, DJ_NonEngagement_Ring) end as first_order_dt_DJ_NonEngagement_Ring,
    case when DJ_Others=1 then min(order_date) over (partition by email_address, email_address_key, DJ_Others) end as first_order_dt_DJ_Others,
    case when Oj_bracelets=1 then min(order_date) over (partition by email_address, email_address_key, oj_bracelets) end as first_order_dt_oj_bracelets,
    case when OJ_Earrings=1 then min(order_date) over (partition by email_address, email_address_key, OJ_Earrings) end as first_order_dt_oj_earrings,
    case when Oj_necklaces=1 then min(order_date) over (partition by email_address, email_address_key, oj_necklaces) end as first_order_dt_oj_necklace,
    case when OJ_NonEngagement_Ring=1 then min(order_date) over (partition by email_address, email_address_key, OJ_NonEngagement_Ring) end as first_order_dt_OJ_NonEngagement_Ring,
    case when OJ_Others=1 then min(order_date) over (partition by email_address, email_address_key, OJ_Others) end as first_order_dt_OJ_Others,

    /* last non band purchase date after initial engagement purchase */
    case when dj_bracelets=1 then max(order_date) over (partition by email_address, email_address_key, dj_bracelets) end as last_order_dt_dj_bracelets,
    case when DJ_Earrings=1 then max(order_date) over (partition by email_address, email_address_key, DJ_Earrings) end as last_order_dt_dj_earrings,
    case when dj_necklaces=1 then max(order_date) over (partition by email_address, email_address_key, dj_necklaces) end as last_order_dt_dj_necklace,
    case when DJ_NonEngagement_Ring=1 then max(order_date) over (partition by email_address, email_address_key, DJ_NonEngagement_Ring) end as last_order_dt_DJ_NonEngagement_Ring,
    case when DJ_Others=1 then max(order_date) over (partition by email_address, email_address_key, DJ_Others) end as last_order_dt_DJ_Others,
    case when Oj_bracelets=1 then max(order_date) over (partition by email_address, email_address_key, oj_bracelets) end as last_order_dt_oj_bracelets,
    case when OJ_Earrings=1 then max(order_date) over (partition by email_address, email_address_key, OJ_Earrings) end as last_order_dt_oj_earrings,
    case when Oj_necklaces=1 then max(order_date) over (partition by email_address, email_address_key, oj_necklaces) end as last_order_dt_oj_necklace,
    case when OJ_NonEngagement_Ring=1 then max(order_date) over (partition by email_address, email_address_key, OJ_NonEngagement_Ring) end as last_order_dt_OJ_NonEngagement_Ring,
    case when OJ_Others=1 then max(order_date) over (partition by email_address, email_address_key, OJ_Others) end as last_order_dt_OJ_Others,

    from non_band_base2
    ) 
    )
    ,nonband_purchase_cnts_aov as 
    (
    /* Non Band Purchase counts and AOV after initial engagement purchase*/
    select 
    distinct email_address,email_address_key,
    first_value(dj_bracelets_purchase_count) over(partition by email_address,email_address_key order by dj_bracelets_purchase_count desc) as dj_bracelets_purchase_count,
    first_value(dj_earrings_purchase_count) over(partition by email_address,email_address_key order by dj_earrings_purchase_count desc) as dj_earrings_purchase_count,
    first_value(dj_necklaces_purchase_count) over(partition by email_address,email_address_key order by dj_necklaces_purchase_count desc) as dj_necklaces_purchase_count,
    first_value(dj_NonEngagement_ring_purchase_count) over(partition by email_address,email_address_key order by dj_NonEngagement_ring_purchase_count desc) as dj_NonEngagement_ring_purchase_count,
    first_value(dj_others_purchase_count) over(partition by email_address,email_address_key order by dj_others_purchase_count desc) as dj_others_purchase_count,

    first_value(oj_bracelets_purchase_count) over(partition by email_address,email_address_key order by oj_bracelets_purchase_count desc) as oj_bracelets_purchase_count,
    first_value(oj_earrings_purchase_count) over(partition by email_address,email_address_key order by oj_earrings_purchase_count desc) as oj_earrings_purchase_count,
    first_value(oj_necklaces_purchase_count) over(partition by email_address,email_address_key order by oj_necklaces_purchase_count desc) as oj_necklaces_purchase_count,
    first_value(oj_NonEngagement_ring_purchase_count) over(partition by email_address,email_address_key order by oj_NonEngagement_ring_purchase_count desc) as oj_NonEngagement_ring_purchase_count,
    first_value(oj_others_purchase_count) over(partition by email_address,email_address_key order by oj_others_purchase_count desc) as oj_others_purchase_count,

    first_value(AOV_dj_bracelets) over(partition by email_address,email_address_key order by AOV_dj_bracelets desc) as AOV_dj_bracelets,
    first_value(AOV_dj_earrings) over(partition by email_address,email_address_key order by AOV_dj_earrings desc) as AOV_dj_earrings,
    first_value(AOV_dj_necklaces) over(partition by email_address,email_address_key order by AOV_dj_necklaces desc) as AOV_dj_necklaces,
    first_value(AOV_dj_NonEngagement_ring) over(partition by email_address,email_address_key order by AOV_dj_NonEngagement_ring desc) as AOV_dj_NonEngagement_ring,
    first_value(AOV_dj_others) over(partition by email_address,email_address_key order by AOV_dj_others desc) as AOV_dj_others,

    first_value(AOV_oj_bracelets) over(partition by email_address,email_address_key order by AOV_oj_bracelets desc) as AOV_oj_bracelets,
    first_value(AOV_oj_earrings) over(partition by email_address,email_address_key order by AOV_oj_earrings desc) as AOV_oj_earrings,
    first_value(AOV_oj_necklaces) over(partition by email_address,email_address_key order by AOV_oj_necklaces desc) as AOV_oj_necklaces,
    first_value(AOV_oj_NonEngagement_ring) over(partition by email_address,email_address_key order by AOV_oj_NonEngagement_ring desc) as AOV_oj_NonEngagement_ring,
    first_value(AOV_oj_others) over(partition by email_address,email_address_key order by AOV_oj_others desc) as AOV_oj_others,

    from 
    (
    select distinct email_address,email_address_key,
    /* Non Band Purchase counts after initial engagement purchase*/
    case when dj_bracelets = 1 then count(*) over (partition by email_address,email_address_key,dj_bracelets) end as dj_bracelets_purchase_count,
    case when dj_earrings = 1 then count(*) over (partition by email_address,email_address_key,dj_earrings) end as dj_earrings_purchase_count,
    case when dj_necklaces = 1 then count(*) over (partition by email_address,email_address_key,dj_necklaces) end as dj_necklaces_purchase_count,
    case when dj_NonEngagement_ring = 1 then count(*) over (partition by email_address,email_address_key,dj_NonEngagement_ring) end as dj_NonEngagement_ring_purchase_count,
    case when dj_others = 1 then count(*) over (partition by email_address,email_address_key,dj_others) end as dj_others_purchase_count,

    case when oj_bracelets = 1 then count(*) over (partition by email_address,email_address_key,oj_bracelets) end as oj_bracelets_purchase_count,
    case when oj_earrings = 1 then count(*) over (partition by email_address,email_address_key,oj_earrings) end as oj_earrings_purchase_count,
    case when oj_necklaces = 1 then count(*) over (partition by email_address,email_address_key,oj_necklaces) end as oj_necklaces_purchase_count,
    case when oj_NonEngagement_ring = 1 then count(*) over (partition by email_address,email_address_key,oj_NonEngagement_ring) end as oj_NonEngagement_ring_purchase_count,
    case when oj_others = 1 then count(*) over (partition by email_address,email_address_key,oj_others) end as oj_others_purchase_count,

    /* AOV of non band purchases after initital engagement purchase  */
    case when dj_bracelets = 1 then (sum(price) over (partition by email_address,email_address_key,dj_bracelets))/(count(*) over (partition by email_address,email_address_key,dj_bracelets)) end as AOV_dj_bracelets,
    case when dj_earrings = 1 then (sum(price) over (partition by email_address,email_address_key,dj_earrings))/(count(*) over (partition by email_address,email_address_key,dj_earrings)) end as AOV_dj_earrings,
    case when dj_necklaces = 1 then (sum(price) over (partition by email_address,email_address_key,dj_necklaces))/(count(*) over (partition by email_address,email_address_key,dj_necklaces)) end as AOV_dj_necklaces,
    case when dj_NonEngagement_ring = 1 then (sum(price) over (partition by email_address,email_address_key,dj_NonEngagement_ring))/(count(*) over (partition by email_address,email_address_key,dj_NonEngagement_ring)) end as AOV_dj_NonEngagement_ring,
    case when dj_others = 1 then (sum(price) over (partition by email_address,email_address_key,dj_others))/(count(*) over (partition by email_address,email_address_key,dj_others)) end as AOV_dj_others,

    case when oj_bracelets = 1 then (sum(price) over (partition by email_address,email_address_key,oj_bracelets))/(count(*) over (partition by email_address,email_address_key,oj_bracelets)) end as AOV_oj_bracelets,
    case when oj_earrings = 1 then (sum(price) over (partition by email_address,email_address_key,oj_earrings))/(count(*) over (partition by email_address,email_address_key,oj_earrings)) end as AOV_oj_earrings,
    case when oj_necklaces = 1 then (sum(price) over (partition by email_address,email_address_key,oj_necklaces))/(count(*) over (partition by email_address,email_address_key,oj_necklaces)) end as AOV_oj_necklaces,
    case when oj_NonEngagement_ring = 1 then (sum(price) over (partition by email_address,email_address_key,oj_NonEngagement_ring))/(count(*) over (partition by email_address,email_address_key,oj_NonEngagement_ring)) end as AOV_oj_NonEngagement_ring,
    case when oj_others = 1 then (sum(price) over (partition by email_address,email_address_key,oj_others))/(count(*) over (partition by email_address,email_address_key,oj_others)) end as AOV_oj_others,

    from 
    (
    select email_address,email_address_key,order_date,basket_id,offer_id,price,
           dj_bracelets,dj_earrings,dj_necklaces,dj_NonEngagement_ring,dj_others,
           oj_bracelets,oj_earrings,oj_necklaces,oj_NonEngagement_ring,oj_others,              
    from non_band_base2
    where basket_id is not null and offer_id is not null
    )
    )
    )

    select f.*,b.* except(email_address,email_address_key),nbd.* except(email_address,email_address_key),nba.* except(email_address,email_address_key),
    overall_aov.* except(email_address,email_address_key)
    from fin f    /* Contains Engagement KPI's */
    left join band_fin b /* Contains first and last band purchase dates, number of band purchases and aov, after initial engagement purchase */
    on f.email_address = b.email_address
    left join nonband_purchase_dts nbd /* Contains first and last non band purchase dates at product category level after initial engagement purchase */
    on f.email_address = nbd.email_address
    left join nonband_purchase_cnts_aov nba /* Contains number of non band purchases and their aov at prod category level after initial eng purchase */
    on f.email_address = nba.email_address
    left join 
    (
      select email_address,email_address_key,sum(price)/count(*) as Overall_AOV_NonEngagement 
      from 
      (
      select email_address,email_address_key,order_date, basket_id, offer_id,price 
      from base 
      where (basket_id is not null and offer_id is not null) and merch_category_rollup not in ('Engagement','Loose Diamond')
      )
      group by email_address,email_address_key
    ) overall_aov /* Contains overall AOV for non engagement purchases. It will include non eng purchase even before first eng purchase */
    on f.email_address = overall_aov.email_address
    """

    return bql
