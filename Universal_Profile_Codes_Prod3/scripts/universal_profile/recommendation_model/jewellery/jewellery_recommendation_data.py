def recommendation_jewellery(params):
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name1 = params["dataset_name1"]
    dataset_name2 = params["dataset_name2"]
    dataset_name3 = params["dataset_name3"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""with bnid_jewellery_interaction as(
    Select a.*, dp.PRODUCT_LIST_PRICE
    from
    (Select  distinct pd.* from (
    select  bnid, B.date_key,  B.offer_id, view_cnt, A.*
    from (select offer_id, bnid, date_key, sum(view_cnt) as view_cnt
    from `{project_name}.{dataset_name}.bn_daily_product_views` 
    group by offer_id, bnid, date_key)B
    JOIN (select *except(mode) from (
    Select product_offer_key, CHAIN_LENGTH_INCHES,CHAIN_WIDTH_MM,CLASP_TYPE,CHAIN_TYPE,GRAM_WEIGHT,
    MOUNTING_STYLE,PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SURFACE_MARKINGS,PRIMARY_NACRE_THICKNESS,
    PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,product_class_name ,merch_category_rollup,PRODUCT_SUB_CLASS_NAME ,PRODUCT_CATEGORY_TYPE,Merch_product_category,
    row_number() over(partition by product_offer_key  order by count(*) desc) mode
    from `{project_name}.{dataset_name_dssprod}.product_dim`
    group by product_offer_key,CHAIN_LENGTH_INCHES,CHAIN_WIDTH_MM,CLASP_TYPE,CHAIN_TYPE,GRAM_WEIGHT,
    MOUNTING_STYLE,PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SURFACE_MARKINGS,PRIMARY_NACRE_THICKNESS,
    PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,product_class_name ,merch_category_rollup,PRODUCT_SUB_CLASS_NAME,PRODUCT_CATEGORY_TYPE,Merch_product_category )
    where mode = 1) A
    ON B.offer_id = A.product_offer_key
    and A.PRODUCT_SUB_CLASS_NAME not in ('Diamonds')
    and A.PRODUCT_CATEGORY_TYPE not in ('BADSKU')
    and A.PRODUCT_CATEGORY_TYPE is not null
    where MERCH_CATEGORY_ROLLUP in ('nan','Other Jewelry','Bands','Diamond Jewelry') and Product_sub_class_name not in ('nan','BYO 3 Stone','BYO 5 Stone Ring','BYO Ring','BYO STONE') and product_class_name not in ('BYO  STONE'))as pd
    inner join
    (select distinct offer_id from `{project_name}.{dataset_name3}.daily_offer_fact` where  any_product_sellable='Yes' and parse_date('%Y%m%d',cast(date_key as string)) = date_sub(parse_date('%Y%m%d',cast({delta_date} as string)),interval 1 day)) as dof
    on pd.OFFER_ID=dof.offer_id
    where  parse_date('%Y%m%d', cast( date_key as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 2 month) and parse_date('%Y%m%d', cast({delta_date} as string))) a
    inner join (Select avg(PRODUCT_LIST_PRICE) as PRODUCT_LIST_PRICE , offer_key from `{project_name}.{dataset_name_dssprod}.daily_product_fact`
    group by offer_key) dp
    on a.OFFER_id= dp.offer_key),
    pii_info as(
    select rgk.*,cf.guid_key,peg.PAGE_GROUP_NAME ,peg.activity_set_code,primary_browse_segment, offer_key from bnid_jewellery_interaction  rgk
    left join `{project_name}.{dataset_name}.bn_guid_all`    bgm 
    on lower(rgk.bnid) = lower(bgm.bnid)
    left join`{project_name}.{dataset_name_dssprod}.email_guid` eg 
                on bgm.guid = eg.guid
    left join `{project_name}.{dataset_name_dssprod}.click_fact` cf
    on eg.guid_key = cf.guid_key and rgk.offer_id = cf.offer_key and rgk.date_key = cf.date_key
    left join `{project_name}.{dataset_name_dssprod}.page_group_dim` peg
    on cf.PAGE_GROUP_KEY = peg.page_group_key
    ),
    flag as(
    select *,max(add_to_cart) over(partition by bnid,date_key, offer_id) add_to_basket,max(purchase) over(partition by bnid,date_key, offer_id) item_purchase
    from (
    select pii_info.*,case when activity_set_code in ('add item to basket','add diamond to basket')  and primary_browse_segment in ('wedding_band','diamond_jewelry','other_jewelry','misc_jewelry')then 1 else 0 end as add_to_cart, case when customer_submit_date_key is null then 0 else 1 end as purchase
    from pii_info
    left join (select pof customer_submit_date_key,offer_key,guid_key from `{project_name}.{dataset_name_dssprod}.product_order_fact` pof
                join  `{project_name}.{dataset_name_dssprod}.product_dim` pd
                on pof.product_key = pd.product_key and MERCH_CATEGORY_ROLLUP in ('nan','Other Jewelry','Diamond Jewelry','Bands')  and
                Product_sub_class_name not in ('nan','BYO 3 Stone','BYO 5 Stone Ring','BYO Ring','BYO STONE')
                where  pof.CANCEL_DATE_KEY is null and pof.RETURN_ACCEPTED_DATE_KEY is null and parse_date('%Y%m%d', cast( pof.CUSTOMER_SUBMIT_DATE_KEY as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 2 month) and parse_date('%Y%m%d', cast({delta_date} as string))) pof
    on pii_info.guid_key  = pof.guid_key and pii_info.offer_id = pof.offer_key))
    select distinct flag.*except(add_to_basket,item_purchase,add_to_cart,purchase,PAGE_GROUP_NAME,activity_set_code,guid_key,primary_browse_segment,offer_key),  first_value(add_to_basket) over(partition by flag.bnid,date_key, offer_id order by add_to_basket desc)  add_to_basket,first_value(item_purchase) over(partition by flag.bnid,date_key, offer_id order by item_purchase desc)item_purchase,derived_gender,uea.marital_status ,uea.primary_currency_code  from flag
    left join `{project_name}.{dataset_name2}.bn_customer_profile` uca
    on flag.bnid = uca.bnid
    left join  `{project_name}.{dataset_name2}.bn_explicit_attributes` uea
    on flag.bnid = uea.bnid""".format(
        project_name,
        dataset_name,
        dataset_name1,
        dataset_name2,
        dataset_name3,
        dataset_name_dssprod,
        delta_date,
    )
    return bql
