def recommendation_settings(params):
    '''
    customer interaction data with setting skus
    with customers pii data and setting feature
    '''
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name1 = params["dataset_name1"]
    dataset_name2 = params["dataset_name2"]
    dataset_name3 = params["dataset_name3"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""with bnid_setting_interaction as(
    Select a.*, dpf.PRODUCT_LIST_PRICE
    from
    (Select  distinct pd.* from (
    select  bnid, B.date_key,  B.offer_id, view_cnt, A.*
    from (select offer_id, bnid, date_key, sum(view_cnt) as view_cnt
    from `{project_name}.{dataset_name}.bn_daily_product_views`

    group by offer_id, bnid, date_key)B
    JOIN (select *except(mode) from (
    Select PRODUCT_OFFER_KEY, PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE
    ,PRODUCT_CLASS_NAME,PRODUCT_SUB_CLASS_NAME,PRODUCT_CATEGORY_TYPE,MERCH_PRODUCT_CATEGORY,MERCH_CATEGORY_ROLLUP,
    row_number() over(partition by product_offer_key  order by count(*) desc) mode
    from {project_name}.{dataset_name_dssprod}.product_dim

    group by PRODUCT_OFFER_KEY, PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,
    PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,product_class_name,MERCH_CATEGORY_ROLLUP,MERCH_PRODUCT_CATEGORY,PRODUCT_SUB_CLASS_NAME,PRODUCT_CLASS_NAME,PRODUCT_CATEGORY_TYPE )
    where mode = 1) A
    ON B.offer_id = A.product_offer_key
    and A.PRODUCT_SUB_CLASS_NAME not in ('Diamonds')
    and A.PRODUCT_CATEGORY_TYPE not in ('BADSKU')
    and A.PRODUCT_CATEGORY_TYPE is not null
    where A.MERCH_CATEGORY_ROLLUP in ('Engagement') and (not(PRIMARY_METAL_COLOR) = 'nan' or     not(PRIMARY_METAL_NAME) = 'nan' or    not(PRIMARY_SHAPE_NAME) = 'nan' or    not(PRIMARY_SETTING_TYPE) = 'nan' or not(PRIMARY_STONE_TYPE) = 'nan') ) as pd
    inner join  (select distinct offer_id from `{project_name}.{dataset_name3}.daily_offer_fact` where  any_product_sellable='Yes' and parse_date('%Y%m%d',cast(date_key as string)) = date_sub(parse_date('%Y%m%d',cast({delta_date} as string)),interval 1 day)) as dof
	on pd.OFFER_ID=dof.offer_id
    and parse_date('%Y%m%d', cast(date_key as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 6 month) and parse_date('%Y%m%d', cast({delta_date} as string))) a
    join
    (select offer_key,avg(PRODUCT_LIST_PRICE) PRODUCT_LIST_PRICE from `{project_name}.{dataset_name_dssprod}.daily_product_fact` group by 1) dpf
    on a.offer_id = dpf.offer_key),

    pii_info as(
    select rgk.*,cf.guid_key,peg.PAGE_GROUP_NAME , offer_key from bnid_setting_interaction  rgk
    left join {project_name}.{dataset_name}.bn_guid_all   bgm

    on lower(rgk.bnid) = lower(bgm.bnid)
    left join `{project_name}.{dataset_name_dssprod}.email_guid` eg 
                on bgm.guid = eg.guid
    left join `{project_name}.{dataset_name_dssprod}.click_fact` cf
    on eg.guid_key = cf.guid_key and rgk.offer_id = cf.offer_key and rgk.date_key = cf.date_key
    left join `{project_name}.{dataset_name_dssprod}.page_group_dim` peg
    on cf.PAGE_GROUP_KEY = peg.page_group_key
    ),

    flag as(
    select *,max(add_to_cart) over(partition by bnid,date_key, offer_id) add_to_basket,max(purchase) over(partition by bnid,date_key, offer_id) item_purchase
    from (
    select pii_info.*,case when page_group_name in ('byor add to basket', 'byo3sr add to basket','preset engagement add to basket') then 1 else 0 end as add_to_cart, case when customer_submit_date_key is null then 0 else 1 end as purchase
    from pii_info
    left join `{project_name}.{dataset_name_dssprod}.product_order_fact` pof
    on pii_info.guid_key  = pof.guid_key and pii_info.offer_id = pof.OFFER_KEY and pof.CANCEL_DATE_KEY is null and pii_info.date_key=pof.CUSTOMER_SUBMIT_DATE_KEY
    and pof.RETURN_ACCEPTED_DATE_KEY is null and parse_date('%Y%m%d', cast( pof.CUSTOMER_SUBMIT_DATE_KEY as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 6 month) and parse_date('%Y%m%d', cast({delta_date} as string))))


    select distinct flag.*except(add_to_basket,item_purchase,add_to_cart,page_group_name,purchase,guid_key,offer_key),  first_value(add_to_basket) over(partition by flag.bnid,date_key, offer_id order by add_to_basket desc)  add_to_basket,first_value(item_purchase) over(partition by flag.bnid,date_key, offer_id order by item_purchase desc)item_purchase,gender,uea.marital_status ,uea.primary_currency_code  from flag
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