def recommendation_jewellery(params):
    """
	Returns Jewellery Browsed information for last 3 months
	arguments:
		params(Dict) : Dictionary with input information
	returns:
		bql(DataFrame) : Input DataFrame
    """
    project_name = params["project_name"]
    stg_data = params["stg_data"]
    customer_data = params["customer_data"]
    product_data = params["product_data"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""with bnid_jewellery_interaction as(
    Select a.*, dp.PRODUCT_LIST_PRICE
    from
    (Select  distinct pd.* from (
    select  bnid, B.date_key,  B.offer_id, view_cnt, A.*
    from (select offer_id, bnid, date_key, sum(view_cnt) as view_cnt
    from `{project_name}.{stg_data}.bn_daily_product_views` 
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
    (select distinct offer_id from `{project_name}.{product_data}.daily_offer_fact` where  any_product_sellable='Yes' and parse_date('%Y%m%d',cast(date_key as string)) = date_sub(parse_date('%Y%m%d',cast({delta_date} as string)),interval 1 day)) as dof
    on pd.OFFER_ID=dof.offer_id
    where  parse_date('%Y%m%d', cast( date_key as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 3 month) and parse_date('%Y%m%d', cast({delta_date} as string))) a
    inner join (Select avg(PRODUCT_LIST_PRICE) as PRODUCT_LIST_PRICE , offer_key from `{project_name}.{dataset_name_dssprod}.daily_product_fact`
    group by offer_key) dp
    on a.OFFER_id= dp.offer_key),
    pii_info as(
    select rgk.*,cf.guid_key,peg.PAGE_GROUP_NAME ,peg.activity_set_code,primary_browse_segment, offer_key from bnid_jewellery_interaction  rgk
    left join `{project_name}.{stg_data}.bn_guid_all`    bgm 
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
                where  pof.CANCEL_DATE_KEY is null and pof.RETURN_ACCEPTED_DATE_KEY is null and parse_date('%Y%m%d', cast( pof.CUSTOMER_SUBMIT_DATE_KEY as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 3 month) and parse_date('%Y%m%d', cast({delta_date} as string))) pof
    on pii_info.guid_key  = pof.guid_key and pii_info.offer_id = pof.offer_key))
    select distinct flag.*except(add_to_basket,item_purchase,add_to_cart,purchase,PAGE_GROUP_NAME,activity_set_code,guid_key,primary_browse_segment,offer_key),  first_value(add_to_basket) over(partition by flag.bnid,date_key, offer_id order by add_to_basket desc)  add_to_basket,first_value(item_purchase) over(partition by flag.bnid,date_key, offer_id order by item_purchase desc)item_purchase,derived_gender,uea.marital_status ,uea.primary_currency_code  from flag
    left join `{project_name}.{customer_data}.bn_customer_profile` uca
    on flag.bnid = uca.bnid
    left join  `{project_name}.{customer_data}.bn_explicit_attributes` uea
    on flag.bnid = uea.bnid""".format(
        project_name,
        stg_data,
        customer_data,
        product_data,
        dataset_name_dssprod,
        delta_date,
    )
    return bql


def recommendation_diamonds(params):
    """
	Returns Diamonds Browsed information for last 3 months
	arguments:
		params(Dict) : Dictionary with input information
	returns:
		bql(DataFrame) : Input DataFrame
    """
    project_name = params["project_name"]
    stg_data = params["stg_data"]
    customer_data = params["customer_data"]
    diamond_data = params["diamond_data"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""with bnid_diamond_interaction as(
    select distinct  bnid, final.date_key,  final.sku1 as SKU_ID, view_cnt, pd.cut as  DIAMOND_CUT,pd.color as DIAMOND_COLOR,pd.clarity as DIAMOND_CLARITY,pd.carat as DIAMOND_CARAT_WEIGHT,pd.shape as DIAMOND_SHAPE 
    from `{project_name}.{stg_data}.bn_daily_product_views` final
    JOIN (select distinct sku	,shape,	carat,	cut,	color,	clarity from `{project_name}.{diamond_data}.diamond_attributes_dim` ) pd
    ON lower(final.sku1) = lower(cast(pd.SKU as string))
    where lower(final.sku1) like 'ld%'
    and parse_date('%Y%m%d', cast(date_key as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 3 month) and parse_date('%Y%m%d', cast({delta_date} as string)) ),

    pii_info as(
    select rgk.*,cf.guid_key,pgd.PAGE_GROUP_NAME ,sku1 from bnid_diamond_interaction  rgk
    left join `{project_name}.{stg_data}.bn_guid_all`   bgm 
    on lower(rgk.bnid) = lower(bgm.bnid)
    left join  `{project_name}.{dataset_name_dssprod}.email_guid` gd 
    on bgm.guid = gd.guid
    left join `{project_name}.{dataset_name_dssprod}.click_fact` cf
    on gd.guid_key = cf.guid_key and rgk.SKU_ID = cf.SKU1 and rgk.date_key = cf.date_key
    left join `{project_name}.{dataset_name_dssprod}.page_group_dim` pgd
    on cf.PAGE_GROUP_KEY = pgd.page_group_key
    ),

    flag as(
    select *,max(add_to_cart) over(partition by bnid,date_key, SKU_ID) add_to_basket,max(purchase) over(partition by bnid,date_key, SKU_ID) item_purchase
    from (
    select pii_info.*,case when page_group_name in ('loose diamond add to basket') then 1 else 0 end as add_to_cart, case when customer_submit_date_key is null then 0 else 1 end as purchase
    from pii_info
    left join (select  cast(customer_submit_date_key as string) customer_submit_date_key,sku,guid_key from `{project_name}.{dataset_name_dssprod}.product_order_fact` pof
                join  `{project_name}.{dataset_name_dssprod}.product_dim` pd
                on pof.product_key = pd.product_key and lower(product_class_name) = 'loose diamonds'
                where  pof.CANCEL_DATE_KEY is null and pof.RETURN_ACCEPTED_DATE_KEY is null and parse_date('%Y%m%d', cast(CUSTOMER_SUBMIT_DATE_KEY as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 3 month) and parse_date('%Y%m%d', cast({delta_date} as string))) pof
    on pii_info.guid_key  = pof.guid_key and lower(pii_info.sku1) = lower(pof.sku) and parse_date('%Y%m%d', cast(date_key as string)) = parse_date('%Y%m%d', CUSTOMER_SUBMIT_DATE_KEY)  ))

		  select distinct flag.*except(add_to_basket,item_purchase,add_to_cart,purchase,PAGE_GROUP_NAME,guid_key,sku1),  first_value(add_to_basket) over(partition by flag.bnid,date_key, SKU_ID order by add_to_basket desc)  add_to_basket,first_value(item_purchase) over(partition by flag.bnid,date_key, SKU_ID order by item_purchase desc)item_purchase,gender,uea.marital_status ,uea.primary_currency_code  from flag
    left join `{project_name}.{customer_data}.bn_customer_profile` uca
    on flag.bnid = uca.bnid
    left join `{project_name}.{customer_data}.bn_explicit_attributes` uea
    on flag.bnid = uea.bnid""".format(
        project_name,
        stg_data,
        customer_data,
        diamond_data,
        dataset_name_dssprod,
        delta_date,
    )
    return bql


def recommendation_settings(params):
    """
	Returns Setting Browsed information for last 3 months
	arguments:
		params(Dict) : Dictionary with input information
	returns:
		bql(DataFrame) : Input DataFrame
    """

    project_name = params["project_name"]
    stg_data = params["stg_data"]
    customer_data = params["customer_data"]
    product_data = params["product_data"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""with bnid_setting_interaction as(
    Select a.*, dpf.PRODUCT_LIST_PRICE
    from
    (Select  distinct pd.* from (
    select  bnid, B.date_key,  B.offer_id, view_cnt, A.*
    from (select offer_id, bnid, date_key, sum(view_cnt) as view_cnt
    from `{project_name}.{stg_data}.bn_daily_product_views`

    group by offer_id, bnid, date_key)B
    JOIN (select *except(mode) from (
    Select PRODUCT_OFFER_KEY, PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE
    ,PRODUCT_CLASS_NAME,PRODUCT_SUB_CLASS_NAME,PRODUCT_CATEGORY_TYPE,MERCH_PRODUCT_CATEGORY,MERCH_CATEGORY_ROLLUP,
    row_number() over(partition by product_offer_key  order by count(*) desc) mode
    from `{project_name}.{dataset_name_dssprod}.product_dim`

    group by PRODUCT_OFFER_KEY, PRIMARY_METAL_COLOR,PRIMARY_METAL_NAME,PRIMARY_SHAPE_NAME,PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,
    PRIMARY_SETTING_TYPE,PRIMARY_STONE_TYPE,product_class_name,MERCH_CATEGORY_ROLLUP,MERCH_PRODUCT_CATEGORY,PRODUCT_SUB_CLASS_NAME,PRODUCT_CLASS_NAME,PRODUCT_CATEGORY_TYPE )
    where mode = 1) A
    ON B.offer_id = A.product_offer_key
    and A.PRODUCT_SUB_CLASS_NAME not in ('Diamonds')
    and A.PRODUCT_CATEGORY_TYPE not in ('BADSKU')
    and A.PRODUCT_CATEGORY_TYPE is not null
    where A.MERCH_CATEGORY_ROLLUP in ('Engagement') and (not(PRIMARY_METAL_COLOR) = 'nan' or     not(PRIMARY_METAL_NAME) = 'nan' or    not(PRIMARY_SHAPE_NAME) = 'nan' or    not(PRIMARY_SETTING_TYPE) = 'nan' or not(PRIMARY_STONE_TYPE) = 'nan') ) as pd
    inner join  (select distinct offer_id from `{project_name}.{product_data}.daily_offer_fact` where  any_product_sellable='Yes' and parse_date('%Y%m%d',cast(date_key as string)) = date_sub(parse_date('%Y%m%d',cast({delta_date} as string)),interval 1 day)) as dof
	on pd.OFFER_ID=dof.offer_id
    and parse_date('%Y%m%d', cast(date_key as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 3 month) and parse_date('%Y%m%d', cast({delta_date} as string))) a
    join
    (select offer_key,avg(PRODUCT_LIST_PRICE) PRODUCT_LIST_PRICE from `{project_name}.{dataset_name_dssprod}.daily_product_fact` group by 1) dpf
    on a.offer_id = dpf.offer_key),

    pii_info as(
    select rgk.*,cf.guid_key,peg.PAGE_GROUP_NAME , offer_key from bnid_setting_interaction  rgk
    left join `{project_name}.{stg_data}.bn_guid_all`   bgm

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
    and pof.RETURN_ACCEPTED_DATE_KEY is null and parse_date('%Y%m%d', cast( pof.CUSTOMER_SUBMIT_DATE_KEY as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 3 month) and parse_date('%Y%m%d', cast({delta_date} as string))))


    select distinct flag.*except(add_to_basket,item_purchase,add_to_cart,page_group_name,purchase,guid_key,offer_key),  first_value(add_to_basket) over(partition by flag.bnid,date_key, offer_id order by add_to_basket desc)  add_to_basket,first_value(item_purchase) over(partition by flag.bnid,date_key, offer_id order by item_purchase desc)item_purchase,gender,uea.marital_status ,uea.primary_currency_code  from flag
    left join `{project_name}.{customer_data}.bn_customer_profile` uca
    on flag.bnid = uca.bnid
    left join  `{project_name}.{customer_data}.bn_explicit_attributes` uea
    on flag.bnid = uea.bnid""".format(
        project_name,
        stg_data,
        customer_data,
        product_data,
        dataset_name_dssprod,
        delta_date,
    )
    return bql


def lastpurchase_product(params):
    """
	Returns Last Purchase Informaiton for last 2 years
	arguments:
		params(Dict) : Dictionary with input information
	returns:
		bql(DataFrame) : Last Purchase DataFrame at BNID level
    """

    project_name = params["project_name"]
    stg_data = params["stg_data"]
    customer_data = params["customer_data"]
    product_data = params["product_data"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""WITH last_purchase AS
	(
	SELECT bnid,ph.email_address_key,order_date,merch_category_rollup,OFFER_ID,SETTING_OFFER_ID,SKU,usd_after_disc_sale_amount
	,ROW_NUMBER() OVER (PARTITION BY ph.email_address_key  ORDER BY order_date desc,usd_after_disc_sale_amount DESC) item_rank 
	FROM `{project_name}.{customer_data}.up_purchase_history` ph
	left JOIN `{project_name}.{dataset_name_dssprod}.email_guid` eg
	ON ph.EMAIL_ADDRESS_KEY = eg.EMAIL_ADDRESS_KEY
	left JOIN `{project_name}.{stg_data}.bn_guid_all` bga
	ON eg.guid = bga.guid
	where order_date between DATE_SUB(parse_date('%Y%m%d' ,cast({delta_date} as string)), INTERVAL 2 YEAR) and parse_date('%Y%m%d',cast({delta_date} as string))
	and merch_category_rollup is not null
	and FINAL_STATE_ORDER_TYPE is null
	and RETURN_ORDER_TYPE NOT IN ('RETURN FOR CREDIT')
	and return_date is null
	)
	select * from last_purchase where item_rank = 1""".format(
        project_name,
        stg_data,
        customer_data,
        product_data,
        dataset_name_dssprod,
        delta_date,
    )
    return bql
