def recommendation_diamonds(params):
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name1 = params["dataset_name1"]
    dataset_name2 = params["dataset_name2"]
    dataset_name4 = params["dataset_name4"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = f"""with bnid_diamond_interaction as(
    select distinct  bnid, final.date_key,  final.sku1 as SKU_ID, view_cnt, pd.cut as  DIAMOND_CUT,pd.color as DIAMOND_COLOR,pd.clarity as DIAMOND_CLARITY,pd.carat as DIAMOND_CARAT_WEIGHT,pd.shape as DIAMOND_SHAPE 
    from `{project_name}.{dataset_name}.bn_daily_product_views` final
    JOIN (select distinct sku	,shape,	carat,	cut,	color,	clarity from `{project_name}.{dataset_name4}.diamond_attributes_dim` ) pd
    ON lower(final.sku1) = lower(cast(pd.SKU as string))
    where lower(final.sku1) like 'ld%'
    and parse_date('%Y%m%d', cast(date_key as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 2 month) and parse_date('%Y%m%d', cast({delta_date} as string)) ),

    pii_info as(
    select rgk.*,cf.guid_key,pgd.PAGE_GROUP_NAME ,sku1 from bnid_diamond_interaction  rgk
    left join `{project_name}.{dataset_name}.bn_guid_all`   bgm 
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
                where  pof.CANCEL_DATE_KEY is null and pof.RETURN_ACCEPTED_DATE_KEY is null and parse_date('%Y%m%d', cast(CUSTOMER_SUBMIT_DATE_KEY as string)) between date_sub(parse_date('%Y%m%d', cast({delta_date} as string)),interval 2 month) and parse_date('%Y%m%d', cast({delta_date} as string))) pof
    on pii_info.guid_key  = pof.guid_key and lower(pii_info.sku1) = lower(pof.sku) and parse_date('%Y%m%d', cast(date_key as string)) = parse_date('%Y%m%d', CUSTOMER_SUBMIT_DATE_KEY)  ))

		  select distinct flag.*except(add_to_basket,item_purchase,add_to_cart,purchase,PAGE_GROUP_NAME,guid_key,sku1),  first_value(add_to_basket) over(partition by flag.bnid,date_key, SKU_ID order by add_to_basket desc)  add_to_basket,first_value(item_purchase) over(partition by flag.bnid,date_key, SKU_ID order by item_purchase desc)item_purchase,gender,uea.marital_status ,uea.primary_currency_code  from flag
    left join `{project_name}.{dataset_name2}.bn_customer_profile` uca
    on flag.bnid = uca.bnid
    left join `{project_name}.{dataset_name2}.bn_explicit_attributes` uea
    on flag.bnid = uea.bnid""".format(
        project_name,
        dataset_name,
        dataset_name1,
        dataset_name2,
        dataset_name4,
        dataset_name_dssprod,
        delta_date,
    )
    return bql