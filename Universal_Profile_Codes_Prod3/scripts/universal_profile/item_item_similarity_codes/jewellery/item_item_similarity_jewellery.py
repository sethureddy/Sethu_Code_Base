def jewellery_item_interaction(params):
    """
    Creating the data for interactions and product similarities
    by using latest month data
    """
    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name1 = params["dataset_name1"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = """
    SELECT DISTINCT pd.*
FROM (
    SELECT bnid
        ,dpv.date_key
        ,dpv.offer_id
        ,view_cnt
    FROM (
        SELECT offer_id
            ,bnid
            ,date_key
            ,sum(view_cnt) AS view_cnt
        FROM `{project_name}.{dataset_name}.bn_daily_product_views`
        GROUP BY offer_id
            ,bnid
            ,date_key
        ) dpv
    JOIN (
        SELECT *
       
        EXCEPT
       
        (mode)
        FROM (
            SELECT product_offer_key
                ,product_class_name
                ,merch_category_rollup
                ,PRODUCT_SUB_CLASS_NAME
                ,PRODUCT_CATEGORY_TYPE
                ,Merch_product_category
                ,row_number() OVER (
                    PARTITION BY product_offer_key ORDER BY count(*) DESC
                    ) mode
            FROM `{project_name}.{dataset_name_dssprod}.product_dim`
            GROUP BY product_offer_key
                ,product_class_name
                ,merch_category_rollup
                ,PRODUCT_SUB_CLASS_NAME
                ,PRODUCT_CATEGORY_TYPE
                ,Merch_product_category
            )
        WHERE mode = 1
        ) prod
        ON dpv.offer_id = prod.product_offer_key
            AND prod.PRODUCT_SUB_CLASS_NAME NOT IN ('Diamonds')
            AND prod.PRODUCT_CATEGORY_TYPE NOT IN ('BADSKU')
            AND prod.PRODUCT_CATEGORY_TYPE IS NOT NULL
    WHERE MERCH_CATEGORY_ROLLUP IN ('nan', 'Other Jewelry', 'Bands', 'Diamond Jewelry')
        AND Product_sub_class_name NOT IN ('nan', 'BYO 3 Stone', 'BYO 5 Stone Ring', 'BYO Ring', 'BYO STONE')
        AND product_class_name NOT IN ('BYO STONE')
    ) AS pd
WHERE parse_date('%Y%m%d', cast(date_key AS string)) BETWEEN date_sub(parse_date('%Y%m%d', cast({delta_date} AS string)), interval 1 month)
        AND parse_date('%Y%m%d', cast({delta_date} AS string))""".format(
        project_name=project_name,
        dataset_name=dataset_name,
        dataset_name1=dataset_name1,
        dataset_name_dssprod=dataset_name_dssprod,
        delta_date=delta_date,
    )

    return bql
