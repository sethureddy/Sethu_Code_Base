def setting_item_interaction(params):

    """
    Creating the data for interactions and product similarities
    by using latest month data 

    """

    project_name = params["project_name"]
    dataset_name = params["dataset_name"]
    dataset_name1 = params["dataset_name1"]
    dataset_name_dssprod = params["dataset_name_dssprod"]
    delta_date = params["delta_date"]

    bql = """SELECT DISTINCT offer_id
        ,bnid
        ,view_cnt
        ,date_key
    FROM (
        SELECT bnid
            ,dpv.date_key
            ,dpv.offer_id
            ,view_cnt
            ,pd.*
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
                SELECT PRODUCT_OFFER_KEY
                    ,PRODUCT_SUB_CLASS_NAME
                    ,PRODUCT_CATEGORY_TYPE
                    ,MERCH_CATEGORY_ROLLUP
                    ,row_number() OVER (
                        PARTITION BY product_offer_key ORDER BY count(*) DESC
                        ) mode
                FROM `{project_name}.{dataset_name_dssprod}.product_dim`
                GROUP BY PRODUCT_OFFER_KEY
                    ,MERCH_CATEGORY_ROLLUP
                    ,PRODUCT_CATEGORY_TYPE
                    ,PRODUCT_SUB_CLASS_NAME
                )
            WHERE mode = 1
            ) pd
            ON dpv.offer_id = pd.product_offer_key
                AND pd.PRODUCT_SUB_CLASS_NAME NOT IN ('Diamonds')
                AND pd.PRODUCT_CATEGORY_TYPE NOT IN ('BADSKU')
                AND pd.PRODUCT_CATEGORY_TYPE IS NOT NULL
        WHERE pd.MERCH_CATEGORY_ROLLUP IN ('Engagement')
        )
    WHERE parse_date('%Y%m%d', cast(date_key AS string)) BETWEEN date_sub(parse_date('%Y%m%d', cast({delta_date} AS string)), interval 1 month)
            AND parse_date('%Y%m%d', cast({delta_date} AS string))""".format(
        project_name=project_name,
        dataset_name=dataset_name,
        dataset_name1=dataset_name1,
        dataset_name_dssprod=dataset_name_dssprod,
        delta_date=delta_date,
    )

    return bql
