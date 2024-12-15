def product_propensity(params):
    project_name = params["project_name"]
    top_categories = 3
    _query = query(project_name, top_categories)
    main_query = _query
    return main_query


def query(project_name, top_categories):
    bql = f'''        
        WITH category_views AS (
            SELECT DISTINCT pv.bnid, offer_id, pd.MERCH_PRODUCT_CATEGORY, sum(view_cnt) OVER (PARTITION BY pv.bnid, pd.MERCH_PRODUCT_CATEGORY) AS category_cnt
            FROM 
            (select * from `{project_name}.up_stg_customer.bn_daily_product_views` 
            union all
            select * from `{project_name}.up_stg_customer.bn_daily_product_views_history_2017_2019`)pv
            JOIN (
                SELECT *

                EXCEPT

                (mode)
                FROM (
                    SELECT product_offer_key, Merch_product_category, row_number() OVER (
                            PARTITION BY product_offer_key ORDER BY count(*) DESC
                            ) mode
                    FROM `{project_name}.dssprod_o_warehouse.product_dim`
                    WHERE MERCH_PRODUCT_CATEGORY IS NOT NULL
                    GROUP BY product_offer_key, Merch_product_category
                    )
                WHERE mode = 1
                ) pd
                ON pv.offer_id = pd.PRODUCT_OFFER_KEY
            WHERE offer_id IS NOT NULL
            ), basket_category AS (
            SELECT DISTINCT bnid, Merch_product_category, category_cnt, sum(basket_flag) OVER (PARTITION BY bnid, MERCH_PRODUCT_CATEGORY, category_cnt) AS number_of_baskets, sum(review_flag) OVER (PARTITION BY bnid, MERCH_PRODUCT_CATEGORY, category_cnt) AS number_of_reviews, CASE 
                    WHEN pof.guid_key IS NOT NULL
                        AND pof.offer_key IS NOT NULL
                        THEN 1
                    ELSE 0
                    END AS order_flag
            FROM (
                SELECT DISTINCT cv.*, cf.guid_key, CASE 
                        WHEN lower(page_group_name) LIKE '%basket%'
                            THEN 1
                        ELSE 0
                        END AS basket_flag, CASE 
                        WHEN lower(activity_set_code) LIKE '%review%'
                            THEN 1
                        ELSE 0
                        END AS review_flag
                FROM category_views cv
                JOIN (
                    SELECT DISTINCT bnid, customer_guid
                    FROM `{project_name}.up_stg_customer.bnid_guid_mapping`
                    ) bgm
                    ON cv.bnid = bgm.bnid
                JOIN `{project_name}.up_tgt_ora_tables.guid_dim_cf` eg
                    ON bgm.customer_guid = eg.guid
                JOIN (select distinct guid_key,offer_key,PAGE_GROUP_KEY from `{project_name}.dssprod_o_warehouse.click_fact`) cf
                    ON eg.GUID_KEY = cf.GUID_KEY
                        AND cv.offer_id = cast(cf.OFFER_key AS int64)
                JOIN `{project_name}.dssprod_o_warehouse.page_group_dim` pgd
                    ON cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
                ) br
            LEFT JOIN (
                SELECT DISTINCT guid_key, offer_key
                FROM `{project_name}.dssprod_o_warehouse.product_order_fact`
                WHERE RETURN_ACCEPTED_DATE_KEY IS NULL
                    AND cancel_date_key IS NULL
                ) pof
                ON br.guid_key = pof.guid_key
                    AND br.offer_id = pof.offer_key
            ) SELECT * FROM (
        SELECT DISTINCT bnid, merch_product_category, rank() OVER (
                PARTITION BY bnid ORDER BY number_of_orders DESC, number_of_baskets DESC, number_of_reviews DESC, view_rank
                ) AS category_rank
        FROM (
            SELECT DISTINCT *, row_number() OVER (
                    PARTITION BY bnid ORDER BY category_cnt DESC
                    ) AS view_rank
            FROM (
                SELECT DISTINCT *

                EXCEPT

                (order_flag), sum(order_flag) OVER (PARTITION BY bnid, MERCH_PRODUCT_CATEGORY, category_cnt) AS number_of_orders
                FROM basket_category
                )
            )
        ) WHERE category_rank <= {top_categories}
    '''.format(project_name, top_categories)
    return bql