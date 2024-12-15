def bn_recommendations_json(params):
    project_name = params["project_name"]
    _query = query(project_name)
    main_query = _query + " SELECT * FROM BN_Recommendations_JSON"
    return main_query


def query(project_name):
    bql = f'''
    WITH OfferSummary AS
    (
      SELECT 
      CAST (A.snapshot_date AS DATE) as snapshot_day, CAST(B.offer_id AS STRING) AS offer_id, COALESCE(A.site, C.site) as site,
      COALESCE(A.currency_code, C.currency_code) as currency_code,
      A.website_list_price as today_list_price, A.website_price AS today_price,
      C.website_list_price as yesterday_list_price, C.website_price AS yesterday_price,
      CASE WHEN C.website_price > 0 AND A.website_price > 0 THEN
          CASE WHEN C.website_price > A.website_price THEN 1 
               WHEN C.website_price < A.website_price THEN -1
               ELSE 0 END
       ELSE 0 END As price_drop_flag,
      CASE WHEN B.MERCH_CATEGORY_ROLLUP = 'Engagement' THEN 'Eng'
           WHEN B.MERCH_CATEGORY_ROLLUP = 'Bands' OR STRPOS(MARKETING_CATEGORY,'Wedding Rings') > 0 THEN 'WB'
           WHEN B.MERCH_CATEGORY_ROLLUP in ('Other Jewelry') THEN 'OJ'
           WHEN B.MERCH_CATEGORY_ROLLUP in ('Diamond Jewelry') THEN 'DJ'
           ELSE 'Other' END AS item_category,
      CASE WHEN STRPOS(B.MERCH_PRODUCT_CATEGORY,'BYO') > 0 AND B.MERCH_CATEGORY_ROLLUP IN ('Engagement','Other Jewelry','Diamond Jewelry') THEN 1 ELSE 0 END AS byo,
      B.name as display_name,
      COALESCE(SUBSTR(B.IMAGE_URL,0, STRPOS(IMAGE_URL,'wid=')-1),
                CONCAT(B.alternate_image_url , CASE WHEN (STRPOS(B.alternate_image_url,'?') > 0) THEN '&' ELSE '?' END )) AS image_url,
      B.alternate_image_url,
      CONCAT('/_',CAST(B.OFFER_ID as string)) AS cta_url,
      FROM {project_name}.o_product.offer_attributes_dim B 
      LEFT JOIN
          (SELECT snapshot_date, offer_id,website_list_price, website_price , site, currency_code FROM {project_name}.o_product.daily_offer_fact 
            WHERE DATETIME_DIFF(CURRENT_DATETIME("America/Los_Angeles"),snapshot_date, day) = 0 --0 for today
            AND site = 'BN' AND website_price > 0 AND any_product_sellable = 'Yes' AND currency_code = 'USD' 
          ) A ON A.offer_id = B.offer_id
      LEFT JOIN
          (SELECT snapshot_date, offer_id, website_list_price, website_price , site, currency_code FROM {project_name}.o_product.daily_offer_fact 
            WHERE DATETIME_DIFF(CURRENT_DATETIME("America/Los_Angeles"),snapshot_date, day) = 1 --0 for yesterday
            AND site = 'BN' AND website_price > 0 AND any_product_sellable = 'Yes' AND currency_code = 'USD' 
          ) C ON B.offer_id = C.offer_id
    WHERE image_url is not null
    AND (B.name not like '%Sizer%' or B.name not like '%Cleaner%' OR B.name not like '%Cleaning%')

    UNION DISTINCT 

      SELECT 
      B.FULL_DATE as snapshot_day, A.SKU as offer_id, 'BN' as site, 'USD' AS currency_code,
      B.USD_PRICE as today_list_price, B.USD_PRICE as today_price, C.USD_PRICE as yesterday_list_price, C.USD_PRICE as yesterday_price, 
      CASE WHEN C.USD_PRICE > 0 AND B.USD_PRICE > 0 THEN
        CASE WHEN C.USD_PRICE > B.USD_PRICE THEN 1 
             WHEN C.USD_PRICE < B.USD_PRICE THEN -1
             ELSE 0 END
        ELSE 0 END AS price_drop_flag,
      'LD' as item_category,
      0 as byo,
    CONCAT (IFNULL(CARAT,0), " Carat, ", 
              CASE WHEN IFNULL(A.SHAPE,'') = 'CU'THEN 'Cushion'
                   WHEN IFNULL(A.SHAPE,'') = 'AS'THEN 'Asscher'
                   WHEN IFNULL(A.SHAPE,'') = 'RA'THEN 'Radiant'
                   WHEN IFNULL(A.SHAPE,'') = 'EC'THEN 'Emerald'
                   WHEN IFNULL(A.SHAPE,'') = 'PR'THEN 'Princess'
                   WHEN IFNULL(A.SHAPE,'') = 'OV'THEN 'Oval'
                   WHEN IFNULL(A.SHAPE,'') = 'PS'THEN 'Pear'
                   WHEN IFNULL(A.SHAPE,'') = 'HS'THEN 'Heart'
                   WHEN IFNULL(A.SHAPE,'') = 'MQ'THEN 'Marquise'
                   WHEN IFNULL(A.SHAPE,'') = 'RD'THEN 'Round'
                   ELSE '' END,
      " Shape (Cut: ", IFNULL(A.CUT,''), ", Clarity: ", IFNULL(A.CLARITY,''), ", Color: ", IFNULL(A.Color,''), " )" ) as display_name,
        A.image_url,
      CASE WHEN STRPOS(ds.image_link,'wid=') > 0 THEN NULL ELSE ds.image_link END as alternate_image_url,
      CONCAT('/diamond-details/',CAST(A.SKU as string)) AS cta_url,
      FROM {project_name}.o_diamond.diamond_attributes_dim A
      LEFT JOIN (select id, max(image_link) as image_link FROM {project_name}.product_feeds.src_us_bn_diamonds group by 1) ds ON ds.id = A.sku
      LEFT JOIN 
      (
        SELECT CAST(dB.FULL_DATE AS DATE) AS FULL_DATE, sku, USD_PRICE
        FROM {project_name}.dssprod_o_diamond.diamond_daily_data B
        JOIN {project_name}.dssprod_o_warehouse.date_dim dB on dB.DATE_KEY = B.CAPTURE_DATE_KEY 
        AND DATETIME_DIFF(CURRENT_DATETIME("America/Los_Angeles") , dB.FULL_DATE, day ) = 1-- 1 day behind so 1 for today and 2 for yesterday
      ) B ON A.sku = B.sku
      LEFT JOIN (
        SELECT CAST(dB.FULL_DATE AS DATE) AS FULL_DATE , sku, USD_PRICE
        FROM {project_name}.dssprod_o_diamond.diamond_daily_data B
        JOIN {project_name}.dssprod_o_warehouse.date_dim dB on dB.DATE_KEY = B.CAPTURE_DATE_KEY 
        AND DATETIME_DIFF(CURRENT_DATETIME("America/Los_Angeles") , dB.FULL_DATE, day ) = 2 -- 1 day behind so 1 for today and 2 for yesterday
      ) C ON A.sku = C.sku
      WHERE A.image_url is not null
    )

    ----------------------------------------------------------Recommendations---------------------------------------------------------- 
    ,max_date_recomm AS
    (
       SELECT A.BNID, MERCH_PRODUCT_CATEGORY, MAX(RUN_DATE) as RUN_DATE 
       FROM {project_name}.o_customer.bn_product_recommendation A
       GROUP BY BNID, MERCH_PRODUCT_CATEGORY
    )


    ,TempRecommendations AS
    (
      SELECT
      A.BNID, 
      CASE WHEN substr(UPPER(COALESCE(SETTING_OFFER_ID, CAST(ITEM_OFFER_ID AS STRING))),0,2) = 'LD' THEN 'Loose Diamonds' ELSE CAST (PRODUCT_CLASS_NAME AS STRING) END AS PRODUCT_CLASS_NAME,
      CASE WHEN substr(UPPER(COALESCE(SETTING_OFFER_ID, CAST(ITEM_OFFER_ID AS STRING))),0,2) = 'LD' THEN 'Loose Diamonds' ELSE CAST (A.MERCH_PRODUCT_CATEGORY AS STRING) END AS MERCH_PRODUCT_CATEGORY,
      RECO_RANK,
      COALESCE(SETTING_OFFER_ID, CAST(ITEM_OFFER_ID AS STRING)) as offer_id, 
      A.RUN_DATE AS RUN_DATE, 
      FROM {project_name}.o_customer.bn_product_recommendation A
      JOIN max_date_recomm B ON A.BNID = B.BNID AND A.RUN_DATE = B.RUN_DATE AND A.MERCH_PRODUCT_CATEGORY = B.MERCH_PRODUCT_CATEGORY
      WHERE COALESCE(SETTING_OFFER_ID, CAST(ITEM_OFFER_ID AS STRING)) IS NOT NULL

      UNION DISTINCT 

      SELECT
      A.BNID, 
      CASE WHEN SUBSTR(UPPER(ITEM_SKU),0,2) = 'LD' THEN 'Loose Diamonds' ELSE CAST (PRODUCT_CLASS_NAME AS STRING) END AS PRODUCT_CLASS_NAME,
      CASE WHEN SUBSTR(UPPER(ITEM_SKU),0,2) = 'LD' THEN 'Loose Diamonds' ELSE CAST (A.MERCH_PRODUCT_CATEGORY AS STRING) END AS MERCH_PRODUCT_CATEGORY,
      RECO_RANK,
      CASE WHEN SUBSTR(UPPER(ITEM_SKU),0,2) = 'LD' THEN UPPER(ITEM_SKU) ELSE NULL END as offer_id,
      A.RUN_DATE AS RUN_DATE, 
      FROM {project_name}.o_customer.bn_product_recommendation A
      JOIN max_date_recomm B ON A.BNID = B.BNID AND A.RUN_DATE = B.RUN_DATE AND A.MERCH_PRODUCT_CATEGORY = B.MERCH_PRODUCT_CATEGORY
      WHERE SUBSTR(UPPER(ITEM_SKU),0,2) = 'LD' 

      UNION DISTINCT 

      SELECT
      A.BNID, 
      CASE WHEN SUBSTR(UPPER(DIAMOND_SKU_1),0,2) = 'LD' THEN 'Loose Diamonds' ELSE CAST (PRODUCT_CLASS_NAME AS STRING) END AS PRODUCT_CLASS_NAME,
      CASE WHEN SUBSTR(UPPER(DIAMOND_SKU_1),0,2) = 'LD' THEN 'Loose Diamonds' ELSE CAST (A.MERCH_PRODUCT_CATEGORY AS STRING) END AS MERCH_PRODUCT_CATEGORY,
      RECO_RANK,
      CASE WHEN SUBSTR(UPPER(DIAMOND_SKU_1),0,2) = 'LD' THEN UPPER(DIAMOND_SKU_1) ELSE NULL END as offer_id,
      A.RUN_DATE AS RUN_DATE, 
      FROM {project_name}.o_customer.bn_product_recommendation A
      JOIN max_date_recomm B ON A.BNID = B.BNID AND A.RUN_DATE = B.RUN_DATE AND A.MERCH_PRODUCT_CATEGORY = B.MERCH_PRODUCT_CATEGORY
      WHERE SUBSTR(UPPER(DIAMOND_SKU_1),0,2) = 'LD' 
    )

    , recommendations AS
    (
      SELECT BNID, PRODUCT_CLASS_NAME, MERCH_PRODUCT_CATEGORY,offer_id, MIN(RECO_RANK) AS RECO_RANK, MAX(RUN_DATE) AS RUN_DATE, COUNT(*) as dups
      FROM TempRecommendations
      GROUP BY BNID, PRODUCT_CLASS_NAME, MERCH_PRODUCT_CATEGORY,offer_id
    )
    ,
    TodayRecommendations AS
    (
        SELECT 
        A.BNID, 
        ROW_NUMBER() OVER (PARTITION BY A.BNID, PRODUCT_CLASS_NAME  ORDER BY A.RECO_RANK ASC ) product_class_seq,
        ROW_NUMBER() OVER (PARTITION BY A.BNID, MERCH_PRODUCT_CATEGORY  ORDER BY A.RECO_RANK ASC ) merch_category_seq,
        ROW_NUMBER() OVER (PARTITION BY A.BNID, item_category, byo ORDER BY A.RECO_RANK ASC ) item_category_seq,
        A.RECO_RANK, 
        B.ITEM_CATEGORY,
        B.BYO,
        PRODUCT_CLASS_NAME, 
        MERCH_PRODUCT_CATEGORY,
        A.offer_id, 
        A.RUN_DATE,
        DISPLAY_NAME,
        IMAGE_URL,
        alternate_image_url,
        CTA_URL,
        B.site, 
        B.currency_code, 
        B.today_list_price,
        B.yesterday_price,
        B.today_price,
        price_drop_flag
        FROM recommendations A
        JOIN OfferSummary B ON A.offer_id = B.offer_id AND today_price > 0
     )


    , contextRecommendationJSONData AS
    ( 
      SELECT  BNID, ITEM_CATEGORY, BYO, item_category_seq, PRODUCT_CLASS_NAME, product_class_seq, MERCH_PRODUCT_CATEGORY, merch_category_seq, original_price,today_price, price_drop_flag, TO_JSON_STRING(B) as json_data
      FROM ( 
            SELECT 
            BNID, product_class_seq, PRODUCT_CLASS_NAME, merch_category_seq, MERCH_PRODUCT_CATEGORY, item_category_seq,ITEM_CATEGORY, BYO, offer_id ,yesterday_price as original_price, today_price, price_drop_flag, currency_code, cta_url, display_name, IMAGE_URL AS base_image_link, alternate_image_url as alternate_image_link
            FROM TodayRecommendations
            where product_class_seq < 5  OR merch_category_seq < 5 OR item_category_seq < 5
            ) B
    )

    , Final_BN_Recos_Temp_JSON AS
    (
      SELECT bnid,
      product_class_name,
      merch_product_category,
      CONCAT (item_category, CASE WHEN byo = 1 THEN '-byo' ELSE '' END)  as item_category,

       -- PRODUCT_CLASS_NAME
       MAX(IF(product_class_seq  = 1, json_data, NULL)) AS reco_pcn_1,
       MAX(IF(product_class_seq  = 1, today_price, NULL)) AS price_reco_pcn_1,
       MAX(IF(product_class_seq  = 2, json_data, NULL)) AS reco_pcn_2,
       MAX(IF(product_class_seq  = 2, today_price, NULL)) AS price_reco_pcn_2,
       MAX(IF(product_class_seq  = 3, json_data, NULL)) AS reco_pcn_3,
       MAX(IF(product_class_seq  = 3, today_price, NULL)) AS price_reco_pcn_3,
       MAX(IF(product_class_seq  = 4, json_data, NULL)) AS reco_pcn_4,
       MAX(IF(product_class_seq  = 4, today_price, NULL)) AS price_reco_pcn_4,

          -- MERCH_PRODUCT_CATEGORY
       MAX(IF(merch_category_seq  = 1, json_data, NULL)) AS reco_mpc_1,
       MAX(IF(merch_category_seq  = 1, today_price, NULL)) AS price_reco_mpc_1,
       MAX(IF(merch_category_seq  = 2, json_data, NULL)) AS reco_mpc_2,
       MAX(IF(merch_category_seq  = 2, today_price, NULL)) AS price_reco_mpc_2,
       MAX(IF(merch_category_seq  = 3, json_data, NULL)) AS reco_mpc_3,
       MAX(IF(merch_category_seq  = 3, today_price, NULL)) AS price_reco_mpc_3,
       MAX(IF(merch_category_seq  = 4, json_data, NULL)) AS reco_mpc_4,
       MAX(IF(merch_category_seq  = 4, today_price, NULL)) AS price_reco_mpc_4,

        --item_category
       MAX(IF(item_category_seq  = 1, json_data, NULL)) AS reco_ic_1,
       MAX(IF(item_category_seq  = 1, today_price, NULL)) AS price_reco_ic_1,
       MAX(IF(item_category_seq  = 2, json_data, NULL)) AS reco_ic_2,
       MAX(IF(item_category_seq  = 2, today_price, NULL)) AS price_reco_ic_2,
       MAX(IF(item_category_seq  = 3, json_data, NULL)) AS reco_ic_3,
       MAX(IF(item_category_seq  = 3, today_price, NULL)) AS price_reco_ic_3,
       MAX(IF(item_category_seq  = 4, json_data, NULL)) AS reco_ic_4,
       MAX(IF(item_category_seq  = 4, today_price, NULL)) AS price_reco_ic_4,

       FROM contextRecommendationJSONData
       GROUP BY bnid,
          product_class_name,
          merch_product_category,
          item_category
    )

    , BN_Recommendations_JSON AS
    ( SELECT current_date("America/Los_Angeles") as run_date, bnid, 'product_class_name' as type, product_class_name as specs, 
            max(price_reco_pcn_1) as price_reco_1, max(reco_pcn_1) as reco_1,
            max(price_reco_pcn_2) as price_reco_2, max(reco_pcn_2) as reco_2,
            max(price_reco_pcn_3) as price_reco_3, max(reco_pcn_3) as reco_3,
            max(price_reco_pcn_4) as price_reco_4, max(reco_pcn_4) as reco_4
      FROM Final_BN_Recos_Temp_JSON
      GROUP BY bnid, type, specs

      UNION DISTINCT 

      SELECT current_date("America/Los_Angeles") as run_date, bnid, 'merch_product_category' as type, merch_product_category as specs, 
            max(price_reco_mpc_1) as price_reco_1, max(reco_mpc_1) as reco_1,
            max(price_reco_mpc_2) as price_reco_2, max(reco_mpc_2) as reco_2,
            max(price_reco_mpc_3) as price_reco_3, max(reco_mpc_3) as reco_3,
            max(price_reco_mpc_4) as price_reco_4, max(reco_mpc_4) as reco_4
      FROM Final_BN_Recos_Temp_JSON
      GROUP BY bnid, type, specs

      UNION DISTINCT 

      SELECT current_date("America/Los_Angeles") as run_date,bnid, 'item_category' as type, item_category as specs, 
            max(price_reco_ic_1) as price_reco_1, max(reco_ic_1) as reco_1,
            max(price_reco_ic_2) as price_reco_2, max(reco_ic_2) as reco_2,
            max(price_reco_ic_3) as price_reco_3, max(reco_ic_3) as reco_3,
            max(price_reco_ic_4) as price_reco_4, max(reco_ic_4) as reco_4
      FROM Final_BN_Recos_Temp_JSON
      GROUP BY bnid, type, specs

    )
    '''.format(project_name)
    return bql