def basketaudienceview(params):
    project_name = params["project_name"]
    _query = query(project_name)
    main_query = _query + " SELECT * FROM  abandoned_baskets_audience "
    return main_query


def lookup_BasketItemTable(params):
    project_name = params["project_name"]
    _query = query(project_name)
    main_query = _query + " select * FROM lookup_basket_item_table "
    return main_query


def query(project_name):
    bql = f'''
    WITH OfferSummary AS
    (
      SELECT DISTINCT 
      CAST(B.offer_id AS STRING) AS offer_id, 
      CASE WHEN B.MERCH_CATEGORY_ROLLUP = 'Engagement' THEN 'Eng'
           WHEN B.MERCH_CATEGORY_ROLLUP = 'Bands' OR STRPOS(MARKETING_CATEGORY,'Wedding Rings') > 0 THEN 'WB'
           WHEN B.MERCH_CATEGORY_ROLLUP in ('Other Jewelry') THEN 'OJ'
           WHEN B.MERCH_CATEGORY_ROLLUP in ('Diamond Jewelry') THEN 'DJ'
           ELSE 'Other' END AS item_category,
      merch_product_category,
      CASE WHEN STRPOS(B.MERCH_PRODUCT_CATEGORY,'BYO') > 0 AND B.MERCH_CATEGORY_ROLLUP IN ('Engagement','Other Jewelry','Diamond Jewelry') THEN 1 ELSE 0 END AS byo,  
      '' as DIAMOND_SHAPE,
      B.name as display_name,
      COALESCE(SUBSTR(B.IMAGE_URL,0, STRPOS(IMAGE_URL,'wid=')-1),
                CONCAT(B.alternate_image_url , CASE WHEN (STRPOS(B.alternate_image_url,'?') > 0) THEN '&' ELSE '?' END )) AS image_url,
      B.alternate_image_url,
      CONCAT('/_',CAST(B.OFFER_ID as string)) AS cta_url

      FROM {project_name}.o_product.offer_attributes_dim B 

    UNION DISTINCT 

      SELECT DISTINCT 
      A.SKU as offer_id, 
      'LD' as item_category,
      'Loose Diamonds' as merch_product_category,
      0 as byo,
      A.SHAPE as DIAMOND_SHAPE,
      CONCAT (IFNULL(A.CARAT,0), " Carat, ", IFNULL(A.SHAPE,''),  " Shape (Cut: ", IFNULL(A.CUT,''), ", Clarity: ", IFNULL(A.CLARITY,''), ", Color: ", IFNULL(A.Color,''), " )" ) as display_name,
      A.image_url,
      CASE WHEN STRPOS(ds.image_link,'wid=') > 0 THEN NULL ELSE ds.image_link END as alternate_image_url,
      CONCAT('/diamond-details/',CAST(A.SKU as string)) AS cta_url
      FROM {project_name}.o_diamond.diamond_attributes_dim A
      LEFT JOIN (select id, max(image_link) as image_link FROM {project_name}.product_feeds.src_us_bn_diamonds group by 1) ds ON ds.id = A.sku
      WHERE A.image_url is not null
    )

    , TodayOfferSummary_JSON AS
    ( 
      SELECT  offer_id, TO_JSON_STRING(B) as json_data
      FROM ( 
            SELECT 
             A.offer_id, website_price as price, currency_code, cta_url, display_name, IMAGE_URL AS base_image_link
            FROM OfferSummary A
            JOIN
                (SELECT snapshot_date, offer_id,website_list_price, website_price , site, currency_code FROM {project_name}.o_product.daily_offer_fact 
                  WHERE DATETIME_DIFF(CURRENT_DATETIME("America/Los_Angeles"),snapshot_date, day) = 0 --0 for today
                  AND site = 'BN' AND website_price > 0 AND any_product_sellable = 'Yes' AND currency_code = 'USD' 
                ) B ON A.offer_id = CAST(B.offer_id AS STRING)
            WHERE image_url is not null
            AND (display_name not like '%Sizer%' or display_name not like '%Cleaner%' OR display_name not like '%Cleaning%')
            ) B
    )


    ------------------------------------------GET OPEN BASKET IDS---------------------------------------------------------------
    , tempBaskets AS
    (
    SELECT bnid, BASKET_ID, CUSTOMER_LANGUAGE_CODE,SITE,CURRENCY_CODE, CUSTOMER_ID, BASKET_IP_ADDRESS,customer_guid,
    COALESCE(EMAIL_ADDRESS,CASE WHEN count_email_address < 2 THEN derived_email_address ELSE NULL END) as EMAIL_ADDRESS ,FIRST_NAME, LAST_NAME,PHONE_NUMBER,SOURCE_CODE,INPUT_METHOD,ORDER_TYPE,SHIP_TO_COUNTRY_CODE,
    BASKET_START_DATE,BASKET_STATUS_DATE,PRIMARY_CONTACT_METHOD, HOSTNAME, ITEM_TOTAL_PRICE
    FROM
    (
        SELECT COALESCE(ecp.bnid, X.bnid) as bnid, BASKET_ID, CUSTOMER_LANGUAGE_CODE,SITE,bskt.CURRENCY_CODE, bskt.CUSTOMER_ID, bskt.BASKET_IP_ADDRESS,bskt.customer_guid,
        COALESCE(bskt.EMAIL_ADDRESS) as EMAIL_ADDRESS ,FIRST_NAME,LAST_NAME,PHONE_NUMBER,SOURCE_CODE,INPUT_METHOD,ORDER_TYPE,SHIP_TO_COUNTRY_CODE,
        BASKET_START_DATE,BASKET_STATUS_DATE,COALESCE(PRIMARY_CONTACT_METHOD,'email') AS PRIMARY_CONTACT_METHOD,
        MAX(COALESCE(caed.EMAIL_ADDRESS, ecp.EMAIL_ADDRESS, beg.EMAIL_ADDRESS )) as derived_email_address,
        COUNT(DISTINCT COALESCE(caed.EMAIL_ADDRESS,ecp.EMAIL_ADDRESS,beg.EMAIL_ADDRESS,x.email_address)) as count_email_address,
        MAX(
        CONCAT('www.bluenile.com',
        case when (bskt.ship_to_country_code = 'US' and COALESCE(bskt.CUSTOMER_LANGUAGE_CODE , 'en-us') = 'en-us') then ''
                                         when (bskt.ship_to_country_code is null and bskt.hostname = 'www.bluenile.com') then ''
                                         when (bskt.ship_to_country_code is null and bskt.hostname = 'www.bluenile.ca') then '/ca'
                                         when (bskt.ship_to_country_code is null and bskt.hostname = 'www.bluenile.co.uk') then '/uk'
                                         when (bskt.ship_to_country_code is null ) then '/'||concat(substr(bskt.hostname,0,2),'')
                                         when (coalesce(bskt.CUSTOMER_LANGUAGE_CODE, 'en-us') = stc.LANGUAGE_CODE) then '/'||concat(c.display_code,'')
                                         else concat('/',concat(concat(concat(c.display_code,'/'),l.display_code),''))
        end)) as HOSTNAME,
        MAX(SUBTOTAL) AS ITEM_TOTAL_PRICE
        FROM {project_name}.nileprod_o_warehouse.basket bskt
        LEFT JOIN 
        (SELECT A.bnid, A.customer_guid ,C.email_address
          FROM {project_name}.up_stg_customer.bnid_guid_distinct A
          JOIN ( SELECT customer_guid , count( distinct bnid) as cnt FROM {project_name}.up_stg_customer.bnid_guid_distinct group by customer_guid having cnt = 1 ) B
          ON A.customer_guid = B.customer_guid
          JOIN {project_name}.o_customer.email_customer_profile C ON A.bnid = C.bnid) X ON x.customer_guid = bskt.customer_guid
        LEFT JOIN {project_name}.up_stg_customer.bn_email_guid beg ON beg.guid = bskt.customer_guid
        LEFT JOIN {project_name}.o_customer.email_customer_profile ecp ON beg.bnid = ecp.bnid
        LEFT JOIN {project_name}.dssprod_o_warehouse.customer_account_email_address caed ON caed.CUSTOMER_ACCOUNT_KEY = bskt.customer_id
        LEFT JOIN {project_name}.nileprod_o_warehouse.country c ON coalesce(bskt.ship_to_country_code, 'US') = c.country_code
        JOIN {project_name}.nileprod_o_warehouse.ship_to_country_language stc ON c.country_code = stc.SHIP_TO_COUNTRY
        LEFT JOIN {project_name}.product_feeds.language_code l ON coalesce(bskt.CUSTOMER_LANGUAGE_CODE, 'en-us') = l.language_code
        WHERE ( TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(bskt.BASKET_START_DATE as timestamp),HOUR) between 0 and 360
        OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(bskt.BASKET_STATUS_DATE as timestamp),HOUR) between 0 and 360)
        AND bskt.basket_status = "Open"
        AND ( (bskt.ORDER_TYPE IS NOT NULL AND bskt.ORDER_TYPE NOT IN ('EXCHANGE','REPLACEMENT','REPAIR','RESIZE')) OR bskt.ORDER_TYPE IS NULL)
        AND ( (bskt.ORDER_TYPE IS NOT NULL AND bskt.FRAUD_STATUS NOT IN ('Suspected Fraud')) OR bskt.FRAUD_STATUS IS NULL)
        AND CUSTOMER_SUBMIT_DATE IS NULL
        AND SUBTOTAL > 0
        AND bskt.CURRENCY_CODE IS NOT NULL
        GROUP BY bnid, BASKET_ID, CUSTOMER_LANGUAGE_CODE,SITE,CURRENCY_CODE,CUSTOMER_ID,BASKET_IP_ADDRESS,bskt.customer_guid,
        EMAIL_ADDRESS,FIRST_NAME,LAST_NAME,PHONE_NUMBER,SOURCE_CODE,INPUT_METHOD,ORDER_TYPE,bskt.SHIP_TO_COUNTRY_CODE,
        BASKET_START_DATE,BASKET_STATUS_DATE,COALESCE(PRIMARY_CONTACT_METHOD,'email')
    )
    WHERE COALESCE(EMAIL_ADDRESS,CASE WHEN count_email_address < 2 THEN derived_email_address ELSE NULL END) IS NOT NULL
    )


    ------------------------------------------GET ID for ITEMS (SKU AND OFFER_ID) FOR ALL OPEN BASKETS ---------------------------------------------
    , tempSKUOfferIDs AS
    (
    SELECT DISTINCT bitems.SKU ,bitems.OFFER_ID, COALESCE(CAST(bitems.OFFER_ID AS STRING) ,CAST (bitems.SKU AS STRING)) as pd_offer_id,item_category, merch_product_category, byo ,DIAMOND_SHAPE
    FROM {project_name}.nileprod_o_warehouse.basket_item bitems
    JOIN tempBaskets bskt ON bitems.basket_id = bskt.basket_id
    LEFT JOIN OfferSummary os ON os.OFFER_ID = COALESCE(CAST(bitems.OFFER_ID AS STRING) ,CAST (bitems.SKU AS STRING)) 
    WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(bitems.CREATE_DATE as timestamp),DAY) < 31
    OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(bitems.CHANGE_DATE as timestamp),DAY) < 31
    ) 

    ------------------------------------------GET ITEMS ID FOR OPEN BASKETS FOR BYO SKU---------------------------------------------
    , tempBYOSKU AS
    (
    SELECT DISTINCT bitems.SKU, bitems.BASKET_ITEM_ID, bitems.basket_id 
    FROM {project_name}.nileprod_o_warehouse.basket_item bitems
    JOIN tempBaskets bskt ON bitems.basket_id = bskt.basket_id
    WHERE SKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNITEM_ENG','BNPEND_MTD','BNRING_MTD')
    AND ( TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(bitems.CREATE_DATE as timestamp),DAY) < 31
    OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(bitems.CHANGE_DATE as timestamp),DAY) < 31)
    ) 

    ---------------------------PARTITION BASKET ITEM ON PARENT_ITEM_ID TO GET UNIQUE ITEMS IN BASKET ---------------------------------------------
    , tempBasketItems AS
    (
      SELECT 
      ROW_NUMBER() OVER (PARTITION BY bitems.basket_id ORDER BY bitems.CHANGE_DATE desc, bitems.CREATE_DATE asc) seq,
      ROW_NUMBER() OVER (PARTITION BY bitems.basket_id, bitems.PARENT_ITEM_ID ORDER BY bitems.CHANGE_DATE desc, bitems.CREATE_DATE DESC) item_seq,
      ROW_NUMBER() OVER (PARTITION BY bitems.basket_id, bitems.PARENT_ITEM_ID,SUBSTR(bitems.SKU,0,2) ORDER BY bitems.PARENT_ITEM_ID asc) diamond_seq,

      CASE WHEN bitems.PARENT_ITEM_ID is null OR bitems.PARENT_ITEM_ID = 0 THEN bitems.BASKET_ITEM_ID ELSE bitems.PARENT_ITEM_ID END as ID,
      BYO.SKU AS BYOSKU,
      item_category, merch_product_category, byo ,DIAMOND_SHAPE,
      bitems.*
      FROM {project_name}.nileprod_o_warehouse.basket_item bitems
      JOIN (SELECT distinct basket_id FROM tempBaskets) bskt ON bitems.basket_id = bskt.basket_id
      LEFT JOIN tempBYOSKU BYO ON BYO.BASKET_ITEM_ID = bitems.PARENT_ITEM_ID
      LEFT JOIN OfferSummary os ON os.OFFER_ID = COALESCE(CAST(bitems.OFFER_ID AS STRING) ,CAST (bitems.SKU AS STRING)) 

      WHERE bitems.SKU NOT IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNITEM_ENG','BNPEND_MTD','BNRING_MTD')
      AND (bitems.IS_VISIBLE_TO_CUSTOMER = 1 OR CAST(bitems.PURCHASE_PRICE as NUMERIC) > 0)
      ORDER BY bitems.CHANGE_DATE desc, bitems.CREATE_DATE DESC
    )



    ---------------------------PIVOT AT BASKET ITEM LEVEL AND BREAK IT DOWN IN COMPONENTS  ---------------------------------------------

    , pivotBasketItems_temp AS

    (
      SELECT
      BASKET_ID ,
      ID,
      CASE WHEN BYOSKU IS NULL AND SUBSTR(SKU,0,2) = 'LD'  THEN 'LD' 
           WHEN BYOSKU IS NULL THEN 'PREBUILT'
          ELSE BYOSKU END AS BYOSKU,
      MIN (CREATE_DATE) as CREATE_DATE,
      MAX(CHANGE_DATE) as LAST_CHANGE_DATE,
      MAX(seq) as ITEMS_COUNT,
      MAX(CASE WHEN BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD') THEN diamond_seq ELSE 0 END) as DIAMOND_COUNT,

      MAX(IF(diamond_seq = 1 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), SKU, NULL)) AS DIAMOND_SKU_1,
      MAX(IF(diamond_seq = 1 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DESCRIPTION, NULL)) AS DIAMOND_1_DISPLAY_NAME,
      MAX(IF(diamond_seq = 1 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), LINE_TOTAL, NULL)) AS DIAMOND_1_PRICE,
      MAX(CASE WHEN diamond_seq = 1 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD') THEN
           CASE WHEN STRPOS(DESCRIPTION, 'Cushion') > 0 THEN 'CU' 
                WHEN STRPOS(DESCRIPTION, 'Emerald') > 0 THEN 'EC' 
                WHEN STRPOS(DESCRIPTION, 'Asscher') > 0 THEN 'AS' 
                WHEN STRPOS(DESCRIPTION, 'Radiant') > 0 THEN 'RA' 
                WHEN STRPOS(DESCRIPTION, 'Princess') > 0 THEN 'PR' 
                WHEN STRPOS(DESCRIPTION, 'Oval') > 0 THEN 'OV' 
                WHEN STRPOS(DESCRIPTION, 'Pear') > 0 THEN 'PS' 
                WHEN STRPOS(DESCRIPTION, 'Heart') > 0 THEN 'HS'  
                WHEN STRPOS(DESCRIPTION, 'Marquise') > 0 THEN 'MQ'
                WHEN STRPOS(DESCRIPTION, 'Round') > 0 THEN 'RD'
            ELSE ''  END
         ELSE '' END ) as DIAMOND_1_SHAPE,

      MAX(IF(diamond_seq = 2 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), SKU, NULL)) AS DIAMOND_SKU_2,
      MAX(IF(diamond_seq = 2 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DESCRIPTION, NULL)) AS DIAMOND_2_DISPLAY_NAME,
      MAX(IF(diamond_seq = 2 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), LINE_TOTAL, NULL)) AS DIAMOND_2_PRICE,
      MAX(IF(diamond_seq = 2 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DIAMOND_SHAPE, NULL)) AS DIAMOND_2_SHAPE,

      MAX(IF(diamond_seq = 3 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), SKU, NULL)) AS DIAMOND_SKU_3,
      MAX(IF(diamond_seq = 3 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DESCRIPTION, NULL)) AS DIAMOND_3_DISPLAY_NAME,
      MAX(IF(diamond_seq = 3 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), LINE_TOTAL, NULL)) AS DIAMOND_3_PRICE,
      MAX(IF(diamond_seq = 3 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DIAMOND_SHAPE, NULL)) AS DIAMOND_3_SHAPE,



      MAX(IF(diamond_seq = 4 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), SKU, NULL)) AS DIAMOND_SKU_4,
      MAX(IF(diamond_seq = 4 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DESCRIPTION, NULL)) AS DIAMOND_4_DISPLAY_NAME,
      MAX(IF(diamond_seq = 4 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), LINE_TOTAL, NULL)) AS DIAMOND_4_PRICE,
      MAX(IF(diamond_seq = 4 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DIAMOND_SHAPE, NULL)) AS DIAMOND_4_SHAPE,



      MAX(IF(diamond_seq = 5 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), SKU, NULL)) AS DIAMOND_SKU_5,
      MAX(IF(diamond_seq = 5 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DESCRIPTION, NULL)) AS DIAMOND_5_DISPLAY_NAME,
      MAX(IF(diamond_seq = 5 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), LINE_TOTAL, NULL)) AS DIAMOND_5_PRICE,
      MAX(IF(diamond_seq = 5 AND SUBSTR(SKU,0,2) = 'LD' AND BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD','BNPEND_MTD'), DIAMOND_SHAPE, NULL)) AS DIAMOND_5_SHAPE,


      --ENGRAVING
      MAX(IF(SUBSTR(SKU,0,5) = 'ENGRA', SKU, NULL)) AS ENGRAVING,
      MAX(IF(SUBSTR(SKU,0,5) = 'ENGRA', ENGRAVING_STYLE, NULL)) AS ENGRAVING_STYLE,
      MAX(IF(SUBSTR(SKU,0,5) = 'ENGRA', ENGRAVING_FONT, NULL)) AS ENGRAVING_FONT,
      MAX(IF(SUBSTR(SKU,0,5) = 'ENGRA', ENGRAVING_TEXT, NULL)) AS ENGRAVING_TEXT,
      MAX(IF(SUBSTR(SKU,0,5) = 'ENGRA', LINE_TOTAL, NULL)) AS ENGRAVING_PRICE,


      MAX(IF (
      (BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD', 'BNPEND_MTD') AND SUBSTR(MERCH_PRODUCT_CATEGORY,0,3) = 'BYO')
          AND SUBSTR(SKU,0,2) not in ('CH', 'LD'), SKU, NULL)) AS SETTING_SKU,
      MAX(IF (
      (BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD', 'BNPEND_MTD') AND SUBSTR(MERCH_PRODUCT_CATEGORY,0,3) = 'BYO')
          AND SUBSTR(SKU,0,2) not in ('CH', 'LD'), OFFER_ID, NULL)) AS SETTING_OFFER_ID,
      MAX(IF (
      (BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD', 'BNPEND_MTD') AND SUBSTR(MERCH_PRODUCT_CATEGORY,0,3) = 'BYO')
          AND SUBSTR(SKU,0,2) not in ('CH', 'LD'), ITEM_SIZE, NULL)) AS SETTING_SIZE,
      MAX(IF (
      (BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD', 'BNPEND_MTD') AND SUBSTR(MERCH_PRODUCT_CATEGORY,0,3) = 'BYO')
          AND SUBSTR(SKU,0,2) not in ('CH', 'LD'), LINE_TOTAL, NULL)) AS SETTING_PRICE,
      MAX(IF (
      (BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD', 'BNPEND_MTD') AND SUBSTR(MERCH_PRODUCT_CATEGORY,0,3) = 'BYO')
          AND SUBSTR(SKU,0,2) not in ('CH', 'LD'), DESCRIPTION, NULL)) AS SETTING_DISPLAY_NAME,
      MAX(IF (
      (BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD', 'BNPEND_MTD') AND SUBSTR(MERCH_PRODUCT_CATEGORY,0,3) = 'BYO')
          AND SUBSTR(SKU,0,2) not in ('CH', 'LD'), MERCH_PRODUCT_CATEGORY, NULL)) AS SETTING_MERCH_PRODUCT_CATEGORY,
      MAX(IF (
      (BYOSKU IN ('BN3STN_MTD','BN5STN_MTD','BNEAR_SET','BNRING_MTD', 'BNPEND_MTD') AND SUBSTR(MERCH_PRODUCT_CATEGORY,0,3) = 'BYO')
          AND SUBSTR(SKU,0,2) not in ('CH', 'LD'), item_category, NULL)) AS SETTING_ITEM_CATEGORY,


      MAX(IF(BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'), SKU, NULL)) AS ITEM_SKU,
      MAX(IF(BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'), OFFER_ID, NULL)) AS ITEM_OFFER_ID,
      MAX(IF(BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'), ITEM_SIZE, NULL)) AS ITEM_SIZE,
      MAX(IF(BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'), LINE_TOTAL, NULL)) AS ITEM_PRICE,
      MAX(IF(BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'), DESCRIPTION, NULL)) AS ITEM_DISPLAY_NAME,
      MAX(IF(BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'), MERCH_PRODUCT_CATEGORY, NULL)) AS ITEM_MERCH_PRODUCT_CATEGORY,
      MAX(IF(BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'), ITEM_CATEGORY, NULL)) AS ITEM_ITEM_CATEGORY,

      MAX(IF (SUBSTR(SKU,0,2) in ('CH') AND BYOSKU = 'BNPEND_MTD', SKU, NULL)) AS CHAIN_SKU,
      MAX(IF (SUBSTR(SKU,0,2) in ('CH') AND BYOSKU = 'BNPEND_MTD', OFFER_ID, NULL)) AS CHAIN_OFFER_ID,
      MAX(IF (SUBSTR(SKU,0,2) in ('CH') AND BYOSKU = 'BNPEND_MTD', LINE_TOTAL, NULL)) AS CHAIN_PRICE,
      MAX(IF (SUBSTR(SKU,0,2) in ('CH') AND BYOSKU = 'BNPEND_MTD', DESCRIPTION, NULL)) AS CHAIN_DISPLAY_NAME,

      --Wedding Band items
      MAX(IF(item_category = 'WB' AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), SKU, NULL)) AS WB_SKU,
      MAX(IF(item_category = 'WB' AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), OFFER_ID, NULL)) AS WB_OFFER_ID,
      MAX(IF(item_category = 'WB' AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), ITEM_SIZE, NULL)) AS WB_SIZE,
      MAX(IF(item_category = 'WB' AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), LINE_TOTAL, NULL)) AS WB_PRICE,
      MAX(IF(item_category = 'WB' AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), DESCRIPTION, NULL)) AS WB_DISPLAY_NAME,
      MAX(IF(item_category = 'WB' AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), MERCH_PRODUCT_CATEGORY, NULL)) AS WB_MERCH_PRODUCT_CATEGORY,
      MAX(IF(item_category = 'WB' AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), item_category, NULL)) AS WB_ITEM_CATEGORY,

      --Other items
      MAX(IF((merch_product_category is null OR item_category not in ('WB')) AND (BYOSKU IS NULL OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), SKU, NULL)) AS OTHER_ITEM_SKU,
      MAX(IF((merch_product_category is null OR item_category not in ('WB')) AND (BYOSKU IS NULL  OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), OFFER_ID, NULL)) AS OTHER_ITEM_OFFER_ID,
      MAX(IF((merch_product_category is null OR item_category not in ('WB')) AND (BYOSKU IS NULL  OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), ITEM_SIZE, NULL)) AS OTHER_ITEM_SIZE,
      MAX(IF((merch_product_category is null OR item_category not in ('WB')) AND (BYOSKU IS NULL  OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), LINE_TOTAL, NULL)) AS OTHER_ITEM_PRICE,
      MAX(IF((merch_product_category is null OR item_category not in ('WB')) AND (BYOSKU IS NULL  OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), DESCRIPTION, NULL)) AS OTHER_ITEM_DISPLAY_NAME,
      MAX(IF((merch_product_category is null OR item_category not in ('WB')) AND (BYOSKU IS NULL  OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), MERCH_PRODUCT_CATEGORY, NULL)) AS OTHER_ITEM_MERCH_PRODUCT_CATEGORY,
      MAX(IF((merch_product_category is null OR item_category not in ('WB')) AND (BYOSKU IS NULL  OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))), ITEM_CATEGORY, NULL)) AS OTHER_ITEM_ITEM_CATEGORY,

      MAX(CASE 
        WHEN (
        (merch_product_category is null OR item_category not in ('WB'))
          AND (BYOSKU IS NULL  OR (BYOSKU = 'BNITEM_ENG' AND SUBSTR(SKU,0,5) not in ('ENGRA'))) 
            AND SUBSTR(SKU,0,2) = 'LD' 
            )  THEN 
                 CASE WHEN STRPOS(DESCRIPTION, 'Cushion') > 0 THEN 'CU' 
                      WHEN STRPOS(DESCRIPTION, 'Emerald') > 0 THEN 'EC' 
                      WHEN STRPOS(DESCRIPTION, 'Asscher') > 0 THEN 'AS' 
                      WHEN STRPOS(DESCRIPTION, 'Radiant') > 0 THEN 'RA' 
                      WHEN STRPOS(DESCRIPTION, 'Princess') > 0 THEN 'PR' 
                      WHEN STRPOS(DESCRIPTION, 'Oval') > 0 THEN 'OV' 
                      WHEN STRPOS(DESCRIPTION, 'Pear') > 0 THEN 'PS' 
                      WHEN STRPOS(DESCRIPTION, 'Heart') > 0 THEN 'HS'  
                      WHEN STRPOS(DESCRIPTION, 'Marquise') > 0 THEN 'MQ'
                      WHEN STRPOS(DESCRIPTION, 'Round') > 0 THEN 'RD'
                      ELSE ''  END
              ELSE '' END)  AS OTHER_ITEM_SHAPE,

      FROM tempBasketItems
      GROUP BY BASKET_ID,ID,
      BYOSKU
      ORDER BY CREATE_DATE DESC
     )
     , NonEngagementBaskets AS
     (
      SELECT basket_id, count(*) as cnt 
      FROM pivotBasketItems_temp  
      WHERE BYOSKU not in ('LD') AND  COALESCE(SETTING_ITEM_CATEGORY, ITEM_ITEM_CATEGORY, WB_ITEM_CATEGORY,OTHER_ITEM_ITEM_CATEGORY) not in ('Eng', 'LD')
      GROUP BY basket_id
     )


     , ConsolidateBasketatID AS
    (
      SELECT 
      --ROW_NUMBER() OVER (PARTITION BY A.BASKET_ID, BYOSKU ORDER BY LAST_CHANGE_DATE desc, A.CREATE_DATE DESC) seq,
      ROW_NUMBER() OVER (PARTITION BY A.BASKET_ID, CASE WHEN BYOSKU IN ('LD','PREBUILT') THEN 'PREBUILT' ELSE BYOSKU END ORDER BY LAST_CHANGE_DATE desc, A.CREATE_DATE DESC) seq,
      CASE WHEN no_eng.basket_id IS NOT NULL AND BYOSKU in ('LD') THEN 'Loose Diamonds' ELSE 
      COALESCE(A.SETTING_MERCH_PRODUCT_CATEGORY, A.ITEM_MERCH_PRODUCT_CATEGORY, A.WB_MERCH_PRODUCT_CATEGORY,A.OTHER_ITEM_MERCH_PRODUCT_CATEGORY) END AS BASKET_ITEM_MERCH_PRODUCT_CATEGORY,
      CASE WHEN no_eng.basket_id IS NOT NULL AND BYOSKU in ('LD') THEN 'DJ' ELSE 
      COALESCE(A.SETTING_ITEM_CATEGORY, A.ITEM_ITEM_CATEGORY, A.WB_ITEM_CATEGORY,A.OTHER_ITEM_ITEM_CATEGORY) END AS BASKET_ITEM_ITEM_CATEGORY,   
      A.*, 
      CASE WHEN SETTING_SIZE IS NOT NULL THEN CONCAT(SETTING_DISPLAY_NAME, ' in Size ' , SETTING_SIZE) ELSE SETTING_DISPLAY_NAME END AS SETTING_DISPLAY_NAME_1,
      CASE WHEN setting_offer_id is not null then COALESCE(od_ring_setting.image_link ,os.IMAGE_URL) ELSE NULL END AS SETTING_base_image_link,

      COALESCE(A.ITEM_SKU, A.WB_SKU,A.OTHER_ITEM_SKU) AS SKU,
      COALESCE(A.ITEM_OFFER_ID, A.WB_OFFER_ID,A.OTHER_ITEM_OFFER_ID) AS OFFER_ID,
      COALESCE(A.ITEM_MERCH_PRODUCT_CATEGORY, A.WB_MERCH_PRODUCT_CATEGORY,A.OTHER_ITEM_MERCH_PRODUCT_CATEGORY) AS MERCH_PRODUCT_CATEGORY,  
      COALESCE(A.ITEM_ITEM_CATEGORY, A.WB_ITEM_CATEGORY,A.OTHER_ITEM_ITEM_CATEGORY) AS ITEM_CATEGORY,  
      COALESCE(A.ITEM_SIZE, A.WB_SIZE,A.OTHER_ITEM_SIZE) AS SIZE,
      COALESCE(A.ITEM_PRICE, A.WB_PRICE,A.OTHER_ITEM_PRICE) AS PRICE,
      IF ( COALESCE(A.ITEM_SIZE, A.WB_SIZE,A.OTHER_ITEM_SIZE) IS NOT NULL, CONCAT(COALESCE(A.ITEM_DISPLAY_NAME, A.WB_DISPLAY_NAME,A.OTHER_ITEM_DISPLAY_NAME), ' in Size ' , COALESCE(A.ITEM_SIZE, A.WB_SIZE,A.OTHER_ITEM_SIZE) ), COALESCE(A.ITEM_DISPLAY_NAME, A.WB_DISPLAY_NAME,A.OTHER_ITEM_DISPLAY_NAME)) AS DISPLAY_NAME,
     CASE WHEN ENGRAVING IS NOT NULl THEN CONCAT('Engraving : Style: ', ENGRAVING_STYLE, ', Font : ', ENGRAVING_FONT, ', Text : ',ENGRAVING_TEXT) ELSE ENGRAVING END AS ENGRAVE_DISPLAY_NAME,
     os.IMAGE_URL AS base_image_link,
      FROM pivotBasketItems_temp A
      LEFT JOIN NonEngagementBaskets no_eng ON no_eng.basket_id = A.basket_id and A.BYOSKU = 'LD'
      LEFT JOIN (SELECT OFFER_ID, diamond_shape, CONCAT(IMAGE_LINK,'&') AS IMAGE_LINK FROM {project_name}.o_product.byo_offer_images_dim ) od_ring_setting ON CAST(od_ring_setting.Offer_id AS STRING) = CAST(A.SETTING_OFFER_ID AS STRING) AND od_ring_setting.diamond_shape = A.DIAMOND_1_SHAPE AND BYOSKU IN ('BNRING_MTD') -- rings need to look at the shape
      LEFT JOIN OfferSummary os on os.offer_id = COALESCE(CAST(COALESCE(A.SETTING_OFFER_ID, A.ITEM_OFFER_ID, A.WB_OFFER_ID,A.OTHER_ITEM_OFFER_ID) AS STRING),A.OTHER_ITEM_SKU) AND BYOSKU NOT IN ('BNRING_MTD')
    )



    , pivotBasketItems AS
    (
      SELECT BASKET_ID,
      COUNT( DISTINCT ID) AS TOTAL_ITEMS_IN_BASKET,
      MAX(CASE WHEN BASKET_ITEM_ITEM_CATEGORY = 'WB' THEN 1 ELSE 0 END) AS WB_FLAG,
      MAX(CASE WHEN BASKET_ITEM_ITEM_CATEGORY in ('Eng') THEN 1 ELSE 0 END) AS ENG_FLAG,
      MAX(CASE WHEN BASKET_ITEM_ITEM_CATEGORY in ('OJ', 'DJ') THEN 1 ELSE 0 END) AS JL_FLAG,
      MAX(CASE WHEN BYOSKU = 'LD' THEN 1 ELSE 0 END) AS LD_FLAG, --LD_ONLY_FLAG,


      --BNRING_MTD
       MAX(IF(seq  = 1 AND BYOSKU = 'BNRING_MTD' , ID, NULL)) AS BNRING_MTD_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNRING_MTD' , SETTING_OFFER_ID, NULL)) AS BNRING_SETTING_OFFER_ID_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNRING_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BNRING_SETTING_MERCH_PRODUCT_CATEGORY_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNRING_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BNRING_SETTING_ITEM_CATEGORY_1,

       MAX(IF(seq  = 2 AND BYOSKU = 'BNRING_MTD' , ID, NULL)) AS BNRING_MTD_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNRING_MTD' , SETTING_OFFER_ID, NULL)) AS BNRING_SETTING_OFFER_ID_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNRING_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BNRING_SETTING_MERCH_PRODUCT_CATEGORY_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNRING_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BNRING_SETTING_ITEM_CATEGORY_2,

      --BN3STN_MTD
       MAX(IF(seq  = 1 AND BYOSKU = 'BN3STN_MTD' , ID, NULL)) AS BN3STN_MTD_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BN3STN_MTD' , SETTING_OFFER_ID, NULL)) AS BN3STN_SETTING_OFFER_ID_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BN3STN_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BN3STN_SETTING_MERCH_PRODUCT_CATEGORY_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BN3STN_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BN3STN_SETTING_ITEM_CATEGORY_1,

       MAX(IF(seq  = 2 AND BYOSKU = 'BN3STN_MTD' , ID, NULL)) AS BN3STN_MTD_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BN3STN_MTD' , SETTING_OFFER_ID, NULL)) AS BN3STN_SETTING_OFFER_ID_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BN3STN_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BN3STN_SETTING_MERCH_PRODUCT_CATEGORY_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BN3STN_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BN3STN_SETTING_ITEM_CATEGORY_2,


      --BN5STN_MTD
       MAX(IF(seq  = 1 AND BYOSKU = 'BN5STN_MTD' , ID, NULL)) AS BN5STN_MTD_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BN5STN_MTD' , SETTING_OFFER_ID, NULL)) AS BN5STN_SETTING_OFFER_ID_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BN5STN_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BN5STN_SETTING_MERCH_PRODUCT_CATEGORY_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BN5STN_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BN5STN_SETTING_ITEM_CATEGORY_1,

       MAX(IF(seq  = 2 AND BYOSKU = 'BN5STN_MTD' , ID, NULL)) AS BN5STN_MTD_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BN5STN_MTD' , SETTING_OFFER_ID, NULL)) AS BN5STN_SETTING_OFFER_ID_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BN5STN_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BN5STN_SETTING_MERCH_PRODUCT_CATEGORY_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BN5STN_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BN5STN_SETTING_ITEM_CATEGORY_2,

      --BNEAR_SET
       MAX(IF(seq  = 1 AND BYOSKU = 'BNEAR_SET' , ID, NULL)) AS BNEAR_SET_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNEAR_SET' , SETTING_OFFER_ID, NULL)) AS BNEAR_SETTING_OFFER_ID_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNEAR_SET' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BNEAR_SETTING_MERCH_PRODUCT_CATEGORY_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNEAR_SET' , SETTING_ITEM_CATEGORY, NULL)) AS BNEAR_SETTING_ITEM_CATEGORY_1,


       MAX(IF(seq  = 2 AND BYOSKU = 'BNEAR_SET' , ID, NULL)) AS BNEAR_SET_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNEAR_SET' , SETTING_OFFER_ID, NULL)) AS BNEAR_SETTING_OFFER_ID_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNEAR_SET' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BNEAR_SETTING_MERCH_PRODUCT_CATEGORY_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNEAR_SET' , SETTING_ITEM_CATEGORY, NULL)) AS BNEAR_SETTING_ITEM_CATEGORY_2,


      --BNPEND_MTD
       MAX(IF(seq  = 1 AND BYOSKU = 'BNPEND_MTD' , ID, NULL)) AS BNPEND_MTD_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNPEND_MTD' , SETTING_OFFER_ID, NULL)) AS BNPEND_SETTING_OFFER_ID_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNPEND_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BNPEND_SETTING_MERCH_PRODUCT_CATEGORY_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNPEND_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BNPEND_SETTING_ITEM_CATEGORY_1,


       MAX(IF(seq  = 2 AND BYOSKU = 'BNPEND_MTD' , ID, NULL)) AS BNPEND_MTD_2, 
       MAX(IF(seq  = 2 AND BYOSKU = 'BNPEND_MTD' , SETTING_OFFER_ID, NULL)) AS BNPEND_SETTING_OFFER_ID_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNPEND_MTD' , SETTING_MERCH_PRODUCT_CATEGORY, NULL)) AS BNPEND_SETTING_MERCH_PRODUCT_CATEGORY_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNPEND_MTD' , SETTING_ITEM_CATEGORY, NULL)) AS BNPEND_SETTING_ITEM_CATEGORY_2,

      --BNITEM_ENG
       MAX(IF(seq  = 1 AND BYOSKU = 'BNITEM_ENG' , ID, NULL)) AS BNITEM_ENG_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNITEM_ENG' , ITEM_OFFER_ID, NULL)) AS BNITEM_ITEM_OFFER_ID_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNITEM_ENG' , ITEM_MERCH_PRODUCT_CATEGORY, NULL)) AS BNITEM_ITEM_MERCH_PRODUCT_CATEGORY_1,
       MAX(IF(seq  = 1 AND BYOSKU = 'BNITEM_ENG' , ITEM_ITEM_CATEGORY, NULL)) AS BNITEM_ITEM_ITEM_CATEGORY_1,

       MAX(IF(seq  = 2 AND BYOSKU = 'BNITEM_ENG' , ID, NULL)) AS BNITEM_ENG_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNITEM_ENG' , ITEM_OFFER_ID, NULL)) AS BNITEM_ITEM_OFFER_ID_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNITEM_ENG' , ITEM_MERCH_PRODUCT_CATEGORY, NULL)) AS BNITEM_ITEM_MERCH_PRODUCT_CATEGORY_2,
       MAX(IF(seq  = 2 AND BYOSKU = 'BNITEM_ENG' , ITEM_ITEM_CATEGORY, NULL)) AS BNITEM_ITEM_ITEM_CATEGORY_2,



      --OTHER ITEMS
       MAX(IF(seq  = 1 AND BYOSKU IN ('PREBUILT', 'LD') , ID, NULL)) AS PREBUILT_SKU_1,
       MAX(IF(seq  = 2 AND BYOSKU IN ('PREBUILT', 'LD') , ID, NULL)) AS PREBUILT_SKU_2,
       MAX(IF(seq  = 3 AND BYOSKU IN ('PREBUILT', 'LD') , ID, NULL)) AS PREBUILT_SKU_3,
       MAX(IF(seq  = 4 AND BYOSKU IN ('PREBUILT', 'LD') , ID, NULL)) AS PREBUILT_SKU_4,
       MAX(IF(seq  = 5 AND BYOSKU IN ('PREBUILT', 'LD') , ID, NULL)) AS PREBUILT_SKU_5,
       MAX(IF(seq  = 1 AND BYOSKU IN ('PREBUILT', 'LD') , OFFER_ID, NULL)) AS PREBUILT_OFFER_ID,
       MAX(IF(seq  = 1 AND BYOSKU IN ('PREBUILT', 'LD') , MERCH_PRODUCT_CATEGORY, NULL)) AS PREBUILT_MERCH_PRODUCT_CATEGORY,
       MAX(IF(seq  = 1 AND BYOSKU IN ('PREBUILT', 'LD') , ITEM_CATEGORY, NULL)) AS PREBUILT_ITEM_CATEGORY

      FROM ConsolidateBasketatID
      GROUP BY BASKET_ID
    )



    , finalBasketMain AS
    (
      SELECT 
      B.bnid,
      B.BASKET_ID, 
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(BASKET_STATUS_DATE as timestamp),HOUR) as HOURS_SINCE_LAST_UPDATE,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP() ,cast(BASKET_STATUS_DATE as timestamp),DAY) as DAYS_SINCE_LAST_UPDATE,
      CUSTOMER_LANGUAGE_CODE, HOSTNAME,SITE, CURRENCY_CODE, CUSTOMER_ID, BASKET_IP_ADDRESS, CUSTOMER_GUID,
      EMAIL_ADDRESS,FIRST_NAME,LAST_NAME,PHONE_NUMBER,SOURCE_CODE,INPUT_METHOD,ORDER_TYPE,SHIP_TO_COUNTRY_CODE,
      BASKET_START_DATE,BASKET_STATUS_DATE,PRIMARY_CONTACT_METHOD, ITEM_TOTAL_PRICE,
      CONCAT (CAST(ROUND(ITEM_TOTAL_PRICE,0) AS STRING), ' ' ,CURRENCY_CODE ) as PRICE_DISPLAY_NAME ,
      A.BASKET_ID as b_BASKET_ID,
      TOTAL_ITEMS_IN_BASKET,
      WB_FLAG,
      ENG_FLAG,
      JL_FLAG,
      CASE WHEN WB_FLAG = 0 AND ENG_FLAG =0 AND JL_FLAG = 0 AND LD_FLAG = 1 THEN 1 ELSE 0 END AS LD_ONLY_FLAG,
      COALESCE(BNRING_MTD_1,BNRING_MTD_2,BN3STN_MTD_1,BN3STN_MTD_2,BN5STN_MTD_1,BN5STN_MTD_2, BNEAR_SET_1, BNEAR_SET_2,BNPEND_MTD_1,BNPEND_MTD_2,BNITEM_ENG_1,BNITEM_ENG_2,PREBUILT_SKU_1,PREBUILT_SKU_2,PREBUILT_SKU_3,PREBUILT_SKU_4,PREBUILT_SKU_5) AS HERO_ITEM_ID,
      COALESCE(BNRING_SETTING_OFFER_ID_1, BNRING_SETTING_OFFER_ID_2, BN3STN_SETTING_OFFER_ID_1,BN3STN_SETTING_OFFER_ID_2,BN5STN_SETTING_OFFER_ID_1,BN5STN_SETTING_OFFER_ID_2,
      BNEAR_SETTING_OFFER_ID_1, BNEAR_SETTING_OFFER_ID_2,BNPEND_SETTING_OFFER_ID_1, BNPEND_SETTING_OFFER_ID_2, BNITEM_ITEM_OFFER_ID_1,BNITEM_ITEM_OFFER_ID_2, PREBUILT_OFFER_ID) AS HERO_Offer_ID,
      COALESCE(BNRING_SETTING_MERCH_PRODUCT_CATEGORY_1, BNRING_SETTING_MERCH_PRODUCT_CATEGORY_2, BN3STN_SETTING_MERCH_PRODUCT_CATEGORY_1, BN3STN_SETTING_MERCH_PRODUCT_CATEGORY_2, BN5STN_SETTING_MERCH_PRODUCT_CATEGORY_1, BN5STN_SETTING_MERCH_PRODUCT_CATEGORY_2, 
      BNEAR_SETTING_MERCH_PRODUCT_CATEGORY_1, BNEAR_SETTING_MERCH_PRODUCT_CATEGORY_2, BNPEND_SETTING_MERCH_PRODUCT_CATEGORY_1, BNPEND_SETTING_MERCH_PRODUCT_CATEGORY_2, BNITEM_ITEM_MERCH_PRODUCT_CATEGORY_1,BNITEM_ITEM_MERCH_PRODUCT_CATEGORY_2, PREBUILT_MERCH_PRODUCT_CATEGORY) AS HERO_MPC,
     COALESCE(BNRING_SETTING_ITEM_CATEGORY_1, BNRING_SETTING_ITEM_CATEGORY_2, BN3STN_SETTING_ITEM_CATEGORY_1, BN3STN_SETTING_ITEM_CATEGORY_2, BN5STN_SETTING_ITEM_CATEGORY_1, BN5STN_SETTING_ITEM_CATEGORY_2, 
      BNEAR_SETTING_ITEM_CATEGORY_1, BNEAR_SETTING_ITEM_CATEGORY_2, BNPEND_SETTING_ITEM_CATEGORY_1, BNPEND_SETTING_ITEM_CATEGORY_2, BNITEM_ITEM_ITEM_CATEGORY_1,BNITEM_ITEM_ITEM_CATEGORY_2, PREBUILT_ITEM_CATEGORY) AS HERO_ITEM_CATEGORY,

      CASE WHEN BNRING_MTD_1 IS NOT NULL AND BNRING_MTD_2 IS NOT NULL THEN CONCAT(CAST(BNRING_MTD_1 AS STRING)," ,",CAST(BNRING_MTD_2 AS STRING)) 
           WHEN BNRING_MTD_1 IS NOT NULL AND BNRING_MTD_2 IS NULL THEN CAST(BNRING_MTD_1 AS STRING)
           WHEN BNRING_MTD_1 IS  NULL AND BNRING_MTD_2 IS NOT NULL THEN CAST(BNRING_MTD_2 AS STRING)
           ELSE NULL END AS BNRING_MTD_IDS,
      CASE WHEN BN3STN_MTD_1 IS NOT NULL AND BN3STN_MTD_2 IS NOT NULL THEN CONCAT(CAST(BN3STN_MTD_1 AS STRING)," ,",CAST(BN3STN_MTD_2 AS STRING)) 
           WHEN BN3STN_MTD_1 IS NOT NULL AND BN3STN_MTD_2 IS NULL THEN CAST(BN3STN_MTD_1 AS STRING)
           WHEN BN3STN_MTD_1 IS NULL AND BN3STN_MTD_2 IS NOT NULL THEN CAST(BN3STN_MTD_2 AS STRING)
           ELSE NULL END AS BN3STN_MTD_IDS,
      CASE WHEN BN5STN_MTD_1 IS NOT NULL AND BN5STN_MTD_2 IS NOT NULL THEN CONCAT(CAST(BN5STN_MTD_1 AS STRING)," ,",CAST(BN5STN_MTD_2 AS STRING)) 
           WHEN BN5STN_MTD_1 IS NOT NULL AND BN5STN_MTD_2 IS NULL THEN CAST(BN5STN_MTD_1 AS STRING)
           WHEN BN5STN_MTD_1 IS  NULL AND BN5STN_MTD_2 IS NOT NULL THEN CAST(BN3STN_MTD_2 AS STRING)
           ELSE NULL END AS BN5STN_MTD_IDS, 
      CASE WHEN BNEAR_SET_1 IS NOT NULL AND BNEAR_SET_2 IS NOT NULL THEN CONCAT(CAST(BNEAR_SET_1 AS STRING)," ,",CAST(BNEAR_SET_2 AS STRING)) 
           WHEN BNEAR_SET_1 IS NOT NULL AND BNEAR_SET_2 IS NULL THEN CAST(BNEAR_SET_1 AS STRING)
           WHEN BNEAR_SET_1 IS  NULL AND BNEAR_SET_2 IS NOT NULL THEN CAST(BNEAR_SET_2 AS STRING)
           ELSE NULL END AS BNEAR_SET_IDS,
      CASE WHEN BNPEND_MTD_1 IS NOT NULL AND BNPEND_MTD_2 IS NOT NULL THEN CONCAT(CAST(BNPEND_MTD_1 AS STRING)," ,",CAST(BNPEND_MTD_2 AS STRING)) 
           WHEN BNPEND_MTD_1 IS NOT NULL AND BNPEND_MTD_2 IS NULL THEN CAST(BNPEND_MTD_1 AS STRING)
           WHEN BNPEND_MTD_1 IS  NULL AND BNPEND_MTD_2 IS NOT NULL THEN CAST(BNPEND_MTD_2 AS STRING)
           ELSE NULL END AS BNPEND_MTD_IDS,
      CASE WHEN BNITEM_ENG_1 IS NOT NULL AND BNITEM_ENG_2 IS NOT NULL THEN CONCAT(CAST(BNITEM_ENG_1 AS STRING)," ,",CAST(BNITEM_ENG_2 AS STRING)) 
           WHEN BNITEM_ENG_1 IS NOT NULL AND BNITEM_ENG_2 IS NULL THEN CAST(BNITEM_ENG_1 AS STRING)
           WHEN BNITEM_ENG_1 IS  NULL AND BNITEM_ENG_2 IS NOT NULL THEN CAST(BNITEM_ENG_2 AS STRING)
           ELSE NULL END AS BNITEM_ENG_IDS,
      CONCAT(
      IF(PREBUILT_SKU_1 is null,"",CAST(PREBUILT_SKU_1 AS STRING)) , IF(PREBUILT_SKU_2 is null,'' ,',') , IF(PREBUILT_SKU_2 is null,'',CAST(PREBUILT_SKU_2 AS STRING)), IF(PREBUILT_SKU_3 is null,'',',') , IF(PREBUILT_SKU_3 is null,'',CAST(PREBUILT_SKU_3 AS STRING)), IF(PREBUILT_SKU_4 is null,'',',') , IF(PREBUILT_SKU_4 is null,'',CAST(PREBUILT_SKU_4 AS STRING)), IF(PREBUILT_SKU_5 is null,'',',') , IF(PREBUILT_SKU_5 is null,'',CAST(PREBUILT_SKU_5 AS STRING))
      ) AS OTHER_IDS
      FROM pivotBasketItems A
      JOIN tempBaskets B ON A.BASKET_ID = B.BASKET_ID
    )



    , BasketAudienceView_Temp AS
    (
      SELECT 
      ROW_NUMBER() OVER (PARTITION BY COALESCE(A.EMAIL_ADDRESS, C.email_address) ORDER BY BASKET_STATUS_DATE desc) AS priority,
      COALESCE(A.bnid, C.bnid) as bnid, 
      A.customer_guid, 
      A.EMAIL_ADDRESS,
      C.up_email_address_key,
      IFNULL (email_address_test_key,1) AS email_address_test_key, 
      CASE WHEN email_contactable_flag IS NULL THEN 'Y' ELSE email_contactable_flag END email_contactable_flag, 
      COALESCE(A.SITE,C.primary_site) as primary_site, 
      COALESCE(A.HOSTNAME,C.primary_hostname) as primary_hostname, 
      COALESCE(A.CUSTOMER_LANGUAGE_CODE,C.primary_language_code) as primary_language_code, 
      COALESCE(A.CURRENCY_CODE,C.primary_currency_code) as currency_code, 
      CASE WHEN subscribe_flag IS NULL AND COALESCE(C.primary_site, A.SITE) = 'BN' THEN 'Y' ELSE subscribe_flag END subscribe_flag,  --ONLY FOR US
      CASE WHEN never_send_flag IS NULL THEN 'Y' ELSE never_send_flag END never_send_flag, 
      CASE WHEN fraud_flag IS NULL THEN 'Y' ELSE fraud_flag END fraud_flag, 
      CASE WHEN pending_order_flag IS NULL THEN 'N' ELSE pending_order_flag END pending_order_flag, 
      region, queens_english_flag , 

      COALESCE(D.first_name , A.FIRST_NAME) as first_name, 
      COALESCE(D.last_name , A.LAST_NAME) as last_name, 
      COALESCE(D.gender , D.derived_gender ) as gender, 
      CAST(C.last_subscribe_date AS datetime) as last_subscribe_date,
      C.bad_address_flag as bad_address_flag, 
      cast(C.undeliverable_date as datetime) as undelivered_date,
      C.cust_dna AS cust_dna, 
      A.BASKET_ID as basket_id,  
      A.CUSTOMER_ID as customer_id, 
      A.BASKET_IP_ADDRESS as basket_ip_address,  
      A.PHONE_NUMBER as phone_number, A.SOURCE_CODE as source_code, A.INPUT_METHOD as input_method, A.ORDER_TYPE as order_type, 
       cast(A.basket_start_date as datetime) as basket_start_date, cast(A.basket_status_date as datetime) as basket_status_date , A.primary_contact_method , 
       round(A.item_total_price,0) as item_total_price , A.price_display_name , A.total_items_in_basket , 
      eng_flag,
      wb_flag,
      jl_flag,
      ld_only_flag,
        CASE WHEN wb_flag = 1 AND eng_flag = 1 THEN 1 ELSE 0 END AS wb_eng_ring_flag ,
      hero_item_id,
      hero_offer_id,
      hero_mpc,
      hero_item_category,
      A.bnring_mtd_ids , A.bn3stn_mtd_ids , A.bn5stn_mtd_ids , A.bnear_set_ids ,A.bnpend_mtd_ids , A.bnitem_eng_ids , A.other_ids ,
      FROM finalBasketMain A
      LEFT JOIN {project_name}.o_customer.email_customer_profile C ON C.email_address = A.email_address
      LEFT JOIN {project_name}.o_customer.bn_customer_profile D ON D.bnid = A.bnid
      WHERE up_email_address_key IS NOT NULL
    )


    ---------------------------------------Similar Offer Recommendations ---------------------------------------------------------------------
    , Similar_Offers AS
    (
    SELECT seed_oid, oid, min(rnk) as rnk
    FROM
        (
          SELECT seed_oid, OID_rank1 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
          UNION DISTINCT 
          SELECT seed_oid, OID_rank2 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
          UNION DISTINCT 
          SELECT seed_oid, OID_rank3 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
          UNION DISTINCT 
          SELECT seed_oid, OID_rank4 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
          UNION DISTINCT 
          SELECT seed_oid, OID_rank6 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
          UNION DISTINCT 
          SELECT seed_oid, OID_rank7 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
          UNION DISTINCT 
          SELECT seed_oid, OID_rank8 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
          UNION DISTINCT 
          SELECT seed_oid, OID_rank9 as oid, 10*RAND() as rnk FROM {project_name}.o_customer.recommendation_offer_offer_similarity
        ) B
    GROUP BY seed_oid, oid
    )
    , Ranked_similar_offers AS
    (
    SELECT ROW_NUMBER() OVER (PARTITION BY seed_oid ORDER BY rnk ASC ) rank,
    seed_oid, oid, json_data
    FROM Similar_Offers A
    JOIN TodayOfferSummary_JSON B ON CAST(A.oid as string) = B.offer_id
    ) 
    , Similar_offer_Recos AS
    (   
       SELECT seed_oid,
           MAX(IF(rank  = 1, json_data, NULL)) AS reco_1,
           MAX(IF(rank  = 2, json_data, NULL)) AS reco_2,
           MAX(IF(rank  = 3, json_data, NULL)) AS reco_3,
           MAX(IF(rank  = 4, json_data, NULL)) AS reco_4,
        FROM Ranked_similar_offers
        WHERE rank < 5 
        GROUP BY seed_oid
        ORDER BY seed_oid
    )

    --------------------------------------------------End of Similar Offer Recommendation -------------------------------------
    , lookup_basket_item_table AS
    (
     SELECT A.basket_id,
      id,
      A.basket_item_price,
      A.cta_url,
      CASE WHEN A.SETTING_DISPLAY_NAME IS NULL THEN A.DISPLAY_NAME ELSE A.SETTING_DISPLAY_NAME END AS display_name,
      CASE WHEN A.SETTING_BASE_IMAGE_LINK IS NULL THEN A.BASE_IMAGE_LINK ELSE A.SETTING_BASE_IMAGE_LINK END AS base_image_link,
      A.diamond_display_name,
      A.engrave_display_name,
      A.chain_display_name
      FROM 
      (
        SELECT
          BASKET_ID
        , CAST( ID AS STRING) AS ID
        , CASE WHEN BYOSKU IS NULL THEN 'PREBUILT' ELSE BYOSKU END AS BYO_OPTIONS
        , BASKET_ITEM_ITEM_CATEGORY AS BASKET_ITEM_CATEGORY
        , SETTING_DISPLAY_NAME_1 AS SETTING_DISPLAY_NAME
        , DIAMOND_1_DISPLAY_NAME AS DIAMOND_DISPLAY_NAME
        , ROUND(IFNULL (DIAMOND_1_PRICE,0) + IFNULL (DIAMOND_2_PRICE,0) + IFNULL (DIAMOND_3_PRICE,0)+ IFNULL (DIAMOND_4_PRICE,0) + IFNULL (DIAMOND_5_PRICE,0) + IFNULL (SETTING_PRICE,0) + IFNULL (CHAIN_PRICE,0) + IFNULL (ENGRAVING_PRICE,0) + IFNULL (PRICE,0),2) AS BASKET_ITEM_PRICE --
        , SETTING_base_image_link as SETTING_BASE_IMAGE_LINK
        , DISPLAY_NAME
        , base_image_link AS BASE_IMAGE_LINK
        , ENGRAVE_DISPLAY_NAME
        , CHAIN_DISPLAY_NAME
        ,  CASE WHEN BYOSKU in ('BN3STN_MTD','BN5STN_MTD','BNRING_MTD') THEN CONCAT('/build-your-own-ring/review?offerId=', CAST(SETTING_OFFER_ID as string), '&diamondSku=', CAST(DIAMOND_SKU_1 as STRING)) 
               WHEN BYOSKU in ('BNEAR_SET','BNPEND_MTD') THEN CONCAT('/_',CAST(SETTING_OFFER_ID as string))
               WHEN BYOSKU in ('LD') THEN CONCAT('/diamond-details/',CAST(SKU as string))
               ELSE CONCAT('/_',CAST(OFFER_ID as string)) END  AS cta_url
        FROM ConsolidateBasketatID
        --WHERE BASKET_ITEM_CATEGORY not in ('A')
      ) A JOIN (SELECT DISTINCT BASKET_ID FROM BasketAudienceView_Temp WHERE PRIORITY = 1 and subscribe_flag = 'Y') B on A.BASKET_ID = B.BASKET_ID

    )
    -------------------------------------Main Audience Table ----------------------------------
    , abandoned_baskets_audience AS
    (
    SELECT *
    FROM
    (
            select 
            ROW_NUMBER() OVER (PARTITION BY A.EMAIL_ADDRESS ORDER BY BASKET_STATUS_DATE desc) AS sup_priority,
            CASE WHEN eng_flag = 1 OR (wb_flag = 0 and jl_flag = 0 and ld_only_flag = 1) THEN 'LC' ELSE 'SC' END AS pj,
            CASE WHEN eng_flag = 1 OR (wb_flag = 0 and jl_flag = 0 and ld_only_flag = 1) THEN
          DATE_DIFF (CURRENT_DATE("America/Los_Angeles"),
          CASE WHEN DATE_DIFF(cast(basket_start_date as date), CAST(last_subscribe_date as date), day) < 0 THEN CAST(last_subscribe_date as date) 
               WHEN DATE_DIFF(cast(basket_status_date as date), CAST(basket_start_date as date), day) > 14 AND DATE_DIFF(cast(basket_status_date as date), CAST(last_subscribe_date as date), day) < 0 THEN CAST(last_subscribe_date as date) 
               WHEN DATE_DIFF(cast(basket_status_date as date), CAST(basket_start_date as date), day) > 14 THEN cast(basket_status_date as date)     
          ELSE cast(basket_start_date as date) END, day) 
          ELSE 
          DATE_DIFF (CURRENT_DATE("America/Los_Angeles"),
          CASE WHEN DATE_DIFF(cast(basket_start_date as date), CAST(last_subscribe_date as date), day) < 0 THEN CAST(last_subscribe_date as date) 
               WHEN DATE_DIFF(cast(basket_status_date as date), CAST(basket_start_date as date), day) > 4 AND DATE_DIFF(cast(basket_status_date as date), CAST(last_subscribe_date as date), day) < 0 THEN CAST(last_subscribe_date as date) 
               WHEN DATE_DIFF(cast(basket_status_date as date), CAST(basket_start_date as date), day) > 4 THEN cast(basket_status_date as date)     
          ELSE cast(basket_start_date as date) END, day) END as eligible_day,
            A.*,
            COALESCE(B.reco_1,C.reco_1,D.reco_1) as reco_offer_1,
            COALESCE(B.reco_2,C.reco_2,D.reco_2) as reco_offer_2,
            COALESCE(B.reco_3,C.reco_3,D.reco_3) as reco_offer_3,
            COALESCE(B.reco_4,C.reco_4,D.reco_4) as reco_offer_4,

            COALESCE(E.reco_1,F.reco_1) as reco_LD_1,
            COALESCE(E.reco_2,F.reco_2) as reco_LD_2,
            COALESCE(E.reco_3,F.reco_3) as reco_LD_3,
            COALESCE(E.reco_4,F.reco_4) as reco_LD_4,


           null as reco_eng_setting_ids, null as reco_eng_preset_ids, null as reco_ld_ids, null as reco_wb_ids, null as reco_jl_byo_ids, null as reco_jl_preset_ids
            FROM BasketAudienceView_Temp A 
            LEFT JOIN {project_name}.o_customer.bn_recommendations_json B ON A.BNID = B.BNID AND A.hero_mpc = specs AND B.type = 'merch_product_category'
            LEFT JOIN {project_name}.o_customer.bn_recommendations_json C ON A.BNID = C.BNID AND CASE WHEN A.hero_item_category = 'Eng' Then 'Eng-byo' ELSE A.hero_item_category END = C.specs AND C.type = 'item_category'
            LEFT JOIN Similar_offer_Recos D ON A.hero_Offer_id = D.seed_oid
            LEFT JOIN {project_name}.o_customer.bn_recommendations_json E ON A.BNID = E.BNID AND 'Loose Diamonds' = E.specs AND E.type = 'merch_product_category'
            LEFT JOIN {project_name}.o_customer.bn_recommendations_json F ON A.BNID = F.BNID AND F.specs = 'LD' AND F.type = 'item_category'

            WHERE A.PRIORITY = 1 and A.subscribe_flag = 'Y' and A.up_email_address_key is not null
     )
     WHERE sup_priority = 1
    )
    '''.format(project_name)
    return bql