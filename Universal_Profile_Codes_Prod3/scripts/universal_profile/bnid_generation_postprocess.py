def bnid_generation_postprocess():
    bql = '''
    --------------------dedup clean-----------------------

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.bnid_dedup_fin` AS
    SELECT
    *
    FROM (
    SELECT
        primary_bnid,
        duplicate_bnid,
        COUNT(DISTINCT primary_bnid) OVER(PARTITION BY duplicate_bnid) AS primary_cnt,
        COUNT(DISTINCT duplicate_bnid) OVER(PARTITION BY primary_bnid) AS duplicate_cnt,
    FROM (
        SELECT
        DISTINCT primary_bnid,
        duplicate_bnid
        FROM
        `bnile-cdw-prod.up_stg_dedup.bnid_dedup_merge` ))
    WHERE
    primary_cnt=1
    AND duplicate_cnt<=15;



    /*--------------------JOIN back to final bnid table------------------------
    drop table if exists `bnile-cdw-prod.up_stg_customer.final_bnid_table_dedup`;

    create table `bnile-cdw-prod.up_stg_customer.final_bnid_table_dedup`
    as
    select 
    ifnull(map.primary_bnid,f.bnid) as primary_bnid,
    f.*      
    from 
    `bnile-cdw-prod.up_stg_customer.final_bnid_table` f
    left join
    (
    SELECT
    distinct primary_bnid,duplicate_bnid
    FROM
    `bnile-cdw-prod.up_stg_dedup.bnid_dedup_map`
    )map
    on f.bnid=map.duplicate_bnid;*/




    --------------------JOIN back to final bnid table----------------------------
    drop table if exists `bnile-cdw-prod.up_stg_customer.final_bnid_table_dedup`;

    create table `bnile-cdw-prod.up_stg_customer.final_bnid_table_dedup`
    as
    select 
    ifnull(map.primary_bnid,f.bnid) as primary_bnid,
    f.*      
    from 
    `bnile-cdw-prod.up_stg_customer.final_bnid_table` f
    left join
    (
    SELECT
    distinct primary_bnid,duplicate_bnid
    FROM
    `bnile-cdw-prod.up_stg_dedup.bnid_dedup_fin`
    )map
    on f.bnid=map.duplicate_bnid;

    --------------------up_email_address_dim_d------------------------------------

    create or replace table `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d`
    as
    select
    f.up_email_address_key,
    f.email_address,
    ifnull(map.primary_bnid,f.bnid) as bnid,
    f.bnid as original_bnid
    from
    `bnile-cdw-prod.up_stg_customer.up_email_address_dim` f
    left join
    (
    SELECT
    distinct primary_bnid,duplicate_bnid
    FROM
    `bnile-cdw-prod.up_stg_dedup.bnid_dedup_fin`
    )map
    on f.bnid=map.duplicate_bnid;



    --creating up_customer_attributes table 

    create or replace table  `bnile-cdw-prod.o_customer.up_customer_attributes`  as
    SELECT
    primary_bnid AS bnid,
    m.up_email_address_key,
    f.email_address,
    --up_email_address_key,
    first_name,
    last_name,
    gender,
    derived_gender,
    phone_number,
    country_code,
    extension,
    phone,
    mobile,
    last_4_digits,
    birthday,
    age,
    bill_to_street,
    bill_to_city,
    bill_to_state,
    bill_to_country_name,
    bill_to_country_code,
    bill_to_postal_code,
    case when 
    (bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') 
    or regexp_contains (bill_to_country_name,r"^[U]S.*" ) or bill_to_country_code in ('US','USA')) 
    and REGEXP_contains (f.bill_to_postal_code,r"^\d{5}(?:[-\s]\d{4})?$")
    and safe_cast(substr(f.bill_to_postal_code,0,5) as numeric) = safe_cast (billing.zip_code as numeric) then 'Y' else 'N' 
    end billing_postal_code_valid_flag,
    ship_to_street,
    ship_to_city,
    ship_to_state,
    ship_to_country_name,
    ship_to_country_code,
    ship_to_postal_code,
    case when (ship_to_country_name in 
    ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')  
    or regexp_contains (ship_to_country_name,r"^[U]S.*" ) or ship_to_country_code in ('US','USA')) 
    and REGEXP_contains (f.ship_to_postal_code,r"^\d{5}(?:[-\s]\d{4})?$")
    and safe_cast(substr(f.ship_to_postal_code,0,5) as numeric) = safe_cast (shipping.zip_code as numeric) then 'Y' else 'N' 
    end shipping_postal_code_valid_flag,
    first_name_is_valid,
    email_is_valid,
    v_to_v_flag
    FROM
    `bnile-cdw-prod.up_stg_customer.final_bnid_table_dedup`f
    left join
    `bnile-cdw-prod.backup_tables.NBER_2016` shipping
    on safe_cast (shipping.zip_code as numeric) = safe_cast(substr(f.ship_to_postal_code,0,5) as numeric)
    left join
    `bnile-cdw-prod.backup_tables.NBER_2016` billing
    on safe_cast (billing.zip_code as numeric) = safe_cast(substr(f.bill_to_postal_code,0,5) as numeric)
    left join
    `bnile-cdw-prod.up_stg_customer.up_email_address_dim` m
    on f.email_address=m.email_address;

    --------------------------------------------------------------------------------
    -----------------Create deduped bnid_guid_distinct table------------------------
    --------------------------------------------------------------------------------

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_customer.bnid_guid_distinct` AS
    SELECT
    ifnull(map.primary_bnid,f.bnid) AS bnid,
    --f.bnid AS original_bnid,
    customer_guid,
    first_event_date,
    last_event_date,
    event_cnt,
    COUNT(DISTINCT bnid         ) OVER(PARTITION BY customer_guid) AS bnid_per_guid_cnt,
    COUNT(DISTINCT customer_guid) OVER(PARTITION BY bnid)          AS guid_per_bnid_cnt,
    ROW_NUMBER() OVER(PARTITION BY customer_guid ORDER BY event_cnt desc,last_event_date desc) AS rf_rank,
    FROM (
    SELECT
        bnid,
        customer_guid,
        MIN(event_date) AS first_event_date,
        MAX(event_date) AS last_event_date,
        COUNT(*) AS event_cnt
    FROM
        `bnile-cdw-prod.up_stg_customer.bnid_guid_mapping`
    GROUP BY
        bnid,
        customer_guid ) f
    LEFT JOIN (
    SELECT
        DISTINCT primary_bnid,
        duplicate_bnid
    FROM
        `bnile-cdw-prod.up_stg_dedup.bnid_dedup_fin` )map
    ON
    f.bnid=map.duplicate_bnid;


        ---------------------------------------------------------------------------------------------------------
        -----------------BN EMAIL GUID -  BNID Level added to Email GUID  table in Oracle------------------------
        ---------------------------------------------------------------------------------------------------------

    CREATE OR REPLACE TABLE `bnile-cdw-prod.up_stg_customer.bn_email_guid` as
    SELECT
    *
    FROM (
    SELECT
        bnid,
        eg.email_address,
        guid_key,
        guid
    FROM (
        SELECT
        EMAIL_ADDRESS,
        guid_key,
        guid
        FROM
        `bnile-cdw-prod.dssprod_o_warehouse.email_guid` ) eg
    JOIN (
        SELECT
        *
        FROM
        `bnile-cdw-prod.up_stg_customer.up_email_address_dim_d`) ead
    ON
        eg.email_address=ead.email_address );

        ---------------------------------------------------------------------------------------------------------
        -----------------BN GUID ALL-  1 to Many associations from BN GUID and Email GUID------------------------
        ---------------------------------------------------------------------------------------------------------

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_customer.bn_guid_all` AS
    --- Mappings from Email - GUID  - converted to BNID level
    SELECT bnid,
        guid,
            COUNT(DISTINCT bnid) OVER(PARTITION BY guid) AS bnid_per_guid_cnt,
            COUNT(DISTINCT guid) OVER(PARTITION BY bnid) AS guid_per_bnid_cnt,
    FROM
    (
    SELECT
    bnid,
    guid
    FROM
    `bnile-cdw-prod.up_stg_customer.bn_email_guid`
    UNION DISTINCT
    --- Mappings from BNID - GUID mapping
    (
    SELECT
        bnid,
        customer_guid AS guid
    FROM
        `bnile-cdw-prod.up_stg_customer.bnid_guid_distinct`
    WHERE
        rf_rank=1 )
    );	


    -----   Filter views by Offer, SKU--------------------------

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_customer.click_fact_sku_views` AS
    (SELECT
    *
    FROM (
    SELECT
        cf.guid_key,
        cf.date_key,
        cf.date_hour_key,
        cf.time_key,
        cf.uri_key,
        cf.querystring_key,
        cf.product_set_key,
        cf.currency_key,
        --  cf.offer_id AS cf_offer_id,
        SAFE_CAST(REGEXP_REPLACE(cf.offer_id, "[.][0-9]+$","") AS NUMERIC) AS offer_id,
        SAFE_CAST(REGEXP_EXTRACT(sku1, r'\d+') AS NUMERIC) AS sku_check,
        CASE
        WHEN SAFE_CAST(REGEXP_REPLACE(cf.offer_id, "[.][0-9]+$","") AS NUMERIC) = SAFE_CAST(REGEXP_EXTRACT(sku1, r'\d+') AS NUMERIC) THEN NULL
        ELSE
        SAFE_CAST(REGEXP_REPLACE(cf.offer_id, "[.][0-9]+$","") AS NUMERIC)
    END
        AS clean_offer_id,
        SKU1,
        SKU2,
        SKU3,
        SKU4,
        SKU5,
    FROM
        `bnile-cdw-prod.dssprod_o_warehouse.click_fact` cf

    WHERE
        request_type = 'page'
        AND date_key>=20200101 )
    WHERE
    (offer_id IS NOT NULL
        OR SKU1 IS NOT NULL)
    ) 
    ;
    -------BNID, product_set, email_cust_key, date, offer_id, sku_id, product_key,price, currency,#views----------------


    CREATE OR REPLACE TABLE `bnile-cdw-prod.up_stg_customer.bn_daily_product_views` AS
    SELECT
    bgm.bnid,
    date_key,
    psd.product_set,
    cd.currency,
    clean_offer_id AS offer_id,
    SKU1,
    SKU2,
    SKU3,
    SKU4,
    SKU5,
    COUNT(DISTINCT uri_key) AS view_cnt
    FROM
    `bnile-cdw-prod.up_stg_customer.click_fact_sku_views` cf
    INNER JOIN (

    SELECT
        guid_key,
        guid
    FROM
        `bnile-cdw-prod.up_tgt_ora_tables.guid_dim_cf` -- Change to final GUID_DIM_CF
    UNION ALL
    SELECT
        guid_key,
        guid
    FROM
        `bnile-cdw-prod.dssprod_o_warehouse.email_guid`) gd
    ON
    cf.guid_key=gd.guid_key
    INNER JOIN
    `bnile-cdw-prod.up_stg_customer.bn_guid_all` bgm
    ON
    gd.guid=bgm.guid
    LEFT JOIN
    `bnile-cdw-prod.up_tgt_ora_tables.product_set_dim` psd
    ON
    cf.product_set_key = psd.product_set_key
    LEFT JOIN
    `bnile-cdw-prod.up_tgt_ora_tables.currency_dim` cd
    ON
    cf.currency_key = cd.currency_key
    GROUP BY
    bnid,
    date_key,
    product_set,
    currency,
    clean_offer_id,
    SKU1,
    SKU2,
    SKU3,
    SKU4,
    SKU5
    ;'''
    return bql
