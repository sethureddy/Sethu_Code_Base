def up_customer_product():
    bql="""Create or replace table `bnile-cdw-prod.o_customer.up_customer_product` as   
    with ecp as 
    (select
    email_address
    ,bnid
    ,up_email_address_key
    ,email_address_test_key
    ,bn_test_key
    ,email_contactable_flag
    ,primary_site
    ,primary_hostname
    ,primary_language_code
    ,country_code
    ,phone
    ,primary_currency_code
    ,region
    ,queens_english_flag
    ,primary_dma_zip
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_2_webroom_key
    ,nearest_store_3_webroom_key
    ,cust_dna
    ,subscribe_flag
    ,email_open_last_dt
    ,last_subscribe_date
    ,browse_last_dt
    ,never_send_flag
    ,fraud_flag
    ,bad_address_flag
    ,undeliverable_date
    ,sweepstakes_entry_last_dt
    ,first_promo_subscribe_date
    ,pending_order_flag
    ,order_cnt
    ,order_last_dt
    ,birth_date
    ,wedding_date
    ,pj_lc_flag
    ,e_sc_most_viewed_offer_id_1
    ,e_lc_most_viewed_setting_OID_1
    ,e_lc_most_viewed_diamond_sku_1
    ,customer_segment_exact
    ,create_date_utc
    ,last_update_date_utc
    FROM o_customer.email_customer_profile),

    --  pof as 
    -- (SELECT
    -- distinct
    -- order_date_key, basket_id, original_basket_id, order_number,ead.email_address,
    -- order_header_id, order_line_id, order_parent_line_id, pof.product_key,
    -- pd.sku, pof.parent_product_key, pd2.sku AS parent_sku,
    -- pof.offer_key, od.name AS offer_name, od.description AS offer_desc
    -- FROM
    -- dssprod_o_warehouse.product_order_fact pof
    -- LEFT JOIN dssprod_o_warehouse.offer_dim od
    -- ON pof.offer_key = od.offer_Key
    -- LEFT JOIN dssprod_o_warehouse.product_dim pd
    -- ON pof.product_key = pd.product_key
    -- LEFT JOIN dssprod_o_warehouse.product_dim pd2
    -- ON pof.parent_product_key = pd2.product_key
    -- left join dssprod_o_warehouse.email_address_dim ead
    -- on ead.email_address_key=pof.email_address_key
    -- where pof.email_address_key is not null),


     basket as

    (
    SELECT
    distinct
    EMAIL_ADDRESS,
    B.BASKET_ID,
    BASKET_STATUS,
    BASKET_ITEM_ID,
    OFFER_ID,
    BASKET_STATUS_DATE,
    BASKET_START_DATE,
    BASKET_COMPLETE_DATE,
    CUSTOMER_SUBMIT_DATE,
    order_type
    FROM `bnile-cdw-prod.nileprod_o_warehouse.basket` B
    LEFT JOIN
    `bnile-cdw-prod.nileprod_o_warehouse.basket_item` BI
     on B.BASKET_ID=BI.BASKET_ID
     where email_address is not null
    )

    select
    distinct
    bnid
    ,ecp.email_address
    ,basket.BASKET_ID
    ,basket.BASKET_STATUS
    ,basket.BASKET_ITEM_ID
    ,basket.OFFER_ID
    ,basket.BASKET_STATUS_DATE
    ,basket.BASKET_START_DATE
    ,basket.BASKET_COMPLETE_DATE
    ,basket.CUSTOMER_SUBMIT_DATE
    ,order_type
    --,order_date_key
    ,up_email_address_key
    ,email_address_test_key
    ,bn_test_key
    ,email_contactable_flag
    ,primary_site
    ,primary_hostname
    ,primary_language_code
    ,country_code
    ,phone
    ,primary_currency_code
    ,region
    ,queens_english_flag
    ,primary_dma_zip
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_2_webroom_key
    ,nearest_store_3_webroom_key
    ,cust_dna
    ,subscribe_flag
    ,email_open_last_dt
    ,last_subscribe_date
    ,browse_last_dt
    ,never_send_flag
    ,fraud_flag
    ,bad_address_flag
    ,undeliverable_date
    ,sweepstakes_entry_last_dt
    ,first_promo_subscribe_date
    ,pending_order_flag
    ,order_cnt
    ,order_last_dt
    ,birth_date
    ,wedding_date
    ,pj_lc_flag
    ,e_sc_most_viewed_offer_id_1
    ,e_lc_most_viewed_setting_OID_1
    ,e_lc_most_viewed_diamond_sku_1
    ,customer_segment_exact
    ,create_date_utc
    ,last_update_date_utc

    from ecp

    left join basket

    on trim(lower(ecp.email_address))=trim(lower(basket.email_address))
    ;
    """

    return bql
