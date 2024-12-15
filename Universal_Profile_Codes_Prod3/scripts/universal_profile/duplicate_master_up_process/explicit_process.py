def explicit_process():
    bql = '''
    --- Query for stg_pii_attributes ----


    CREATE OR REPLACE TABLE `bnile-cdw-prod.dag_migration_test.stg_pii_attributes` as
     (
	select distinct
    b.up_email_address_key ,b.bnid
    ,aa.email_address
    ,customer_account_id
    ,account_create_date 
    ,account_create_source
    , last_account_login_date
    ,wish_list_flag
    ,account_holder_flag
    ,birth_date
    ,birth_date_source
    ,birth_date_timestamp
    ,wedding_date
    ,wedding_date_source
    ,wedding_date_timestamp
    ,anniversary_month
    ,anniversary_month_source
    ,anniversary_month_timestamp
    ,primary_dma_zip
    ,primary_dma_state
    ,primary_dma_name
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_1_distance_miles
    ,nearest_store_2_webroom_key
    ,nearest_store_2_distance_miles
    ,nearest_store_3_webroom_key
    ,nearest_store_3_distance_miles
    ,primary_site
    ,primary_hostname
    ,subscribed_hostname
    ,subscribed_site
    ,primary_currency_code
    ,primary_language_code
    ,email_address_test_key
    ,bn_test_key
    ,region
    ,marital_status
    ,marital_status_source      
    ,marital_status_timestamp
    ,proposal_date

    from
    (
    select distinct 
    email_address,
    customer_account_id
    ,account_create_date 
    ,account_create_source
    ,last_account_login_date
    ,wish_list_flag
    ,account_holder_flag
    ,birth_date
    ,birth_date_source
    ,birth_date_timestamp
    ,wedding_date
    ,wedding_date_source
    ,wedding_date_timestamp
    ,anniversary_month
    ,anniversary_month_source
    ,anniversary_month_timestamp
    , primary_dma_zip
    ,primary_dma_state
    ,primary_dma_name
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_1_distance_miles
    ,nearest_store_2_webroom_key
    ,nearest_store_2_distance_miles
    ,nearest_store_3_webroom_key
    ,nearest_store_3_distance_miles
    ,primary_site
    ,primary_hostname   
    ,primary_currency_code
    ,primary_language_code
    ,email_address_test_key
    ,bn_test_key
    ,region
    ,marital_status
    ,marital_status_source      
    ,marital_status_timestamp
    ,proposal_date
    ,subscribed_hostname
    ,subscribed_site
    from
    (SELECT 
     email_address 
    ,customer_account_id
    ,account_create_date
    ,account_create_source
    , last_account_login_date
    ,wish_list_flag
    ,account_holder_flag
    , birth_date
    ,birth_date_source
    ,birth_date_timestamp
    ,dz.zip AS primary_dma_zip
    ,dz.st AS primary_dma_state
    ,dz.dma AS primary_dma_name
    ,dz.primary_dma_source
    ,dz.nearest_store_1_webroom_key
    ,dz.nearest_store_1_distance_miles
    ,dz.nearest_store_2_webroom_key
    ,dz.nearest_store_2_distance_miles
    ,dz.nearest_store_3_webroom_key
    ,dz.nearest_store_3_distance_miles
    ,primary_site
    ,primary_hostname   
    ,primary_currency_code
    ,primary_language_code
    ,marital_status
    ,wedding_date
    ,wedding_date_source
    ,wedding_date_timestamp
    ,anniversary_month
    ,anniversary_month_source
    ,anniversary_month_timestamp
    ,email_address_test_key
    ,bn_test_key
    ,region
    ,marital_status_source      
    ,marital_status_timestamp
    ,proposal_date
    ,subscribed_hostname
    ,subscribed_site
    ,ROW_NUMBER()OVER(PARTITION BY email_address order by coalesce(last_account_login_date,account_create_date) desc) AS row_num 
    FROM 
    (select * from `bnile-cdw-prod.dag_migration_test.up_customer_attributes`) h
    full outer join

    (select * from
    (select bn.email_address as email_address,bn.bill_to_postal_code ,bn.ship_to_postal_code,
      c.customer_account_key customer_account_id,
      c.created_by_username account_create_source,
      cast (c.create_date as date) account_create_date,
      row_number () over(partition by bn.email_address order by  c.create_date desc) rn
    FROM
      `bnile-cdw-prod.dag_migration_test.up_customer_attributes` bn
    full outer join
      `bnile-cdw-prod.dssprod_o_warehouse.customer_account_email_address` cea
    ON
      bn.email_address = cea.email_address
    full outer join
      `bnile-cdw-prod.dssprod_o_warehouse.customer_account_dim` c
    ON
      c.customer_account_key = cea.customer_account_key
      where cea.email_address is not null and 
      c.customer_account_key is not null and 
      cea.customer_account_key is not null and bn.email_address is not null
      and c.create_date is not null )
      where rn =1 )a

    using(email_address)

    full outer join

    (select * from
    (select bn.email_address as email_address,
      cast (c.most_recent_login_date as date) last_account_login_date,
      row_number () over(partition by bn.email_address order by  c.most_recent_login_date desc) rn
    FROM
      `bnile-cdw-prod.dag_migration_test.up_customer_attributes` bn
    full outer join
      `bnile-cdw-prod.dssprod_o_warehouse.customer_account_email_address` cea
    ON
      bn.email_address = cea.email_address
    full outer join
      `bnile-cdw-prod.dssprod_o_warehouse.customer_account_dim` c
    ON
      c.customer_account_key = cea.customer_account_key
      where cea.email_address is not null and 
      c.customer_account_key is not null and 
      cea.customer_account_key is not null and bn.email_address is not null)
      where rn =1 )j

    using(email_address)


    FULL OUTER JOIN


    (select email_address, wish_list_flag, account_holder_flag
    from
    (select email_address, wish_list_flag, account_holder_flag
    ,ROW_NUMBER() OVER(PARTITION BY email_address order by coalesce(change_date,create_date) desc,
    wish_list_flag desc, account_holder_flag desc) AS row_num
    from
    (select bn.*,
      CASE
        WHEN c.favorite_item_basket_id IS NOT NULL THEN 'Y'
      ELSE
      'N'
    END
      wish_list_flag,
      CASE
        WHEN REPLACE(c.login_name,'***',"") = cea.email_address THEN 'Y'
      ELSE
      'N'
    END
      account_holder_flag,
     c.change_date,
     c.create_date
    FROM
      `bnile-cdw-prod.dag_migration_test.up_customer_attributes` bn
    FULL outer join
      `bnile-cdw-prod.dssprod_o_warehouse.customer_account_email_address` cea
    ON
      bn.email_address = cea.email_address
    FULL outer join
      `bnile-cdw-prod.dssprod_o_warehouse.customer_account_dim` c
    ON
      c.customer_account_key = cea.customer_account_key
      where cea.email_address is not null and 
      c.customer_account_key is not null and 
      cea.customer_account_key is not null and bn.email_address is not null)
    )
    where row_num =1 )b

    USING(EMAIL_ADDRESS)


    -- Primary_site, primary_currency_code, Primary_language_code--

    full outer join

    (select EMAIL_ADDRESS, PRIMARY_SITE, PRIMARY_CURRENCY_CODE, PRIMARY_LANGUAGE_CODE
    from 
    (SELECT  email_address, PRIMARY_SITE,PRIMARY_CURRENCY_CODE, PRIMARY_LANGUAGE_CODE
    , row_number () over (partition by email_address order by last_update_date desc) rank
    FROM `bnile-cdw-prod.dssprod_o_warehouse.email_address` 
    where EMAIL_ADDRESS is not null and ( PRIMARY_SITE is not null or PRIMARY_CURRENCY_CODE is not null or PRIMARY_LANGUAGE_CODE is not null)    )
    where rank =1
    )d

    using (email_address)



    -----primary_hostname-------
    full outer join

    (select email_address, hostname  as primary_hostname from
    (select  distinct email_address,
    CONCAT('www.bluenile.com',
    case when (eas.PRIMARY_SHIP_TO_COUNTRY_CODE = 'US' and COALESCE(eas.PRIMARY_LANGUAGE_CODE , 'en-us') = 'en-us') then ''
                                     when (eas.PRIMARY_SHIP_TO_COUNTRY_CODE is null and eas.primary_hostname = 'www.bluenile.com') then ''
                                     when (eas.PRIMARY_SHIP_TO_COUNTRY_CODE is null and eas.primary_hostname = 'www.bluenile.ca') then '/ca'
                                     when (eas.PRIMARY_SHIP_TO_COUNTRY_CODE is null and eas.primary_hostname = 'www.bluenile.co.uk') then '/uk'
                                     when (eas.PRIMARY_SHIP_TO_COUNTRY_CODE is null ) then '/'||concat(substr(eas.primary_hostname,0,2),'')
                                     when (coalesce(eas.PRIMARY_LANGUAGE_CODE, 'en-us') = stc.default_language) then '/'||concat(c.display_code,'')
                                     else concat('/',concat(concat(concat(c.display_code,'/'),l.display_code),''))
    end) as HOSTNAME,
    row_number() over (partition by email_address order by last_update_date desc) rank
    FROM `bnile-cdw-prod.dssprod_o_warehouse.email_address` eas
    JOIN `bnile-cdw-prod.nileprod_o_warehouse.country` c ON coalesce(eas.PRIMARY_SHIP_TO_COUNTRY_CODE , 'US') = c.country_code
    JOIN `bnile-cdw-prod.up_tgt_ora_tables.ship_to_country` stc ON c.country_code = stc.country_code
    JOIN `bnile-cdw-prod.up_tgt_ora_tables.language_code` l ON coalesce(eas.PRIMARY_LANGUAGE_CODE , 'en-us') = l.language_code
    where primary_hostname is not null and eas.email_address is not null
    )
    where rank =1)

    using (email_address)

    full outer join

    (select email_address,region from
    (select eas.email_address email_address,
    case
         when eas.site = 'BNUK' and e.display_code = 'uk' then 'UK'
         when eas.site = 'BNUK' and e.display_code <> 'uk' then 'EU'
         when eas.site = 'BN' and e.display_code = 'us' then 'US'
         when eas.site = 'BN' and e.display_code <> 'us' then 'ROW'
         when eas.site = 'BNCA' then 'CA'
         when eas.site = 'BNCN' then 'CN'
         else 'US'
    end as region,
    row_number () over (partition by eas.email_address order by last_subscribe_date desc,last_unsubscribe_date desc,LAST_UPDATE_DATE desc) rn
    from  `bnile-cdw-prod.dssprod_o_warehouse.email_address_subscription` eas
    left outer join
    (select country_code,display_code from `bnile-cdw-prod.nileprod_o_warehouse.country`) e
    on ifnull(eas.ship_to_country_code, 'US') = e.country_code
    where eas.email_address is not null)
    where rn =1)f

    using (email_address)

    full outer join

    (select email_address, EMAIL_ADDRESS_TEST_KEY
    from
    (select  email_address,EMAIL_ADDRESS_TEST_KEY,
    row_number () over(partition by email_address order by LAST_UPDATE_DATE desc) rn 
    from `bnile-cdw-prod.dssprod_o_warehouse.email_address` where  EMAIL_ADDRESS is not null and EMAIL_ADDRESS_TEST_KEY is not null)
    where rn=1
    )g

    using(email_address)

    --- BN_test_key addition

    full outer join

    (select bnid, bn_test_key
    from `bnile-cdw-prod.dag_migration_test.master_bn_test_key`
    )btk

    using(bnid)

    full outer join

    ---------------------wedding date logic modified by sethu 06/02/2021------------------

    (SELECT
  DISTINCT trim(lower(email_address)) email_address,
  FIRST_VALUE(wedding_date) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS wedding_date,
  FIRST_VALUE(event_type) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS wedding_date_source,
  FIRST_VALUE(wedding_date_timestamp) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS wedding_date_timestamp
	FROM ((
    SELECT
      DISTINCT ifnull(TRIM(wedding_date_source),'email_address_table') AS event_type,
      1 AS rank,
      email_address,
      wedding_date,
      DATE(wedding_date_timestamp) AS wedding_date_timestamp
    FROM (
      SELECT
        ea.email_address AS email_address,
        case when EXTRACT(year from cast(wedding_date as date))<1773 
     then cast('1800-01-01' as date)
     else cast(wedding_date as date)
    end   
     wedding_date,
        wedding_date_source,
        DATE(wedding_date_timestamp) AS wedding_date_timestamp,
        row_number () OVER(PARTITION BY ea.email_address ORDER BY ea.LAST_UPDATE_DATE DESC) rn
      FROM
        `bnile-cdw-prod.dssprod_o_warehouse.email_address` ea
      WHERE
        ea.email_address IS NOT NULL
        AND wedding_date IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'customer_event' AS event_type,
      2 AS rank,
      email_address,
      wedding_date,
      DATE(wedding_date_timestamp) AS MARITAL_STATUS_TIMESTAMP,
    FROM (
      SELECT
        tpd.email_address AS email_address,
        tpd.wedding_date,
        event_date AS wedding_date_timestamp,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_customer_event_fact` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND wedding_date IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'email_survey' AS event_type,
      3 AS rank,
      email_address,
      wedding_date,
      DATE(wedding_date_timestamp) AS wedding_date_timestamp,
    FROM (
      SELECT
        tpd.email_address AS email_address,
        tpd.wedding_date ,
        event_date AS wedding_date_timestamp,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_email_survey_summary` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND wedding_date IS NOT NULL)
    WHERE
      rn=1))
                  ) p

    using(email_address)


    full outer join

    -------------- marital_status  modified by sethu 06/02/2021-------------------------------------------    
    (SELECT
  DISTINCT trim(lower(email_address)) email_address ,
  FIRST_VALUE(marital_status) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS marital_status,
  FIRST_VALUE(event_type) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS MARITAL_STATUS_SOURCE,
  FIRST_VALUE(MARITAL_STATUS_TIMESTAMP) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS MARITAL_STATUS_TIMESTAMP
	FROM ((
    SELECT
      DISTINCT TRIM(MARITAL_STATUS_SOURCE) AS event_type,
      1 AS rank,
      email_address,
      MARITAL_STATUS,
      DATE(MARITAL_STATUS_TIMESTAMP) AS MARITAL_STATUS_TIMESTAMP
    FROM (
      SELECT
        trim(ea.email_address) AS email_address,
        MARITAL_STATUS,
        MARITAL_STATUS_SOURCE,
        DATE(MARITAL_STATUS_TIMESTAMP) AS MARITAL_STATUS_TIMESTAMP,
        row_number () OVER(PARTITION BY ea.email_address ORDER BY ea.LAST_UPDATE_DATE DESC) rn
      FROM
        `bnile-cdw-prod.dssprod_o_warehouse.email_address` ea
      WHERE
        ea.email_address IS NOT NULL
        AND MARITAL_STATUS IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'backbone' AS event_type,
      3 AS rank,
      email_address,
      MARITAL_STATUS,
      DATE(MARITAL_STATUS_TIMESTAMP) AS MARITAL_STATUS_TIMESTAMP,
    FROM (
      SELECT
        trim(tpd.email_address) AS email_address,
        MARITAL_STATUS,
        event_date AS MARITAL_STATUS_TIMESTAMP,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_backbone` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND MARITAL_STATUS IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'customer_event' AS event_type,
      2 AS rank,
      email_address,
      MARITAL_STATUS,
      DATE(MARITAL_STATUS_TIMESTAMP) AS MARITAL_STATUS_TIMESTAMP,
    FROM (
      SELECT
        tpd.email_address AS email_address,
        MARITAL_STATUS,
        event_date AS MARITAL_STATUS_TIMESTAMP,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_customer_event_fact` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND MARITAL_STATUS IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'email_survey' AS event_type,
      4 AS rank,
      email_address,
      MARITAL_STATUS,
      DATE(MARITAL_STATUS_TIMESTAMP) AS MARITAL_STATUS_TIMESTAMP,
    FROM (
      SELECT
        tpd.email_address AS email_address,
        MARITAL_STATUS,
        event_date AS MARITAL_STATUS_TIMESTAMP,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_email_survey_summary` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND MARITAL_STATUS IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'review_pii' AS event_type,
      5 AS rank,
      email_address,
      MARITAL_STATUS,
      DATE(MARITAL_STATUS_TIMESTAMP) AS MARITAL_STATUS_TIMESTAMP,
    FROM (
      SELECT
        tpd.reviewer_email_address AS email_address,
        MARITAL_STATUS,
        event_date AS MARITAL_STATUS_TIMESTAMP,
        row_number () OVER(PARTITION BY tpd.reviewer_email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_review_pii` tpd
      WHERE
        tpd.reviewer_email_address IS NOT NULL
        AND MARITAL_STATUS IS NOT NULL)
    WHERE
      rn=1) ))

    using(email_address)

	full outer join

	----- anniversary_month logic start (by Sethu)------------------------
	(
	SELECT
  DISTINCT TRIM(LOWER(email_address)) email_address,
  FIRST_VALUE(anniversary_month) OVER(PARTITION BY TRIM(email_address)
  ORDER BY
    rank ) AS anniversary_month,
  FIRST_VALUE(anniversary_month_source) OVER(PARTITION BY TRIM(email_address)
  ORDER BY
    rank ) AS anniversary_month_source,
  FIRST_VALUE(anniversary_month_timestamp) OVER(PARTITION BY TRIM(email_address)
  ORDER BY
    rank ) AS anniversary_month_timestamp
FROM (
  SELECT
    event_type AS anniversary_month_source,
    1 AS rank,
    email_address,
    CASE
      WHEN month = 1 THEN 'January'
      WHEN month = 2 THEN 'February'
      WHEN month = 3 THEN 'March'
      WHEN month = 4 THEN 'April'
      WHEN month = 5 THEN 'May'
      WHEN month = 6 THEN 'June'
      WHEN month = 7 THEN 'July'
      WHEN month = 8 THEN 'August'
      WHEN month = 9 THEN 'September'
      WHEN month = 10 THEN 'October'
      WHEN month = 11 THEN 'November'
      WHEN month = 12 THEN 'December'
    ELSE
    CAST (month AS string)
  END
    anniversary_month,
    anniversary_month_timestamp
  FROM (
    SELECT
      email_address,
      'email_address_table' AS event_type,
      WEDDING_ANNIVERSARY,
      length (TRIM(WEDDING_ANNIVERSARY)) lengths,
      CASE
        WHEN REGEXP_CONTAINS(TRIM(WEDDING_ANNIVERSARY), r'\d{1,2}-') THEN safe_cast (SPLIT(WEDDING_ANNIVERSARY,'-')[ OFFSET (0)] AS int64)
        WHEN REGEXP_CONTAINS(TRIM(WEDDING_ANNIVERSARY), r'\d{1,2}-\d{1,2}') THEN safe_cast (SPLIT(WEDDING_ANNIVERSARY,'-')[
      OFFSET
        (0)] AS int64)
        WHEN REGEXP_CONTAINS(TRIM(WEDDING_ANNIVERSARY), r'-\d{1,2}') THEN safe_cast (SPLIT(WEDDING_ANNIVERSARY,'-')[ OFFSET (1)] AS int64)
    END
      month,
      DATE(LAST_UPDATE_DATE) AS anniversary_month_timestamp,
      row_number () OVER(PARTITION BY ea.email_address ORDER BY ea.LAST_UPDATE_DATE DESC) rn
    FROM
      `bnile-cdw-prod.dssprod_o_warehouse.email_address` ea
    WHERE
      WEDDING_ANNIVERSARY IS NOT NULL )
  WHERE
    rn=1
  UNION ALL (
    SELECT
      DISTINCT 'review_pii' AS event_type,
      2 AS rank,
      email_address,
      anniversary_month,
      DATE(anniversary_month_timestamp) AS anniversary_month_timestamp,
    FROM (
      SELECT
        tpd.reviewer_email_address AS email_address,
        ifnull(tpd.anniversary_month,
          tpd.wedding_month ) AS anniversary_month,
        event_date AS anniversary_month_timestamp,
        row_number () OVER(PARTITION BY tpd.reviewer_email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_review_pii` tpd
      WHERE
        tpd.reviewer_email_address IS NOT NULL
        AND ifnull(tpd.anniversary_month,
          tpd.wedding_month ) IS NOT NULL )
    WHERE
      rn=1)
  UNION ALL
  SELECT
    anniversary_month_source,
    3 AS rank,
    email_address,
    CASE
      WHEN month = 1 THEN 'January'
      WHEN month = 2 THEN 'February'
      WHEN month = 3 THEN 'March'
      WHEN month = 4 THEN 'April'
      WHEN month = 5 THEN 'May'
      WHEN month = 6 THEN 'June'
      WHEN month = 7 THEN 'July'
      WHEN month = 8 THEN 'August'
      WHEN month = 9 THEN 'September'
      WHEN month = 10 THEN 'October'
      WHEN month = 11 THEN 'November'
      WHEN month = 12 THEN 'December'
    ELSE
    CAST(month AS string)
  END
    anniversary_month,
    anniversary_month_timestamp
  FROM (
    SELECT
      email_address,
      'customer_event' AS anniversary_month_source,
      WEDDING_ANNIVERSARY,
      length (TRIM(WEDDING_ANNIVERSARY)) lengths,
      CASE
        WHEN REGEXP_CONTAINS(TRIM(WEDDING_ANNIVERSARY), r'\d{1,2}-') THEN safe_cast (SPLIT(WEDDING_ANNIVERSARY,'-')[ OFFSET (0)] AS int64)
        WHEN REGEXP_CONTAINS(TRIM(WEDDING_ANNIVERSARY), r'\d{1,2}-\d{1,2}') THEN safe_cast (SPLIT(WEDDING_ANNIVERSARY,'-')[
      OFFSET
        (0)] AS int64)
        WHEN REGEXP_CONTAINS(TRIM(WEDDING_ANNIVERSARY), r'-\d{1,2}') THEN safe_cast (SPLIT(WEDDING_ANNIVERSARY,'-')[ OFFSET (1)] AS int64)
    END
      month,
      event_date AS anniversary_month_timestamp,
      row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
    FROM
      `bnile-cdw-prod.up_stg_customer.stg_customer_event_fact` tpd
    WHERE
      WEDDING_ANNIVERSARY IS NOT NULL )
  WHERE
    rn=1)


	)
	----- anniversary_month logic end (by sethu 06/02/2021)----------------
	using(email_address)


    full outer join

     (
      SELECT
      trim(lower(contact_email_address)) as email_address,
        MAX(PROPOSAL_DATE) proposal_date,

      FROM
        `bnile-cdw-prod.dssprod_o_customer.freshdesk_contact`
      WHERE
        contact_email_address IS NOT NULL
        AND contact_email_address NOT LIKE '-'
        AND PROPOSAL_DATE IS NOT NULL
      GROUP BY
        trim(lower(contact_email_address))
        )proposal
    using(email_address)

        ------ subscribed hostname, subscribed_site ----
    full outer join

    (select email_address,  subscribed_hostname, site as subscribed_site,
    from
    (select  distinct email_address,site,
    CONCAT('www.bluenile.com',
    case when (eas.ship_to_country_code = 'US' and COALESCE(eas.language_code , 'en-us') = 'en-us') then ''
                                     when (eas.ship_to_country_code is null and eas.hostname = 'www.bluenile.com') then ''
                                     when (eas.ship_to_country_code is null and eas.hostname = 'www.bluenile.ca') then '/ca'
                                     when (eas.ship_to_country_code is null and eas.hostname = 'www.bluenile.co.uk') then '/uk'
                                     when (eas.ship_to_country_code is null ) then '/'||concat(substr(eas.hostname,0,2),'')
                                    when (coalesce(eas.language_code, 'en-us') = stc.default_language ) then '/'||concat(c.display_code,'')
                                     else concat('/',concat(concat(concat(c.display_code,'/'),l.display_code),''))
    end) as subscribed_hostname, length(hostname) length,hostname,
    row_number() over (partition by email_address order by LAST_SUBSCRIBE_DATE desc, eas.LAST_UNSUBSCRIBE_DATE desc, eas.LAST_UPDATE_DATE desc ) rank
    FROM `bnile-cdw-prod.dssprod_o_warehouse.email_address_subscription` eas
    JOIN `bnile-cdw-prod.nileprod_o_warehouse.country` c ON coalesce(eas.ship_to_country_code, 'US') = c.country_code
    JOIN `bnile-cdw-prod.up_tgt_ora_tables.ship_to_country` stc ON c.country_code = stc.country_code
    JOIN `bnile-cdw-prod.up_tgt_ora_tables.language_code` l ON coalesce(eas.language_code, 'en-us') = l.language_code
    where eas.SUBSCRIBE_FLAG  = 'Y' 
    )
    where rank =1)


    using(email_address)


    full outer join

    ---birth_date   modifed by sethu 06/02/2021----

    (SELECT
  DISTINCT trim(lower(email_address)) email_address,
  FIRST_VALUE(birth_date) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS birth_date,
  FIRST_VALUE(event_type) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS birth_date_source,
  FIRST_VALUE(birth_date_timestamp) OVER(PARTITION BY TRIM(lower(email_address))
  ORDER BY
    rank ) AS birth_date_timestamp
FROM ((
    SELECT
      DISTINCT ifnull(TRIM(birth_date_source),'email_address_table') AS event_type,
      1 AS rank,
      email_address,
      birth_date,
      DATE(birth_date_timestamp) AS birth_date_timestamp
    FROM (
      SELECT
        ea.email_address AS email_address,
        case when EXTRACT(year from cast(birth_date as date))<1773 
     then cast('1800-01-01' as date)
     else cast(birth_date as date)
    end   
     birth_date,
        birth_date_source,
        DATE(birth_date_timestamp) AS birth_date_timestamp,
        row_number () OVER(PARTITION BY ea.email_address ORDER BY ea.LAST_UPDATE_DATE DESC) rn
      FROM
        `bnile-cdw-prod.dssprod_o_warehouse.email_address` ea
      WHERE
        ea.email_address IS NOT NULL
        AND birth_date IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'customer_event' AS event_type,
      2 AS rank,
      email_address,
      birth_date,
      DATE(birth_date_timestamp) AS birth_date_TIMESTAMP,
    FROM (
      SELECT
        tpd.email_address AS email_address,
        tpd.birthday as birth_date,
        event_date AS birth_date_timestamp,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_customer_event_fact` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND birthday IS NOT NULL)
    WHERE
      rn=1)
  UNION ALL (
    SELECT
      DISTINCT 'email_survey' AS event_type,
      3 AS rank,
      email_address,
      birth_date,
      DATE(birth_date_timestamp) AS birth_date_timestamp,
    FROM (
      SELECT
        tpd.email_address AS email_address,
        tpd.birth_date ,
        event_date AS birth_date_timestamp,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_email_survey_summary` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND birth_date IS NOT NULL)
    WHERE
      rn=1)
      UNION ALL (
    SELECT
      DISTINCT 'backbone' AS event_type,
      4 AS rank,
      email_address,
      birth_date,
      DATE(birth_date_timestamp) AS birth_date_timestamp,
    FROM (
      SELECT
        tpd.email_address AS email_address,
        cast(tpd.birth_date as date) birth_date,
        event_date AS birth_date_timestamp,
        row_number () OVER(PARTITION BY tpd.email_address ORDER BY tpd.event_date DESC) rn
      FROM
        `bnile-cdw-prod.up_stg_customer.stg_backbone` tpd
      WHERE
        tpd.email_address IS NOT NULL
        AND birth_date IS NOT NULL
        and safe_cast(age as int64)>18)
    WHERE
      rn=1)

      ))t

    using(email_address)



    full outer join
      (
      with raw_sources as 

     (select aa.EMAIL_ADDRESS,substr(aa.POSTAL_CODE,1,5) POSTAL_CODE,bb.ST,bb.DMA,bb.DMA_RANK,event_type,aa.event_date from 
     (select * from
        (
          (select email_address ,bill_to_postal_code,ship_to_postal_code,
                  coalesce(bill_to_postal_code ,
                  ship_to_postal_code ) postal_code, 
                  event_type, 
                  event_date  
           FROM `bnile-cdw-prod.dag_migration_test.bnid_pii_mapping_cleaned` )
           )  WHERE postal_code is not null and email_address is not null and POSTAL_CODE <> '-') aa
         INNER JOIN
             `bnile-cdw-prod.dssprod_o_warehouse.dma_zip` bb
         ON substr(aa.POSTAL_CODE,1,5) =bb.zip
     UNION DISTINCT
         (
         SELECT b.EMAIL_ADDRESS ,a.POSTAL_CODE,bb.ST,bb.DMA,bb.DMA_RANK, 
               'ip_dim' as event_type,
               cast(a.create_date as timestamp) as event_date,   
         FROM `bnile-cdw-prod.dssprod_o_warehouse.ip_address_dim` a
           INNER JOIN
         `bnile-cdw-prod.dssprod_o_warehouse.email_address_subscription` b
         ON a.IP_ADDRESS =b.IP_ADDRESS 
           INNER JOIN
         `bnile-cdw-prod.dssprod_o_warehouse.dma_zip` bb 
         ON a.POSTAL_CODE =bb.zip
           WHERE a.POSTAL_CODE <> '-' and postal_code IS NOT NULL AND email_address IS NOT NULL
           )
        ), 


     orderby as

     (SELECT EMAIL_ADDRESS, POSTAL_CODE, ST, DMA, DMA_RANK, event_type, event_date, 
         case when event_type = 'orders' then 3 
              when event_type = 'site_event' then 2 
              when event_type = 'ip_dim' then 1
         end order_by

        FROM raw_sources 
      where DMA  is not null
      ),
     --------------------------------------------------------------------------------------
     ----Store Proximity to Customer - Added 24-May-2021 - Sunil Thomas
     --------------------------------------------------------------------------------------
     dma_zip as
     (
      select EMAIL_ADDRESS,POSTAL_CODE as zip,ST,DMA,event_type as primary_dma_source
       from
        ( select *, row_number ()  over (partition by email_address order by order_by desc, event_date desc, dma_rank asc) row_number
          from orderby)
       where row_number =1
       ) 
      SELECT 
          EMAIL_ADDRESS,
          zip,
          ST,
          DMA,
          primary_dma_source,
          MAX(IF(distance_rank = 1, webroom_key, NULL))    AS nearest_store_1_webroom_key,
          MAX(IF(distance_rank = 1, distance_miles, NULL)) AS nearest_store_1_distance_miles,
          MAX(IF(distance_rank = 2, webroom_key, NULL))    AS nearest_store_2_webroom_key,
          MAX(IF(distance_rank = 2, distance_miles, NULL)) AS nearest_store_2_distance_miles,
          MAX(IF(distance_rank = 3, webroom_key, NULL))    AS nearest_store_3_webroom_key,
          MAX(IF(distance_rank = 3, distance_miles, NULL)) AS nearest_store_3_distance_miles,
      from dma_zip cz
      LEFT JOIN
           `bnile-cdw-prod.dssprod_o_warehouse.postal_code_showroom_dist_lkup` dl

        ON cz.zip=dl.postal_code_from
    GROUP by 1,2,3,4,5   
    ) dz

      using (email_address)   )

      where row_num = 1 and coalesce(trim(email_address),'9999') not in('-')
    and coalesce(trim(email_address),'@') like '%@%' and coalesce(trim(email_address),'.') like '%.%'
                and coalesce(trim(email_address),'9999') not like '%jminsure.com'
                and coalesce(trim(email_address),'9999') not like 'apayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like 'cntmpayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like '%test%bluenile%'
                and coalesce(trim(email_address),'9999') not like '%@test.com'
                and coalesce(trim(email_address),'9999') not in ('noemail@noemail.com') 
                and coalesce(trim(email_address),'9999') not like '%@removed.com'
                and email_address is not null 
            )aa
             left join 

    (select  up_email_address_key,bnid,email_address from 
    `bnile-cdw-prod.dag_migration_test.up_customer_attributes`  where email_address is not null) b
    on aa.email_address=b.email_address
    where bnid is not null
               );                  


    ---- Query for stg_order_attributes----



    CREATE OR REPLACE TABLE `bnile-cdw-prod.dag_migration_test.stg_order_attributes`  as
    (

    with email_activity_t1
      AS
    (select
       ead.email_address_key, trim(lower(ead.email_address)) email_address ,
                pof.site,
                hsd.hostname,
                prd.product_department_name as activity_name,
                case
                    when otd.order_category = 'R' and otd.order_type <> 'REPLACEMENT' and cast(otd.new_order_flag as Numeric) = 1 then 'ORDER'
                    when otd.order_category = 'RMA' and otd.order_type not in ('RETURN FOR MOUNTING') and cast(otd.return_for_work_flag as Numeric)= 0 then                     'RETURN'
                end activity_type,
                case
                    when otd.order_category = 'R' and otd.order_type <> 'REPLACEMENT' and cast(otd.new_order_flag as Numeric)= 1 
                      then ifnull(pof.customer_submit_date_key,pof.order_date_key)
                    --               
                    when otd.order_category = 'RMA' and otd.order_type not in ('RETURN FOR MOUNTING') and cast(otd.return_for_work_flag as Numeric)= 0 
                      then pof.order_date_key 

                end activity_date,
                pof.basket_id activity_count,
                cast(pof.order_amount_net as Numeric) activity_amount,
                1 all_flag,
                case when prd.product_department_name = 'Engagement' then 1 else 0 end engagement_flag, -- engagement items as defined by marketing
                case when prd.product_department_name like 'Jewelry%' then 1 else 0 end jewelry_flag, -- engagement items as defined by marketing

                case when prd.sku like 'LD%' and prd.diamond_brand_name = 'Signature' then 1 else 0 end signature_diamond_flag, -- signature diamond orders as defined by marketing
                case when prd.sku like 'LD%' and prd.diamond_brand_name = 'Astor' then 1 else 0 end astor_diamond_flag, -- added for Astor diamonds
                case when prd.sku like 'LD%' and prd.diamond_brand_name is not null then 1 else 0 end branded_diamond_flag, -- added for all branded diamonds i.e., signatue and astor for now
                case when prd.product_department_name = 'Engagement' and prd.product_category_name like '%Ring%' then 1 else 0 end engagement_ring_flag, -- engagement ring orders as defined by marketing
                case when prd.product_department_name like 'Jewelry%' and prd.product_category_name = 'Bands' then 1 else 0 end wedding_band_flag, -- wedding band orders as defined by marketing
                case when prd.product_category_name = 'Loose Diamonds' then 1 else 0 end loose_diamond_flag, -- loose diamond orders as defined by marketing
                case
                    when prd.product_class_name = 'Complete 3 Stone' then 1
                    when prd.product_class_name = 'Complete Solitaire' then 1
                    when prd.product_category_name = 'Diamond Jewelry' and prd.product_sub_class_name in ('Bracelets','Necklaces','Complete') then 1
                    else 0
                end diamond_jewelry_flag, -- diamond jewelry orders as defined by marketing
                case when prd.product_category_name = 'Silver' then 1 else 0 end silver_flag, -- silver orders as defined by marketing
                case when prd.product_category_name = 'Gold' and prd.product_description not like '%White Gold%' and prd.product_description not like '%WG%' then 1 else 0 end yellow_gold_flag, -- yellow gold orders as defined by marketing
                case when prd.product_category_name = 'Pearl' then 1 else 0 end pearl_flag, -- pearl orders as defined by marketing
                case when prd.product_category_name = 'Gemstones' then 1 else 0 end gemstone_flag, -- gemstone orders as defined by marketing
                case when prd.product_category_name = 'Watches' then 1 else 0 end watch_flag, -- watch orders as defined by marketing
                case when prd.product_sub_class_name = 'Charms' then 1 else 0 end charm_flag, -- charm orders as defined by marketing
                case when prd.product_class_name = 'BYO Ring' then 1 else 0 end byo_ring_flag, -- byo ring orders as defined by marketing
                case when prd.product_class_name = 'BYO 3 Stone' then 1 else 0 end byo_3_stone_ring_flag, -- byo 3 stone ring orders as defined by marketing
                case when prd.product_category_name = 'Bands' and prd.product_class_name = 'Diamond Band/Ring' and prd.product_sub_class_name <> 'Complete' then 1 else 0 end byo_5_stone_ring_flag, -- byo 5 stone ring orders as defined by marketing
                case when prd.product_category_name = 'Diamond Jewelry' and prd.product_class_name = 'Stud Earrings' and prd.product_sub_class_name <> 'Complete' then 1 else 0 end byo_earring_flag, -- byo earring orders as defined by marketing
                case when prd.product_category_name = 'Diamond Jewelry' and prd.product_class_name = 'SolitairePendant' and prd.product_sub_class_name <> 'Complete' then 1 else 0 end byo_pendant_flag, -- byo pendant orders as defined by marketing
                case when prd.product_category_name = 'Diamond Jewelry' and prd.product_class_name = 'Pendants' and prd.product_sub_class_name <> 'Complete' then 1 else 0 end byo_3_stone_pendant_flag, -- byo 3s pendant orders as defined by marketing
                prd.diamond_brand_name 
                from
                `bnile-cdw-prod.dssprod_o_warehouse.product_order_fact` pof,
                `bnile-cdw-prod.dssprod_o_warehouse.order_type_dim` otd,
                `bnile-cdw-prod.dssprod_o_warehouse.product_dim` prd,
                `bnile-cdw-prod.dssprod_o_warehouse.host_dim` hsd,
                `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead
                where 1=1
                and pof.order_type_key = otd.order_type_key
                and pof.product_key = prd.product_key
                and pof.host_key = hsd.host_key
                and pof.email_address_key= ead.email_address_key
                and prd.sku not like 'GIFTCERT%'
                and prd.product_category_name not in ('Misc')
                and case
                when otd.order_category = 'R' and otd.order_type <> 'REPLACEMENT' and cast(otd.new_order_flag as Numeric) = 1 then 1
                when otd.order_category = 'RMA' and otd.order_type not in ('RETURN FOR MOUNTING') and cast(otd.return_for_work_flag as Numeric) = 0 then 1
                else 0
                end = 1
                and pof.cancel_date_key is null
                and cast(pof.order_amount_net as Numeric) <> 0
                and pof.email_address_key in
                    ( 
                    select distinct
                    pof.email_address_key
                    from
                    `bnile-cdw-prod.dssprod_o_warehouse.product_order_fact` pof,
                    `bnile-cdw-prod.dssprod_o_warehouse.order_type_dim` otd,
                    `bnile-cdw-prod.dssprod_o_warehouse.product_dim` prd
                    where 1=1
                    and pof.order_type_key = otd.order_type_key
                    and pof.product_key = prd.product_key
                    and prd.sku not like 'GIFTCERT%'
                    and prd.product_category_name not in ('Misc')
                    and case
                    when otd.order_category = 'R' and otd.order_type <> 'REPLACEMENT' and cast(otd.new_order_flag as Numeric) = 1 then 1
                    when otd.order_category = 'RMA' and otd.order_type not in ('RETURN FOR MOUNTING') and cast(otd.return_for_work_flag as Numeric)= 0 
                    then 1
                    else 0
                    end = 1
                    and pof.cancel_date_key is null
                    and cast(pof.order_amount_net as Numeric) <> 0

                    )),



    ---Email_activity t2--

    email_activity_t2
     AS
     (
     ------------all orders-------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "ALL") ) as activity_name,
                   count(distinct activity_count) * max(case when activity_type = 'RETURN' then -1 else 1 end) activity_count,              
                  cast(sum(activity_amount) as Int64) activity_amount,
                   min(activity_date) first_activity_date, 
                   max(activity_date) last_activity_date
             from   
                email_activity_t1
             where
                all_flag=1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type

     UNION ALL
     ----------engagement-------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "ENGAGEMENT") ) as activity_name,
                   count(distinct activity_count) * max(case when activity_type = 'RETURN' then -1 else 1 end) activity_count,              
                   sum(activity_amount) activity_amount,
                   min(activity_date) first_activity_date, 
                   max(activity_date) last_activity_date
             from   
                email_activity_t1
             where
                engagement_flag=1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type
      UNION ALL
      ---------engagement ring-------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "ENGAGEMENT RING") ) as activity_name,
                   count(distinct activity_count) * max(case when activity_type = 'RETURN' then -1 else 1 end) activity_count,              
                   sum(activity_amount) activity_amount,
                   min(activity_date) first_activity_date, 
                   max(activity_date) last_activity_date
             from   
                email_activity_t1
             where
                engagement_ring_flag=1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type
      UNION ALL
      -------------wedding band----------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "WEDDING BAND") ) as activity_name,
                   count(distinct activity_count) * max(case when activity_type = 'RETURN' then -1 else 1 end) activity_count,              
                   sum(activity_amount) activity_amount,
                   min(activity_date) first_activity_date, 
                   max(activity_date) last_activity_date
             from   
                email_activity_t1
             where
                wedding_band_flag = 1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type
    UNION ALL
     ---------jewelry-----------------------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "JEWELRY") ) as activity_name,
                   count(distinct activity_count) * max(case when activity_type = 'RETURN' then -1 else 1 end) activity_count,              
                   sum(activity_amount) activity_amount,
                   min(activity_date) first_activity_date, 
                   max(activity_date) last_activity_date
             from   
                email_activity_t1
             where
                jewelry_flag = 1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type

      )             
     select distinct b.up_email_address_key ,b.bnid 
     ,aa.email_address
    ,order_cnt
    ,order_amt
    ,order_first_dt
    ,order_last_dt
    ,order_engagement_cnt
    ,order_engagement_amt
    ,order_engagement_avg_amt
    ,order_engagement_last_dt
    ,order_engagement_first_dt
    ,order_engagement_ring_cnt
    ,order_engagement_ring_last_dt
    ,order_engagement_ring_amt
    ,order_jewelry_cnt
    ,order_jewelry_amt
    ,order_jewelry_avg_amt
    ,order_jewelry_last_dt
    ,order_wedding_band_cnt
    ,order_wedding_band_last_dt
    ,order_wedding_band_amt
     from
    (select 
    distinct a.email_address
    ,order_cnt
    ,order_amt
    ,order_first_dt
    ,order_last_dt
    ,order_engagement_cnt
    ,order_engagement_amt
    ,order_engagement_avg_amt
    ,order_engagement_last_dt
    ,order_engagement_first_dt
    ,order_engagement_ring_cnt
    ,order_engagement_ring_last_dt
    ,order_engagement_ring_amt
    ,order_jewelry_cnt
    ,order_jewelry_amt
    ,order_jewelry_avg_amt
    ,order_jewelry_last_dt
    ,order_wedding_band_cnt
    ,order_wedding_band_last_dt
    ,order_wedding_band_amt
    from 
    (SELECT 
    DISTINCT
           email_address_key,
           eha.email_address,

           IFNULL (SUM (order_cnt)
                   OVER ( PARTITION BY eha.email_address),
                       0) order_cnt,

           IFNULL (  ROUND (SUM (eha.order_amt)
                           OVER ( PARTITION BY eha.email_address),
                       0),0) order_amt,

           MAX (order_last_dt)
                   OVER ( PARTITION BY eha.email_address) 
                   order_last_dt,
           MIN (order_first_dt) over (PARTITION BY eha.email_address)
               order_first_dt,

         IFNULL (  SUM (eha.order_engagement_cnt)
                          OVER ( PARTITION BY eha.email_address),
                          0) order_engagement_cnt,

            IFNULL (  ROUND (SUM (eha.order_engagement_amt)
                           OVER ( PARTITION BY eha.email_address),
                       0),0) order_engagement_amt,

            IFNULL ( ROUND (
                       CASE WHEN SUM (eha.order_engagement_cnt)
                             OVER ( PARTITION BY eha.email_address) = 0
                            THEN 0
                           ELSE SUM (eha.order_engagement_amt) 
                             OVER ( PARTITION BY eha.email_address)
                              / SUM (eha.order_engagement_cnt)
                             OVER ( PARTITION BY eha.email_address)
                        END,0), 0) order_engagement_avg_amt, -- order enagement average amount

            MAX (order_engagement_last_dt)
                   OVER ( PARTITION BY eha.email_address) 
                   order_engagement_last_dt, -- order Enagement Last date,

            MIN (order_engagement_first_dt)
                   OVER ( PARTITION BY eha.email_address) 
                   order_engagement_first_dt, -- order Enagement first date,   


             IFNULL (SUM (eha.order_engagement_ring_cnt)
                   OVER ( PARTITION BY eha.email_address),
                          0) order_engagement_ring_cnt,

            MAX (order_engagement_ring_last_dt)
                   OVER ( PARTITION BY eha.email_address) 
                   order_engagement_ring_last_dt,-- order Enagement Last date
            ifnull (
                   SUM (order_engagement_ring_amt)
                       OVER (PARTITION BY eha.email_address),
               0)
               order_engagement_ring_amt,    

                          -- Jewelry --


                 IFNULL ( SUM (eha.order_jewelry_cnt)
                           OVER ( PARTITION BY eha.email_address),
                       0) order_jewelry_cnt,
            IFNULL( ROUND( SUM (eha.order_jewelry_amt)
                          OVER ( PARTITION BY eha.email_address),
                          0),0) order_jewelry_amt,
            IFNULL ( ROUND (
                       CASE WHEN SUM (eha.order_jewelry_cnt)
                             OVER ( PARTITION BY eha.email_address) = 0
                            THEN 0
                           ELSE SUM (eha.order_jewelry_amt) 
                             OVER ( PARTITION BY eha.email_address)
                              / SUM (eha.order_jewelry_cnt)
                             OVER ( PARTITION BY eha.email_address)
                        END,0), 0) order_jewelry_avg_amt, -- order jewelry average amount

                          MAX (order_jewelry_last_dt)
                   OVER ( PARTITION BY eha.email_address) 
                   order_jewelry_last_dt ,-- order jewelry last date

                IFNULL (  SUM (eha.order_wedding_band_cnt)
                          OVER ( PARTITION BY eha.email_address),
                          0) order_wedding_band_cnt, -- order wedding band count  

                   MAX (order_wedding_band_last_dt)
                   OVER ( PARTITION BY eha.email_address) 
                   order_wedding_band_last_dt, --- order wedding band last date

                   ifnull( ROUND(SUM (order_wedding_band_amt)
                       OVER ( PARTITION BY  eha.email_address),0),0)
               order_wedding_band_amt --- order wedding band amount

            FROM                   
            (  select email_address_key,
                     email_address,

                     CASE WHEN  activity_type IN ('ORDER', 'RETURN') AND activity_name IN ('ORDER ALL', 'RETURN ALL')
                           THEN activity_count ELSE 0 
                      END order_cnt, -- Order Count

                     CASE WHEN  activity_type IN ('ORDER', 'RETURN') AND activity_name IN ('ORDER ENGAGEMENT', 'RETURN ENGAGEMENT')
                         THEN activity_amount ELSE 0
                     END  order_engagement_amt, -- Order Engagement amount

                     CASE WHEN  activity_type IN ('ORDER', 'RETURN')  AND activity_name IN ('ORDER ENGAGEMENT', 'RETURN ENGAGEMENT')
                         THEN activity_count ELSE 0
                     END order_engagement_cnt, -- Order Engagement Count

                     CASE WHEN  activity_type IN ('ORDER') AND activity_name IN ('ORDER ENGAGEMENT')
                          THEN PARSE_DATE('%Y%m%d', CAST(last_activity_date AS STRING))  ELSE  NULL
                     END  order_engagement_last_dt, -- Order Engagement Last Date

                    CASE WHEN activity_type IN ('ORDER') AND activity_name IN ('ORDER ENGAGEMENT')
                          THEN PARSE_DATE('%Y%m%d', CAST(first_activity_date AS STRING)) ELSE NULL
                              END order_engagement_first_dt,

                     CASE WHEN  activity_type IN ('ORDER', 'RETURN')  AND activity_name IN ('ORDER ENGAGEMENT RING', 'RETURN ENGAGEMENT RING')
                         THEN activity_count ELSE 0
                     END order_engagement_ring_cnt, -- Order Engagement Ring Count

                     CASE WHEN  activity_type IN ('ORDER') AND activity_name IN ('ORDER ENGAGEMENT RING')
                          THEN PARSE_DATE('%Y%m%d', CAST(last_activity_date AS STRING))  ELSE  NULL
                     END  order_engagement_ring_last_dt, -- Order Engagement ring Last Date

                     CASE WHEN  activity_type IN ('ORDER', 'RETURN') AND activity_name IN ('ORDER ENGAGEMENT RING', 'RETURN ENGAGEMENT RING')
                          THEN activity_amount
                          ELSE 0
                     end    order_engagement_ring_amt, --Order Engagement ring amount

                 -- Jewelry--   
                     CASE WHEN  activity_type IN ('ORDER', 'RETURN')  AND activity_name IN ('ORDER JEWELRY', 'RETURN JEWELRY')
                         THEN activity_count ELSE 0
                     END order_jewelry_cnt, -- Order jewelry Count

                  CASE WHEN  activity_type IN ('ORDER', 'RETURN') AND activity_name IN ('ORDER JEWELRY', 'RETURN JEWELRY')
                         THEN activity_amount ELSE 0
                     END  order_jewelry_amt,

                      CASE WHEN  activity_type IN ('ORDER', 'RETURN') AND activity_name IN ('ORDER ALL', 'RETURN ALL')
                         THEN activity_amount ELSE 0
                     END  order_amt,

                     CASE WHEN  activity_type IN ('ORDER') AND activity_name IN ('ORDER JEWELRY') 
                          THEN PARSE_DATE('%Y%m%d', CAST(last_activity_date AS STRING))  ELSE  NULL
                     END  order_jewelry_last_dt, -- Order jewelry Last Date

                     CASE WHEN  activity_type IN ('ORDER', 'RETURN')  AND activity_name IN ('ORDER WEDDING BAND', 'RETURN WEDDING BAND')
                         THEN activity_count ELSE 0
                     END order_wedding_band_cnt, -- Order wedding band Count

                      CASE WHEN  activity_type IN ('ORDER') AND activity_name IN ('ORDER WEDDING BAND') 
                          THEN PARSE_DATE('%Y%m%d', CAST(last_activity_date AS STRING))  ELSE  NULL
                     END  order_wedding_band_last_dt,-- Order wedding band Last Date

                     CASE  WHEN activity_type IN ('ORDER', 'RETURN') AND activity_name IN ('ORDER WEDDING BAND', 'RETURN WEDDING BAND')
                              THEN activity_amount
                              ELSE 0  END order_wedding_band_amt, -- order_wedding_band_amount

                     CASE WHEN activity_type IN ('ORDER') AND activity_name IN ('ORDER ALL')
                                  THEN PARSE_DATE('%Y%m%d', CAST(first_activity_date AS STRING))
                                  ELSE NULL end order_first_dt,


                     CASE WHEN  activity_type IN ('ORDER') AND activity_name IN ('ORDER ALL') 
                          THEN PARSE_DATE('%Y%m%d', CAST(last_activity_date AS STRING))  ELSE  NULL
                     END  order_last_dt, -- Order  Last Date

              from email_activity_t2  where coalesce(trim(email_address),'9999') not in('-')
               and coalesce(trim(email_address),'@') like '%@%' and coalesce(trim(email_address),'.') like '%.%'
                and coalesce(trim(email_address),'9999') not like '%jminsure.com'
                and coalesce(trim(email_address),'9999') not like 'apayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like 'cntmpayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like '%test%bluenile%'
                and coalesce(trim(email_address),'9999') not like '%@test.com'
                and coalesce(trim(email_address),'9999') not in ('noemail@noemail.com') 
                and coalesce(trim(email_address),'9999') not like '%@removed.com'
                and email_address is not null                       
             )  eha ) a )aa

            left join 

    (select up_email_address_key,bnid,email_address from 
    `bnile-cdw-prod.dag_migration_test.up_customer_attributes`
      where email_address is not null) b
    on aa.email_address=b.email_address
    where bnid is not null
          );


    ---- Query for stg_browse_attributes ---


    CREATE OR REPLACE TABLE `bnile-cdw-prod.dag_migration_test.stg_browse_attributes` as
     (
    with browse_activity_t1 as
    (
    (  
    select ead.email_address_key, 
               ead.email_address,  
                sd.site, 
                hsd.hostname,
                'BROWSE' activity_type, 
                cf.date_key browse_date,
                1 all_flag,
                case when (pg.primary_browse_segment = 'engagement') or 
                ( pg.primary_browse_Segment ='education' and pg.secondary_browse_segment ='engagement') 
                then 1 else 0 end engagement_flag,
                case when (pg.primary_browse_segment = 'diamond' ) or 
                (pg.primary_browse_segment ='education' and pg.secondary_browse_segment ='diamond') then 1 else 0 end diamond_flag,
                case when pg.primary_browse_segment = 'wedding_band' then 1 else 0 end wedding_band_flag,
                case when pg.primary_browse_segment = 'diamond_jewelry' then 1 else 0 end diamond_jewelry_flag,
                case when urd.uri like '%pearl-jewelry%' or urd.uri like '%pearl-jewellery%' then 1 else 0 end pearl_jewelry_flag,
                case when urd.uri like '%gold-jewelry%' or urd.uri like '%gold-jewellery%' then 1 else 0 end gold_jewelry_flag,
                case when urd.uri like '%silver-jewelry%' or urd.uri like '%silver-jewellery%' then 1 else 0 end silver_jewelry_flag,
                case when urd.uri like '%gemstone-jewelry%' or urd.uri like '%gemstone-jewellery%' then 1 else 0 end gemstone_jewelry_flag,
                case when pg.primary_browse_segment = 'designer' then 1 else 0 end designer_flag,
                case when pg.primary_browse_segment = 'other_jewelry' then 1 else 0 end other_jewelry_flag
           from `bnile-cdw-prod.dssprod_o_warehouse.click_fact` cf, 
                `bnile-cdw-prod.dssprod_o_warehouse.uri_dim` urd, 
                `bnile-cdw-prod.dssprod_o_warehouse.host_dim` hsd, 
                `bnile-cdw-prod.dssprod_o_warehouse.email_guid` emg,
                `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead, 
                `bnile-cdw-prod.dssprod_o_warehouse.site_dim` sd, 
                `bnile-cdw-prod.dssprod_o_warehouse.page_group_dim` pg
                     where 1=1
                       and cf.uri_key = urd.uri_key
                       and cf.site_key = sd.site_key
                       and cf.host_key = hsd.host_key
                       and cf.guid_key = emg.guid_key
                       and emg.email_address_key = ead.email_address_key 
                       and cf.page_group_key = pg.page_group_key
                       and cast(cf.status as Numeric) in (200, 304)       
      ) ),


    browse_activity_t2
     AS
     (
     ------------all orders-------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "ALL") ) as activity_name,
                   count(*) activity_count,              
                   min(browse_date) first_activity_date, 
                   max(browse_date) last_activity_date
             from   
                browse_activity_t1
             where
                all_flag=1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type

     UNION ALL
     ----------engagement-------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "ENGAGEMENT") ) as activity_name,
                   count(*) activity_count,              
                   min(browse_date) first_activity_date, 
                   max(browse_date) last_activity_date
             from   
                browse_activity_t1
             where
                engagement_flag=1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type
      UNION ALL

      -------------wedding band----------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "WEDDING BAND") ) as activity_name,
                   count(*) activity_count,              
                   min(browse_date) first_activity_date, 
                   max(browse_date) last_activity_date
             from   
                browse_activity_t1
             where
                wedding_band_flag=1
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type

     UNION ALL       
     ---------jewelry-----------------------------------
     select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "JEWELRY") ) as activity_name,
                   count(*) activity_count,              
                   min(browse_date) first_activity_date, 
                   max(browse_date) last_activity_date
             from   
                browse_activity_t1
             where
                (diamond_jewelry_flag=1 or
                  pearl_jewelry_flag = 1 or
                  gold_jewelry_flag = 1 or
                  silver_jewelry_flag = 1 or
                  gemstone_jewelry_flag = 1 or
                  other_jewelry_flag = 1 
                 )
             group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type
      ----- diamond -----				   
	  UNION ALL   			   
		select
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type,
                   (UPPER (activity_type||' '|| "DIAMOND") ) as activity_name,
                   count(*) activity_count,              
                   min(browse_date) first_activity_date, 
                   max(browse_date) last_activity_date
             from   
                browse_activity_t1

			where
                diamond_flag=1

              group by 
                   email_address_key,
                   email_address,
                   site,
                   hostname,
                   activity_type		   

    )
    select distinct b.up_email_address_key,b.bnid 
    ,aa.email_address
    ,browse_cnt,browse_first_dt,browse_last_dt, 
           browse_engagement_cnt,browse_engagement_first_dt,
           browse_engagement_last_dt,browse_jewelry_cnt,browse_jewelry_first_dt,
           browse_jewelry_last_dt,
           browse_wedding_band_cnt,browse_wedding_band_first_dt,browse_wedding_band_last_dt
		   ,browse_diamond_last_dt


    from
    (select s.email_address_key ,
    s.email_address,s.browse_cnt,s.browse_first_dt,s.browse_last_dt, 
           s.browse_engagement_cnt,s.browse_engagement_first_dt,
           s.browse_engagement_last_dt,s.browse_jewelry_cnt,s.browse_jewelry_first_dt,
           s.browse_jewelry_last_dt,
           s.browse_wedding_band_cnt,s.browse_wedding_band_first_dt,s.browse_wedding_band_last_dt
		   ,s.browse_diamond_last_dt
		   from

    (select b.email_address_key,b.email_address,b.browse_cnt,b.browse_first_dt,b.browse_last_dt, 
           b.browse_engagement_cnt,b.browse_engagement_first_dt,
           b.browse_engagement_last_dt,b.browse_jewelry_cnt,b.browse_jewelry_first_dt,
           b.browse_jewelry_last_dt,
           b.browse_wedding_band_cnt,b.browse_wedding_band_first_dt,b.browse_wedding_band_last_dt
		   ,b.browse_diamond_last_dt


    from 

    (select bb.email_address_key,bb.email_address,bb.browse_engagement_cnt
    ,bb.browse_cnt
    ,bb.browse_engagement_last_dt
    ,bb.browse_engagement_first_dt
    ,bb.browse_jewelry_cnt
    ,bb.browse_jewelry_first_dt
    ,bb.browse_jewelry_last_dt
    ,bb.browse_first_dt
    ,bb.browse_last_dt
    ,bb.browse_wedding_band_cnt
    ,bb.browse_wedding_band_last_dt
    ,bb.browse_wedding_band_first_dt
	,bb.browse_diamond_last_dt
    ,ROW_NUMBER() OVER(PARTITION BY email_address order by browse_last_dt desc) AS row_num
    from

    (select ff.email_address_key,coalesce(ff.email_address,c.email_address) as email_address
    ,browse_cnt
    ,browse_engagement_cnt
    ,browse_engagement_last_dt
    ,browse_engagement_first_dt
    ,browse_jewelry_cnt
    ,browse_jewelry_first_dt
    ,browse_jewelry_last_dt
    ,browse_first_dt
    ,browse_last_dt
    ,browse_wedding_band_cnt
    ,browse_wedding_band_last_dt
    ,browse_wedding_band_first_dt
	,browse_diamond_last_dt

      from 
    ((SELECT  
           email_address_key,
           eha.email_address,
           SUM (CASE
                WHEN     eha.activity_type IN ('BROWSE')
                     AND eha.activity_name IN ('BROWSE ALL')
                THEN
                    eha.activity_count
                ELSE
                    0
                END) browse_cnt,
           MIN (CASE
                WHEN     eha.activity_type IN ('BROWSE')
                    AND eha.activity_name IN ('BROWSE ALL')
                THEN
                     PARSE_DATE('%Y%m%d', CAST(eha.first_activity_date AS STRING)) 
                ELSE
                                      NULL
                              END)                      browse_first_dt,
           MAX (CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN ('BROWSE ALL')
                                  THEN
                                     PARSE_DATE('%Y%m%d', CAST(eha.last_activity_date AS STRING)) 
                                  ELSE
                                      NULL
                              END)                      browse_last_dt,
           SUM (CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE ENGAGEMENT')
                                  THEN
                                      eha.activity_count
                                  ELSE
                                      0
                              END)                      browse_engagement_cnt,
                          MIN (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE ENGAGEMENT')
                                  THEN
                                      PARSE_DATE('%Y%m%d', CAST(eha.first_activity_date AS STRING)) 
                                  ELSE
                                      NULL
                              END)                      browse_engagement_first_dt,
                          MAX (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE ENGAGEMENT')
                                  THEN
                                      PARSE_DATE('%Y%m%d', CAST(eha.last_activity_date AS STRING)) 
                                  ELSE
                                      NULL
                              END)                      browse_engagement_last_dt,
                  SUM (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE JEWELRY')
                                  THEN
                                      eha.activity_count
                                  ELSE
                                      0
                              END)                      browse_jewelry_cnt,
                          MIN (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE JEWELRY')
                                  THEN
                                      PARSE_DATE('%Y%m%d', CAST(eha.first_activity_date AS STRING)) 
                                  ELSE
                                      NULL
                              END)                      browse_jewelry_first_dt,
                          MAX (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE JEWELRY')
                                  THEN
                                     PARSE_DATE('%Y%m%d', CAST(eha.last_activity_date AS STRING))  
                                  ELSE
                                      NULL
                              END)                      browse_jewelry_last_dt,
                  SUM (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE WEDDING BAND')
                                  THEN
                                      eha.activity_count
                                  ELSE
                                      0
                              END)                      browse_wedding_band_cnt,
                          MIN (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE WEDDING BAND')
                                  THEN
                                        PARSE_DATE('%Y%m%d', CAST(eha.first_activity_date AS STRING)) 
                                  ELSE
                                      NULL
                              END)                      browse_wedding_band_first_dt,
                          MAX (
                              CASE
                                  WHEN     eha.activity_type IN ('BROWSE')
                                       AND eha.activity_name IN
                                               ('BROWSE WEDDING BAND')
                                  THEN
                                       PARSE_DATE('%Y%m%d', CAST(eha.last_activity_date AS STRING))  
                                  ELSE
                                      NULL
                              END)                      browse_wedding_band_last_dt,
						 MAX (
                          CASE
                              WHEN     eha.activity_type IN ('BROWSE')
                                   AND eha.activity_name IN ('BROWSE DIAMOND')
                              THEN
                                  PARSE_DATE('%Y%m%d', CAST(eha.last_activity_date AS STRING))
                              ELSE
                                  NULL
                          END)                      browse_diamond_last_dt	  


         from browse_activity_t2  eha 

         group by email_address_key,email_address )  ff
         left join

    ((SELECT email_address,email_address_key FROM `bnile-cdw-prod.dssprod_o_warehouse.click_fact` where email_address is not null group by email_address,email_address_key)
    ) c
    on ff.email_address_key  = c.email_address_key))bb
    where bb.email_address is not null
    )b
    where row_num=1 and email_address is not null
    and coalesce(trim(email_address),'9999') not in('-')
    and coalesce(trim(email_address),'@') like '%@%' and coalesce(trim(email_address),'.') like '%.%'
                and coalesce(trim(email_address),'9999') not like '%jminsure.com'
                and coalesce(trim(email_address),'9999') not like 'apayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like 'cntmpayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like '%test%bluenile%'
                and coalesce(trim(email_address),'9999') not like '%@test.com'
                and coalesce(trim(email_address),'9999') not in ('noemail@noemail.com') ) s )aa
                 left join 

    (select up_email_address_key,bnid,email_address from 
    `bnile-cdw-prod.dag_migration_test.up_customer_attributes`
      where email_address is not null) b
    on aa.email_address=b.email_address
    where bnid is not null
                )  ;


    ---- Query for stg_flag_attributes----



    CREATE OR REPLACE TABLE `bnile-cdw-prod.dag_migration_test.stg_flag_attributes` as

    (
    with flags as 
    (
    select email_address, never_send_flag,bad_address_flag
        from

        (select	 email_address,

                max(case
                    when cast(is_never_send as Numeric) = 1 then 'Y'
                    when cast(is_never_send as Numeric) = 0 then 'N'
                end ) never_send_flag,
               max( case
                    when cast(is_bad_address as Numeric) = 1 then 'Y'
                    when cast(is_bad_address as Numeric) = 0 then 'N'
                end ) bad_address_flag			
                from  `bnile-cdw-prod.nileprod_o_warehouse.email_address`
                group by email_address
                  )
                  ),

    ---- undeliverable_date-----------

    undeliver_date as

    (
    select email_address, undeliverable_date
        from

        (select	  email_address,
                    max(undeliverable_date) as undeliverable_date			
                from  `bnile-cdw-prod.nileprod_o_warehouse.email_address`

                group by email_address
                  )
            ),


    --- fraud_flag------

    fraud as
     (select  email_address,max(fraud_flag) fraud_flag from
    (select email_address,
           fraud_flag

            from `bnile-cdw-prod.dssprod_o_warehouse.email_address`
            where email_address is not null )
             group by email_address
            ),

    --- opt_down ----

    opt as 
    (select email_address, opt_down from
    (select  email_address, 
                  opt_down, 
                  row_number() over (partition by email_address order by CREATE_DATE desc) rn
    from `bnile-cdw-prod.nileprod_o_warehouse.email_subscription_change_log` where opt_down is not null and 
    email_address is not null)
    where rn =1),

    promo_date as 
    (select email_address,
     MIN (CASE WHEN subscription_type = 'Promo'   THEN  first_subscribe_date
      ELSE NULL END) first_promo_subscribe_date,
      FROM `bnile-cdw-prod.dssprod_o_warehouse.email_address_subscription`
      group by email_address ),

    --- subscribe_flag, last_subscribe_date ----
    subscribe as

    (select  email_address, max(subscribe_flag) subscribe_flag , max(last_subscribe_date) as last_subscribe_date
    from      
    (select
             email_address,
             IFNULL (subscribe_flag, 'N')
                              subscribe_flag,
                 last_subscribe_date
            from
            `bnile-cdw-prod.dssprod_o_warehouse.email_address_subscription` 

    )
    group by email_address
    ),


    --- sweepstakes_entry_last_dt ----

    sweepstakes as
    (select  email_address,
    MAX (aa.sweepstakes_entry_last_dt) 
    OVER ( PARTITION BY aa.email_address) sweepstakes_entry_last_dt
    from 
    (select distinct email_address, CASE WHEN  eha.activity_type = 'SITE EVENT' AND eha.activity_name = 'SITE EVENT SWEEPSTAKES ENTRY'
                                         THEN eha.last_activity_date
                                         ELSE NULL
                                         END  sweepstakes_entry_last_dt 
     from
    (select

                 email_address,
                ifnull(se.event_site,'BN') site,
                case
                    when se.event_site = 'BN' then 'www.bluenile.com'
                    when se.event_site = 'BNCA' then 'www.bluenile.ca'
                    when se.event_site = 'BNUK' then 'www.bluenile.co.uk'
                    else 'www.bluenile.com'
                end hostname,
                'SITE EVENT' activity_type,
                'SITE EVENT SWEEPSTAKES ENTRY' activity_name,
                0 activity_count, -- only dates matter for sweepstakes
                0 activity_amount, -- only dates matter for sweepstakes
                min(se.event_date) first_activity_date,
                max(se.event_date) last_activity_date
                from `bnile-cdw-prod.nileprod_o_warehouse.site_event` se
                where (lower(se.event_name) like '%sweepstakes%'  or lower(se.event_name) like '%sweeps%')       
                and se.email_address is not null
                group by
                email_address,
                ifnull(se.event_site,'BN'),
                case
                    when se.event_site = 'BN' then 'www.bluenile.com'
                    when se.event_site = 'BNCA' then 'www.bluenile.ca'
                    when se.event_site = 'BNUK' then 'www.bluenile.co.uk'
                    else 'www.bluenile.com'
                end)eha) aa),

    ---- credit_card_create_date, credit_card_status----

    credit_card_details as
    (
    select email_address, 
    cc_application_date as credit_card_create_date,
                    CASE WHEN cc_approved_flag = 1 AND order_cnt >= 1
                         THEN 'Purchased'
                         WHEN cc_approved_flag = 1
                         THEN 'Approved'
                         WHEN cc_approved_flag = 0
                         THEN 'Declined'
                         ELSE 'Prospect'
                    END credit_card_status                                                                                               
    from
    (
    with 
    credit_card 
    as 
    (select  
    distinct   email_address,
    MAX ( CASE  WHEN ace.application_status = 'Approval' THEN 1  ELSE 0 END)  cc_approved_flag,
                         MIN (ace.create_date)    cc_application_date 
     from 
    (SELECT distinct customer_id,application_status,create_date from   `bnile-cdw-prod.nileprod_o_warehouse.alliance_card_event`) ace
                      join
                       (select distinct customer_id,login_name  from `bnile-cdw-prod.nileprod_o_warehouse.customer`) c
                          on c.customer_id = ace.customer_id
                         join
                        ( select distinct email_address from `bnile-cdw-prod.dssprod_o_warehouse.email_address`) ea
                          on ea.email_address = c.login_name
                         group by  email_address)

    select  b.email_address, b.cc_approved_flag, b.cc_application_date, a.order_cnt
    from
    (SELECT email_address, order_cnt  FROM 
    `bnile-cdw-prod.dag_migration_test.stg_order_attributes`

    where email_address in ( select email_address from credit_card)) a
    right join
    (select email_address, cc_approved_flag, cc_application_date
    from 
    credit_card) b
    on a.email_address = b.email_address)),

    --- email_open_last_dt-----
    ---last_email_send_date ---
	-----email_click_last_dt------

 email_metrics_data_fields as (


with email_activity_data as 
(with 
email_metric_fact as (
SELECT
  a.up_email_address_key,
  b.email_address,
  cast (format_date('%Y%m%d', a.activity_date) as int64) date_key,
  a.email_send_count,
  a.email_open_count,
  a.email_click_count,
  cast (a.placement_key as numeric) placement_key
FROM
  `bnile-cdw-prod.o_warehouse.email_activity_fact` a
join
`bnile-cdw-prod.dag_migration_test.up_customer_attributes` b
on cast (a.up_email_address_key as numeric) =  b.up_email_address_key 

WHERE
  CAST (create_date_utc AS date) = (select max(CAST (create_date_utc AS date)) from `bnile-cdw-prod.o_warehouse.email_activity_fact`)
  AND (email_send_count > 0
    OR email_open_count> 0
    OR email_click_count >0)
)
, emailclick as
    (
    Select
    up_email_address_key,
    emf.email_address,
    max(PARSE_DATE('%Y%m%d', CAST(f.date_key AS STRING))) email_click_last_dt
    from
    `bnile-cdw-prod.dssprod_o_warehouse.placement_dim` pd,
    `bnile-cdw-prod.dssprod_o_warehouse.placement_transfer_fact` f,
     email_metric_fact emf
    where 
    pd.placement_key = f.placement_key and
     pd.placement_key = emf.placement_key
    and f.date_key = emf.date_key
    and f.EMAIL_ADDRESS is not null and f.EMAIL_ADDRESS not like ''
    and emf.email_click_count  > 0
    group by email_address,up_email_address_key
    ),  
   emailopen  as(
    SELECT
      distinct  email_address, up_email_address_key,
	  max(PARSE_DATE('%Y%m%d',CAST(date_key AS STRING))) as email_open_last_dt
    FROM
     email_metric_fact f
    INNER JOIN
      `bnile-cdw-prod.dssprod_o_warehouse.placement_dim` pd
    ON
      f.placement_key = pd.placement_key
    WHERE
      f.EMAIL_OPEN_COUNT > 0 and email_address is not null
      and (PLACEMENT_CATEGORY_TYPE_NAME  ='Promo'  or PLACEMENT_CATEGORY_TYPE_NAME = 'Ongoing')
      group by email_address,up_email_address_key
    ),

       last_email_send as
    (SELECT
      distinct email_address, up_email_address_key, max(PARSE_DATE('%Y%m%d',CAST(date_key AS STRING))) as last_email_send_date
    FROM
      email_metric_fact f
    INNER JOIN
      `bnile-cdw-prod.dssprod_o_warehouse.placement_dim` pd
    ON
      f.placement_key = pd.placement_key
    WHERE
      f.email_send_count > 0
      and (PLACEMENT_CATEGORY_TYPE_NAME  ='Promo'  or PLACEMENT_CATEGORY_TYPE_NAME = 'Ongoing')
      group by email_address, up_email_address_key)              
select
distinct safe_cast(coalesce(a.up_email_address_key ,b.up_email_address_key ,c.up_email_address_key) as int64) up_email_address_key,  coalesce(a.email_address,b.email_address,c.email_address) email_address, email_click_last_dt,email_open_last_dt,last_email_send_date  from emailclick a
full outer join
emailopen b
using (email_address)
full outer join
last_email_send c
using(email_address) ),

--- email_metrics_fact_ data all the data 


email_metrics_data  as (
  with emailclick as
    (
    Select 
    ead.email_address,
    max(PARSE_DATE('%Y%m%d', CAST(f.date_key AS STRING))) email_click_last_dt
    from
    `bnile-cdw-prod.dssprod_o_warehouse.placement_dim` pd,
    `bnile-cdw-prod.dssprod_o_warehouse.placement_transfer_fact` f,
    `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead,
    `bnile-cdw-prod.dssprod_o_warehouse.email_metrics_fact` emf
    where 
    pd.placement_key = f.placement_key 
    and lower(trim(f.email_address)) = ead.email_address
    and cast(ead.email_address_key  as string)= cast(emf.email_address_key as string)
    and pd.placement_key = emf.placement_key
    and f.date_key = emf.date_key
    and f.EMAIL_ADDRESS is not null and f.EMAIL_ADDRESS not like ''
    and emf.email_click_count  > 0
    --and emf.DATE_KEY = (select max(DATE_KEY) from  `bnile-cdw-prod.dssprod_o_warehouse.email_metrics_fact` )
    group by email_address
    ),  
   emailopen  as(
    SELECT
      distinct trim(lower(email_address)) email_address, 
	  max(PARSE_DATE('%Y%m%d',CAST(date_key AS STRING))) as email_open_last_dt
    FROM
     `bnile-cdw-prod.dssprod_o_warehouse.email_metrics_fact` f
    INNER JOIN
      `bnile-cdw-prod.dssprod_o_warehouse.placement_dim` pd
    ON
      f.placement_key = pd.placement_key
    INNER JOIN
      `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead
    ON
      f.email_address_key = ead.email_address_key
    WHERE
      f.EMAIL_OPEN_COUNT > 0 and email_address is not null
      and (PLACEMENT_CATEGORY_TYPE_NAME  ='Promo'  or PLACEMENT_CATEGORY_TYPE_NAME = 'Ongoing')

      group by email_address
    ),

       last_email_send as
    (SELECT
      distinct email_address,  max(PARSE_DATE('%Y%m%d',CAST(date_key AS STRING))) as last_email_send_date
    FROM
      `bnile-cdw-prod.dssprod_o_warehouse.email_metrics_fact` f
    INNER JOIN
      `bnile-cdw-prod.dssprod_o_warehouse.placement_dim` pd
    ON
      f.placement_key = pd.placement_key
    INNER JOIN
      `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim` ead
    ON
      f.email_address_key = ead.email_address_key
    WHERE
      f.email_send_count > 0
      and (PLACEMENT_CATEGORY_TYPE_NAME  ='Promo'  or PLACEMENT_CATEGORY_TYPE_NAME = 'Ongoing')

      group by email_address)

select   b.up_email_address_key , a.* from         
(select 
coalesce(a.email_address,b.email_address,c.email_address) email_address, email_click_last_dt,email_open_last_dt,last_email_send_date  from emailclick a
full outer join
emailopen b
using (email_address)
full outer join
last_email_send c
using(email_address)
)a
left join
`bnile-cdw-prod.dag_migration_test.up_customer_attributes` b
on a.email_address = b.email_address)


-- combining the both the email_activity_fact  one day data and email_metrics_data and taking the max dates of the three attributes 


select email_address, max(email_click_last_dt) email_click_last_dt, max(email_open_last_dt) email_open_last_dt, max(last_email_send_date) last_email_send_date
from
(
select email_address,email_click_last_dt,email_open_last_dt,last_email_send_date  from email_activity_data
union all
select email_address,email_click_last_dt,email_open_last_dt,last_email_send_date  from email_metrics_data 
)
group by email_address
),

    -- queens_english_flag----
    queenflag as
    (select * from
    (select 
    eas.email_address as email_address,LAST_SUBSCRIBE_DATE,eas.SUBSCRIBE_FLAG,
      CASE
        WHEN IFNULL(eas.language_code, 'en-us') LIKE 'en%' AND q.queens_english_country IS NOT NULL THEN 'Y'
      ELSE
      'N'
    END
      queens_english_flag,
      language_code,
      ship_to_country_code
      ,ROW_NUMBER() OVER(PARTITION BY eas.email_address order by ifnull(subscribe_flag,'N') desc,eas.LAST_SUBSCRIBE_DATE desc, 
      eas.LAST_UNSUBSCRIBE_DATE  desc, eas.LAST_UPDATE_DATE  desc) AS row_num 
    FROM 
    (
      SELECT
        ship_to_country queens_english_country
      FROM
        `bnile-cdw-prod.nileprod_o_warehouse.ship_to_country_language`
      WHERE
        IFNULL(language_code,'en-us') <> 'en-us'
        AND IFNULL(language_code,'en-us') LIKE 'en%') q 
          right join
        (select * from (select *, ROW_NUMBER() OVER(PARTITION BY email_address order by ifnull(subscribe_flag,'N') desc,LAST_SUBSCRIBE_DATE desc, 
      LAST_UNSUBSCRIBE_DATE  desc, LAST_UPDATE_DATE  desc) row_num from `bnile-cdw-prod.dssprod_o_warehouse.email_address_subscription`) where row_num =1 ) eas --nileprod
       on IFNULL(eas.ship_to_country_code, 'US') = q.queens_english_country
       where language_code <>'nan' and language_code <>'en-us'
       )
    where row_num =1 and queens_english_flag = 'Y'),

    -- ip_time_zone ---
    iptime as
    (select email_address, IP_TIME_ZONE from
    (SELECT
      email_address,
      IP_TIME_ZONE,
      row_number () OVER 
      (PARTITION BY email_address ORDER BY LAST_SUBSCRIBE_DATE DESC, LAST_UNSUBSCRIBE_DATE DESC, LAST_UPDATE_DATE  desc,IP_TIME_ZONE desc ) row_number
    FROM
      `bnile-cdw-prod.dssprod_o_warehouse.email_address_subscription`
      where  IP_TIME_ZONE is not null and IP_TIME_ZONE <> '-'
      )
     where row_number =1 and IP_TIME_ZONE is not null) ,

    --- Pending_order_flag------ 
    pending as 
    (
      select * from 
    (SELECT  email_address,
    CASE  WHEN     basket_status = 'Processed'
                   AND TIMESTAMP_DIFF(cast (EXTRACT(datetime FROM  (SELECT current_timestamp()) AT TIME ZONE "America/Los_Angeles")  as timestamp),cast(basket_status_date as timestamp), DAY) <=3 
           THEN 'Y'
           WHEN basket_status = 'Processed'
           THEN 'N'
           ELSE 'Y' end pending_order_flag
    FROM `bnile-cdw-prod.nileprod_o_warehouse.basket` 
    where CUSTOMER_SUBMIT_DATE is not null and email_address IS NOT NULL and basket_status NOT IN ('Cancelled') )
    where pending_order_flag = 'Y'),







    -- --- activity_status ----

    activity as
    (select email_address,
    CASE
                   WHEN ifnull (subscribe_flag, 'N') = 'N'
                        OR fraud_flag = 'Y'
                        OR never_send_flag = 'Y'
                        OR bad_address_flag = 'Y'
                        OR undeliverable_date IS NOT NULL
                   THEN
                       'Ineligible'
                   WHEN email_open_last_dt >= DATE_SUB (current_date(), INTERVAL 365 DAY) 
                   THEN
                       'Active'
                   WHEN cast(last_subscribe_date as date) >= DATE_SUB (current_date(), INTERVAL 365 DAY) 
                   THEN
                       'Active'
                   WHEN email_click_last_dt >= DATE_SUB (current_date(), INTERVAL 365 DAY) 
                   THEN
                       'Active'
                   WHEN browse_last_dt >= DATE_SUB (current_date(), INTERVAL 365 DAY) 
                   THEN
                       'Active'
                   ELSE
                       'Lapsed'
               END
                   activity_status 
    from flags 
    full outer join
     fraud
     using(email_address)
     full outer join
     email_metrics_data_fields
     using (email_address)
     full outer join
     subscribe
     using(email_address)
     full outer join
     undeliver_date
     using(email_address)
     full outer join
     (select email_address, browse_last_dt from 
     `bnile-cdw-prod.dag_migration_test.stg_browse_attributes`)
     using(email_address)),

    customer_segment as

    ( select email_address,
    case
    when
    (order_engagement_last_dt is null or order_engagement_last_dt <= DATE_SUB(current_date(), interval 365 day))  
    -- customers who has not placed eng order in last 365 days
    and (marital_status not in ('Engaged', 'Married') or marital_status is null)
    and ((BROWSE_ENGAGEMENT_LAST_DT >= DATE_SUB(current_date(), interval 180 day) )
    or (BROWSE_DIAMOND_LAST_DT >= DATE_SUB(current_date(), interval 180 day)) --for customers browsing diamonds -- Added by Sudha as per URES-2
    or ( MARITAL_STATUS = 'Engaged Soon' and cast (MARITAL_STATUS_TIMESTAMP as date) > DATE_SUB(current_date(), interval 120 day) )
    )
    and browse_engagement_cnt >= browse_wedding_band_cnt
    then 'Engagement'
    when ifnull(order_wedding_band_amt,0) = 0
    and ( (BROWSE_WEDDING_BAND_LAST_DT > DATE_SUB(current_date(), interval 180 day))
    or (MARITAL_STATUS = 'Engaged' and cast (MARITAL_STATUS_TIMESTAMP as date) > DATE_SUB(current_date(), interval 120 day)
    and (WEDDING_DATE > current_date() OR WEDDING_DATE is NULL))
    or (ORDER_ENGAGEMENT_RING_AMT > 0 and ORDER_ENGAGEMENT_RING_LAST_DT > DATE_SUB(current_date(), interval 180 day)
    and (WEDDING_DATE > current_date() or WEDDING_DATE is NULL))
    )
    then 'Wedding'
    when (order_jewelry_avg_amt >=500 or order_engagement_amt >= 10000)
    then 'High Buyers'
    when (order_cnt > 0 and order_jewelry_avg_amt < 500 and order_engagement_amt < 10000)
    then 'Low Buyers'
    when (activity_status = 'Active' and ifnull(order_cnt,0) = 0)
    then 'Active NonBuyers'
    when (activity_status = 'Lapsed' and ifnull(order_cnt,0) = 0 )
    then 'Lapsed NonBuyers'
    else 'Other'
    end customer_segment_exact	
    from
    (select distinct 
    coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) as email_address,
    first_value(order_engagement_last_dt) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by order_engagement_last_dt desc) order_engagement_last_dt,
    first_value(marital_status) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by marital_status desc) marital_status,
    first_value(BROWSE_ENGAGEMENT_LAST_DT) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by BROWSE_ENGAGEMENT_LAST_DT desc) BROWSE_ENGAGEMENT_LAST_DT,
    first_value(BROWSE_DIAMOND_LAST_DT) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by BROWSE_DIAMOND_LAST_DT desc) BROWSE_DIAMOND_LAST_DT,
    first_value(cast (MARITAL_STATUS_TIMESTAMP as date)) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by MARITAL_STATUS_TIMESTAMP desc) MARITAL_STATUS_TIMESTAMP,
    first_value(browse_engagement_cnt) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by browse_engagement_cnt desc) browse_engagement_cnt,
    first_value(order_wedding_band_amt) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by order_wedding_band_amt desc) order_wedding_band_amt,
    first_value(browse_wedding_band_cnt) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by order_wedding_band_amt desc) browse_wedding_band_cnt,
    first_value(BROWSE_WEDDING_BAND_LAST_DT) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by BROWSE_WEDDING_BAND_LAST_DT desc) BROWSE_WEDDING_BAND_LAST_DT,
    first_value(WEDDING_DATE) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by WEDDING_DATE desc) WEDDING_DATE,
    first_value(ORDER_ENGAGEMENT_RING_AMT) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by ORDER_ENGAGEMENT_RING_AMT desc) ORDER_ENGAGEMENT_RING_AMT,
    first_value(ORDER_ENGAGEMENT_RING_LAST_DT) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by ORDER_ENGAGEMENT_RING_LAST_DT desc) ORDER_ENGAGEMENT_RING_LAST_DT,
    first_value(order_jewelry_avg_amt) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by order_jewelry_avg_amt desc) order_jewelry_avg_amt,
    first_value(order_engagement_amt) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by order_engagement_amt desc) order_engagement_amt,
    first_value(order_cnt) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by order_cnt desc) order_cnt,
    first_value(activity_status) 
    over (partition by coalesce(act.email_address, stg_b.email_address, stg_o.email_address, stg_p.email_address ) order by activity_status desc) activity_status,
    from
    (
    select distinct
    email_address,
    case
    when cast(email_open_last_dt as date) >= DATE_SUB (current_date(), INTERVAL 365 DAY) then 'Active'
    when cast(last_subscribe_date as date) >= DATE_SUB (current_date(), INTERVAL 365 DAY) then 'Active'
    when cast(email_click_last_dt as date) >= DATE_SUB (current_date(), INTERVAL 365 DAY) then 'Active'
    when cast(browse_last_dt as date) >= DATE_SUB (current_date(), INTERVAL 365 DAY) then 'Active'
    else 'Lapsed' end ACTIVITY_STATUS
    from
    email_metrics_data_fields
    full outer join
    subscribe
    using(email_address)
    full outer join
    (select email_address, browse_last_dt from 
        `bnile-cdw-prod.dag_migration_test.stg_browse_attributes`)
    using(email_address) ) act
    full outer join
    `bnile-cdw-prod.dag_migration_test.stg_browse_attributes` stg_b
    on stg_b.email_address = act.email_address
    full outer join
    `bnile-cdw-prod.dag_migration_test.stg_order_attributes` stg_o
    on stg_b.email_address =  stg_o.email_address
    full outer join
    `bnile-cdw-prod.dag_migration_test.stg_pii_attributes` stg_p
    on stg_o.email_address =  stg_p.email_address)
    )



    select distinct b.up_email_address_key ,b.bnid,aa.email_address
    ,aa.never_send_flag, aa.bad_address_flag,aa.undeliverable_date, aa.fraud_flag,aa.opt_down, aa.first_promo_subscribe_date,aa.subscribe_flag,aa.last_subscribe_date,aa.sweepstakes_entry_last_dt,aa.credit_card_status,aa.credit_card_create_date
    ,aa.email_open_last_dt,aa.last_email_send_date
    ,aa.email_click_last_dt
    ,aa.queens_english_flag,aa.ip_time_zone, aa.pending_order_flag
    ,aa.activity_status, customer_segment_exact  
    from  
    (select distinct email_address, never_send_flag, bad_address_flag,undeliverable_date, fraud_flag,opt_down, first_promo_subscribe_date,subscribe_flag,last_subscribe_date,sweepstakes_entry_last_dt,credit_card_status,credit_card_create_date
    ,email_open_last_dt,last_email_send_date
    ,queens_english_flag
    ,ip_time_zone
    , pending_order_flag
    ,email_click_last_dt
    , activity_status
    ,customer_segment_exact
    from

    (select a.email_address,a.never_send_flag, a.bad_address_flag,a.undeliverable_date, a.fraud_flag,a.opt_down, a.first_promo_subscribe_date,a.subscribe_flag,a.last_subscribe_date,a.sweepstakes_entry_last_dt,a.credit_card_status,a.credit_card_create_date,a.email_open_last_dt
    ,email_click_last_dt
    ,a.queens_english_flag,a.ip_time_zone, a.pending_order_flag, last_email_send_date
    ,a.activity_status, a.customer_segment_exact
    from  
    (select distinct email_address, never_send_flag, bad_address_flag,undeliverable_date, fraud_flag,opt_down, first_promo_subscribe_date,subscribe_flag,last_subscribe_date,sweepstakes_entry_last_dt,credit_card_status,credit_card_create_date
    ,email_open_last_dt,last_email_send_date
    ,queens_english_flag,ip_time_zone, pending_order_flag
    ,email_click_last_dt
    , activity_status,customer_segment_exact
    from flags
    full outer join 
    (select email_address, undeliverable_date from undeliver_date) 
    using (email_address)
    full outer join
    (select email_address, fraud_flag from fraud) 
    using (email_address)
    full outer join
    (select email_address, opt_down from opt)
    using (email_address)
    full outer join
    (select email_address,first_promo_subscribe_date from promo_date)
    using (email_address)
    full outer join
    (select email_address,subscribe_flag, last_subscribe_date from subscribe)
    using(email_address)
    full outer join
    (select email_address, sweepstakes_entry_last_dt from  sweepstakes)
    using(email_address)
    full outer join
    (select email_address, credit_card_create_date, credit_card_status from credit_card_details)
    using(email_address)
    full outer join
    (select email_address, email_open_last_dt from email_metrics_data_fields)
    using(email_address)
    full outer join
    (select email_address,queens_english_flag  from queenflag)
    using(email_address)
    full outer join
    (select email_address, ip_time_zone from iptime)
    using(email_address)
    full outer join
    (select email_address,pending_order_flag from pending)
    using(email_address)
    full outer join
    (select email_address,last_email_send_date from email_metrics_data_fields)
    using(email_address)
     full outer join
    (select email_address,email_click_last_dt from email_metrics_data_fields)
     using(email_address)
     full outer join
     (select email_address,activity_status from  activity)
     using(email_address)
	 full outer join
     (select email_address,customer_segment_exact from  customer_segment)
     using(email_address)

    where email_address is not null
    and coalesce(trim(email_address),'9999') not in('-')
    and coalesce(trim(email_address),'@') like '%@%' and coalesce(trim(email_address),'.') like '%.%'
                and coalesce(trim(email_address),'9999') not like '%jminsure.com'
                and coalesce(trim(email_address),'9999') not like 'apayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like 'cntmpayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like '%test%bluenile%'
                and coalesce(trim(email_address),'9999') not like '%@test.com'
                and coalesce(trim(email_address),'9999') not in ('noemail@noemail.com') ) a )  )aa
                 left join 

    (select up_email_address_key,bnid,email_address from 
    `bnile-cdw-prod.dag_migration_test.up_customer_attributes`
     where email_address is not null) b
    on aa.email_address=b.email_address
    where bnid is not null

               );




    --- Query for explicit_attributes ----


    CREATE OR REPLACE TABLE `bnile-cdw-prod.dag_migration_test.up_explicit_attributes` as
    (
    select distinct
    up_email_address_key
    ,bnid
    ,customer_account_id
    ,account_create_date
    ,account_create_source
    ,last_account_login_date,wish_list_flag,account_holder_flag
     --first_name,a.gender, IFNULL(a.derived_gender,'U') derived_gender,
    , birth_date
	,birth_date_source
    ,birth_date_timestamp
    , email_domain_name
    ,primary_dma_zip
    ,primary_dma_state
    ,primary_dma_name
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_1_distance_miles
    ,nearest_store_2_webroom_key
    ,nearest_store_2_distance_miles
    ,nearest_store_3_webroom_key
    ,nearest_store_3_distance_miles
    ,primary_site
    ,primary_hostname
    ,subscribed_site
    ,subscribed_hostname  
    ,primary_currency_code
    ,primary_language_code
    ,marital_status
    ,wedding_date
	,wedding_date_source
    ,wedding_date_timestamp
	,anniversary_month
    ,anniversary_month_source
    ,anniversary_month_timestamp
    ,email_address_test_key
    ,bn_test_key
    ,region
    ,marital_status_source      
    ,marital_status_timestamp
    ,ifnull(activity_status, 'Lapsed') activity_status
    ,IFNULL(never_send_flag,'N') never_send_flag
    ,first_promo_subscribe_date
    ,IFNULL(subscribe_flag,'N') subscribe_flag
    ,IFNULL(fraud_flag,'N') fraud_flag
    ,IFNULL(bad_address_flag,'N')  bad_address_flag
    ,undeliverable_date    
    ,email_open_last_dt
    ,last_email_send_date
    ,email_click_last_dt   
    ,last_subscribe_date   
    ,ifnull(credit_card_status,'Prospect') credit_card_status
    ,credit_card_create_date
    ,IFNULL(queens_english_flag,'N')  queens_english_flag
    ,opt_down
    ,ip_time_zone
    ,IFNULL(pending_order_flag,'N') pending_order_flag
    ,sweepstakes_entry_last_dt
    ,IFNULL(order_cnt,0) order_cnt
    ,IFNULL(order_amt,0) order_amt
    ,order_last_dt
    ,IFNULL(order_engagement_cnt,0) order_engagement_cnt
    ,IFNULL(order_engagement_amt,0) order_engagement_amt
    ,IFNULL(order_engagement_avg_amt,0) order_engagement_avg_amt
    ,order_engagement_last_dt
    ,order_engagement_first_dt
    ,IFNULL(order_engagement_ring_cnt,0) order_engagement_ring_cnt
    ,order_engagement_ring_last_dt
    ,IFNULL(order_jewelry_cnt,0) order_jewelry_cnt
    ,IFNULL(order_jewelry_amt,0) order_jewelry_amt
    ,IFNULL(order_jewelry_avg_amt,0) order_jewelry_avg_amt
    ,order_jewelry_last_dt
    ,order_wedding_band_cnt
    ,order_wedding_band_last_dt
	,order_wedding_band_amt
	,order_engagement_ring_amt
    ,IFNULL(browse_cnt,0) browse_cnt
    ,IFNULL(browse_engagement_cnt,0) browse_engagement_cnt
    ,browse_engagement_last_dt
    ,browse_engagement_first_dt
    ,IFNULL(browse_jewelry_cnt,0) browse_jewelry_cnt
    ,browse_jewelry_first_dt
    ,browse_jewelry_last_dt
    ,browse_first_dt
    ,browse_last_dt        
    ,IFNULL(browse_wedding_band_cnt,0) browse_wedding_band_cnt
    ,browse_wedding_band_last_dt
    ,browse_wedding_band_first_dt
	,browse_diamond_last_dt
    ,proposal_date
	,customer_segment_exact
      from
    (
    select 
     up_email_address_key 
     ,bnid
    ,customer_account_id
    ,account_create_date
    ,account_create_source
    ,last_account_login_date
    ,IFNULL(wish_list_flag,'N') wish_list_flag
    ,IFNULL (account_holder_flag,'N') account_holder_flag
    --,first_name
    --,gender
    --,IFNULL(derived_gender,'U') derived_gender
    ,birth_date
    ,birth_date_source
    ,birth_date_timestamp
    ,coalesce(REGEXP_REPLACE(email_address,'.*@','')) as email_domain_name
    ,primary_dma_zip
    ,primary_dma_state
    ,primary_dma_name
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_1_distance_miles
    ,nearest_store_2_webroom_key
    ,nearest_store_2_distance_miles
    ,nearest_store_3_webroom_key
    ,nearest_store_3_distance_miles
    ,primary_site
    ,primary_hostname
    ,subscribed_site
    ,subscribed_hostname  
    ,primary_currency_code
    ,primary_language_code
    ,marital_status
    ,wedding_date
	,wedding_date_source
    ,wedding_date_timestamp
	,anniversary_month
    ,anniversary_month_source
    ,anniversary_month_timestamp
    ,email_address_test_key
    ,bn_test_key
    ,region
    ,marital_status_source      
    ,cast(marital_status_timestamp as date) marital_status_timestamp
    ,IFNULL(order_cnt,0) order_cnt
    ,IFNULL(order_amt,0) order_amt
    ,order_last_dt
    ,IFNULL(order_engagement_cnt,0) order_engagement_cnt
    ,IFNULL(order_engagement_amt,0) order_engagement_amt
    ,IFNULL(order_engagement_avg_amt,0) order_engagement_avg_amt
    ,order_engagement_last_dt
    ,order_engagement_first_dt
    ,IFNULL(order_engagement_ring_cnt,0) order_engagement_ring_cnt
    ,order_engagement_ring_last_dt
    ,IFNULL(order_jewelry_cnt,0) order_jewelry_cnt
    ,IFNULL(order_jewelry_amt,0) order_jewelry_amt
    ,IFNULL(order_jewelry_avg_amt,0) order_jewelry_avg_amt
    ,order_jewelry_last_dt
    ,order_wedding_band_cnt
    ,order_wedding_band_last_dt
	,order_wedding_band_amt
	,order_engagement_ring_amt
    ,IFNULL(never_send_flag,'N') never_send_flag
    ,cast(first_promo_subscribe_date as date) first_promo_subscribe_date
    ,IFNULL(subscribe_flag,'N') subscribe_flag
    ,IFNULL(fraud_flag,'N') fraud_flag
    ,IFNULL(bad_address_flag,'N')  bad_address_flag
    ,cast(undeliverable_date as date)   undeliverable_date
    ,email_open_last_dt
    ,last_email_send_date 
    ,email_click_last_dt   
    ,cast(last_subscribe_date as date) last_subscribe_date 
    ,ifnull(credit_card_status,'Prospect') credit_card_status
    ,cast(credit_card_create_date as date) credit_card_create_date
    ,IFNULL(queens_english_flag,'N')  queens_english_flag
    ,opt_down
    ,ip_time_zone
    ,IFNULL(pending_order_flag,'N') pending_order_flag
    ,cast(sweepstakes_entry_last_dt as date) sweepstakes_entry_last_dt
    ,IFNULL(browse_cnt,0) browse_cnt
    ,IFNULL(browse_engagement_cnt,0) browse_engagement_cnt
    ,browse_engagement_last_dt
    ,browse_engagement_first_dt
    ,IFNULL(browse_jewelry_cnt,0) browse_jewelry_cnt
    ,browse_jewelry_first_dt
    ,browse_jewelry_last_dt
    ,browse_first_dt
    ,browse_last_dt        
    ,IFNULL(browse_wedding_band_cnt,0) browse_wedding_band_cnt
    ,browse_wedding_band_last_dt
    ,browse_wedding_band_first_dt
	,browse_diamond_last_dt
    ,cast(proposal_date as date) proposal_date
    ,ifnull(activity_status, 'Lapsed') activity_status
	,ifnull(customer_segment_exact,'Other') customer_segment_exact

    from 
    (
    select 
    coalesce(p.up_email_address_key,o.up_email_address_key,f.up_email_address_key,b.up_email_address_key) as up_email_address_key
    ,coalesce(p.bnid,o.bnid,f.bnid,b.bnid) as bnid
    ,coalesce(p.email_address,o.email_address,f.email_address,b.email_address) as email_address
    ,customer_account_id
    ,account_create_date
    ,account_create_source
    ,last_account_login_date
    ,wish_list_flag
    ,account_holder_flag
    --,first_name
    --,gender
    ,birth_date
	,birth_date_source
    ,birth_date_timestamp
    ,primary_dma_zip
    ,primary_dma_state
    ,primary_dma_name
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_1_distance_miles
    ,nearest_store_2_webroom_key
    ,nearest_store_2_distance_miles
    ,nearest_store_3_webroom_key
    ,nearest_store_3_distance_miles
    ,primary_site
    ,primary_hostname
    ,subscribed_site
    ,subscribed_hostname
    ,primary_currency_code
    ,primary_language_code
    --,derived_gender
    ,marital_status
    ,wedding_date
	,wedding_date_source
    ,wedding_date_timestamp
	,anniversary_month
    ,anniversary_month_source
    ,anniversary_month_timestamp
    ,email_address_test_key
    ,bn_test_key
    ,region
    ,marital_status_source      
    ,marital_status_timestamp
    ,order_cnt
    ,order_amt
    ,order_last_dt
    ,order_engagement_cnt
    ,order_engagement_amt
    ,order_engagement_avg_amt
    ,order_engagement_last_dt
    ,order_engagement_first_dt
    ,order_engagement_ring_cnt
    ,order_engagement_ring_last_dt
    ,order_jewelry_cnt
    ,order_jewelry_amt
    ,order_jewelry_avg_amt
    ,order_jewelry_last_dt
    ,order_wedding_band_cnt
    ,order_wedding_band_last_dt
	,order_wedding_band_amt
	,ORDER_ENGAGEMENT_RING_AMT
    ,never_send_flag
    ,first_promo_subscribe_date
    ,subscribe_flag
    ,fraud_flag
    ,bad_address_flag
    ,undeliverable_date    
    ,email_open_last_dt
    ,last_email_send_date
    ,email_click_last_dt   
    ,last_subscribe_date   
    ,credit_card_status
    ,credit_card_create_date
    ,queens_english_flag
    ,opt_down
    ,ip_time_zone
    ,pending_order_flag
    ,sweepstakes_entry_last_dt
    ,browse_cnt
    ,browse_engagement_cnt
    ,browse_engagement_last_dt
    ,browse_engagement_first_dt
    ,browse_jewelry_cnt
    ,browse_jewelry_first_dt
    ,browse_jewelry_last_dt
    ,browse_first_dt
    ,browse_last_dt        
    ,browse_wedding_band_cnt
    ,browse_wedding_band_last_dt
    ,browse_wedding_band_first_dt
	,BROWSE_DIAMOND_LAST_DT
    ,activity_status
	,customer_segment_exact
    ,proposal_date


    from

    ((SELECT
    up_email_address_key
    ,bnid
    ,email_address
    ,customer_account_id
    ,account_create_date
    ,account_create_source
    ,last_account_login_date
    ,wish_list_flag
    ,account_holder_flag
    --,first_name
    --,gender
    ,birth_date
	,birth_date_source
    ,birth_date_timestamp
    ,primary_dma_zip
    ,primary_dma_state
    ,primary_dma_name
    ,primary_dma_source
    ,nearest_store_1_webroom_key
    ,nearest_store_1_distance_miles
    ,nearest_store_2_webroom_key
    ,nearest_store_2_distance_miles
    ,nearest_store_3_webroom_key
    ,nearest_store_3_distance_miles
    ,primary_site
    ,primary_hostname
    ,subscribed_site
    ,subscribed_hostname
    ,primary_currency_code
    ,primary_language_code
    --,derived_gender
    ,marital_status
    ,wedding_date
	,wedding_date_source
    ,wedding_date_timestamp
	,anniversary_month
    ,anniversary_month_source
    ,anniversary_month_timestamp
    ,email_address_test_key
    ,bn_test_key
    ,region
    ,marital_status_source      
    ,marital_status_timestamp
    ,proposal_date
    FROM `bnile-cdw-prod.dag_migration_test.stg_pii_attributes`) p 

    FULL OUTER JOIN

    (SELECT 
    up_email_address_key
    ,bnid
    ,email_address
    ,order_cnt
    ,order_amt
    ,order_last_dt
    ,order_engagement_cnt
    ,order_engagement_amt
    ,order_engagement_avg_amt
    ,order_engagement_last_dt
    ,order_engagement_first_dt
    ,order_engagement_ring_cnt
    ,order_engagement_ring_last_dt
    ,order_jewelry_cnt
    ,order_jewelry_amt
    ,order_jewelry_avg_amt
    ,order_jewelry_last_dt
    ,order_wedding_band_cnt
    ,order_wedding_band_last_dt
    ,order_wedding_band_amt
    ,ORDER_ENGAGEMENT_RING_AMT

    FROM `bnile-cdw-prod.dag_migration_test.stg_order_attributes`) o

    on o.email_address=p.email_address 

    FULL OUTER JOIN

    (SELECT 
    up_email_address_key
    ,bnid
    ,email_address
    ,never_send_flag
    ,first_promo_subscribe_date
    ,subscribe_flag
    ,fraud_flag
    ,bad_address_flag
    ,undeliverable_date 
    ,email_open_last_dt
    ,last_email_send_date
    ,email_click_last_dt
    ,last_subscribe_date
    ,credit_card_status
    ,credit_card_create_date
    ,queens_english_flag
    ,opt_down
    ,ip_time_zone
    ,pending_order_flag
    ,sweepstakes_entry_last_dt
    ,customer_segment_exact
    ,activity_status

    FROM `bnile-cdw-prod.dag_migration_test.stg_flag_attributes`) f
    on p.email_address=f.email_address



    FULL OUTER JOIN

    (SELECT
    up_email_address_key
    ,bnid
    ,email_address
    ,browse_cnt
    ,browse_engagement_cnt
    ,browse_engagement_last_dt
    ,browse_engagement_first_dt
    ,browse_jewelry_cnt
    ,browse_jewelry_first_dt
    ,browse_jewelry_last_dt
    ,browse_first_dt
    ,browse_last_dt        
    ,browse_wedding_band_cnt
    ,browse_wedding_band_last_dt
    ,browse_wedding_band_first_dt
	,BROWSE_DIAMOND_LAST_DT

    FROM `bnile-cdw-prod.dag_migration_test.stg_browse_attributes`) b

    on p.email_address=b.email_address

    ))aa
    where 
     coalesce(trim(aa.email_address),'9999') not in('-') 
    and coalesce(trim(aa.email_address),'@') like '%@%' and coalesce(trim(aa.email_address),'.') like '%.%'
                and coalesce(trim(aa.email_address),'9999') not like '%jminsure.com'
                and coalesce(trim(aa.email_address),'9999') not like 'apayment@bluenile.com' 
                and coalesce(trim(aa.email_address),'9999') not like 'cntmpayment@bluenile.com'
                and coalesce(trim(aa.email_address),'9999') not like '%test%bluenile%'
                and coalesce(trim(aa.email_address),'9999') not like '%@test.com'
                and coalesce(trim(aa.email_address),'9999') not in ('noemail@noemail.com')
                and coalesce(trim(aa.email_address),'9999') not like '%@removed.com'
    and REGEXP_CONTAINS ( aa.email_address,r"^[a-zA-Z0-9._+%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$") 
           and  LENGTH (aa.email_address) < 100
            and aa.email_address NOT LIKE 'testsun%@bluenile.com'
            and aa.email_address NOT LIKE 'testmon%@bluenile.com'
            and aa.email_address NOT LIKE 'testtue%@bluenile.com'
            and aa.email_address NOT LIKE 'testwed%@bluenile.com'
            and aa.email_address NOT LIKE 'testthu%@bluenile.com'
            and aa.email_address NOT LIKE 'testfri%@bluenile.com'
            and aa.email_address NOT LIKE 'testsat%@bluenile.com'
            and aa.email_address NOT LIKE '%.con'
            and aa.email_address NOT LIKE '%.ccom'
            and aa.email_address NOT LIKE '%.come'
            and aa.email_address NOT LIKE '%.cpm'
            and aa.email_address NOT LIKE '%.dom'
            and aa.email_address NOT LIKE '%.ccom'
            and aa.email_address NOT LIKE '%.comm'
            and aa.email_address NOT LIKE '%.cim'
            and aa.email_address NOT LIKE '%.xom'
            and aa.email_address NOT LIKE '%..%' 
            and aa.email_address NOT LIKE '%@.%'
    ) a
    where bnid is not null
    );
	'''
    return bql