def bnid_generation_preprocess():
    bql = '''
        ---- Query for master_event_stage ---------

    drop table if exists `bnile-cdw-prod.up_stg_customer.master_event_stage`;
    CREATE TABLE IF NOT EXISTS
    `bnile-cdw-prod.up_stg_customer.master_event_stage` as
    (
    select
    event_type
    ,basket_id
    ,customer_id
    ,first_name
    ,middle_name
    ,last_name
    ,gender
    ,birthday
    ,customer_guid
    ,email_address
    ,phone_number
    ,last_4_digits
    ,bill_to_street_address_1
    ,bill_to_street_address_2
    ,bill_to_street_address_3
    ,bill_to_country_name
    ,bill_to_country_code
    ,bill_to_city
    ,bill_to_state
    ,bill_to_postal_code
    ,ship_to_street_address_1
    ,ship_to_street_address_2
    ,ship_to_street_address_3
    ,ship_to_country_name
    ,ship_to_country_code
    ,ship_to_city
    ,ship_to_state
    ,ship_to_postal_code
    ,cast(coalesce(change_date, (cast(create_date as string))) as timestamp) as event_date
    ,v_to_v_flag
    from
    (
    (
    select 
    event_type
    ,basket_id
    ,customer_id
    ,first_name
    ,middle_name
    ,last_name
    ,gender
    ,birthday
    ,customer_guid
    ,email_address
    ,phone_number
    ,null as last_4_digits
    ,null  as bill_to_street_address_1
    ,null  as bill_to_street_address_2
    ,null  as bill_to_street_address_3
    ,null  as bill_to_country_name
    ,null as bill_to_country_code
    ,null as bill_to_city
    ,null as bill_to_state
    ,null  as bill_to_postal_code
    ,null  as ship_to_street_address_1
    ,null  as ship_to_street_address_2
    ,null  as ship_to_street_address_3
    ,null  as ship_to_country_name
    ,null  as ship_to_country_code
    ,null  as ship_to_city
    ,null  as ship_to_state
    ,null as ship_to_postal_code
    ,create_date
    ,change_date
    ,null as v_to_v_flag
    from

    (

    (
    select 
    event_type,
    basket_id,
    first_value(customer_id) over (partition by email_address order by  case when customer_id is not null then 1 else 0 end desc ,
    change_date  desc,create_date desc, customer_id ) as  customer_id,
     first_value(first_name) over (partition by email_address order by  case when first_name is not null then 1 else 0 end desc ,
     change_date  desc,create_date desc, first_name ) as  first_name,
     first_value(middle_name) over (partition by email_address order by  case when middle_name is not null then 1 else 0 end desc ,
     change_date  desc,create_date desc, middle_name ) as  middle_name,
     first_value(last_name) over (partition by email_address order by  case when last_name is not null then 1 else 0 end desc ,
     change_date  desc,create_date desc, last_name ) as  last_name,
    first_value(gender) over (partition by email_address order by  case when gender is not null then 1 else 0 end desc ,change_date  desc,
    create_date desc, gender ) as  gender,
    first_value(birthday) over (partition by email_address order by  case when birthday is not null then 1 else 0 end desc ,change_date  desc,
    create_date desc, birthday ) as  birthday,
    customer_guid,
    email_address,
    phone_number,
    create_date,
    change_date,

    from
    (select
    event_type
    ,cast(null as string) as basket_id
    ,cast(cus.customer_id as string) as customer_id
    ,cus.first_name
    ,cus.middle_name
    ,cus.last_name
    ,cus.gender
    ,cus.birthday
    ,cusguid.customer_guid
    ,REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(ced.email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com')
    ,'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com') email_address
    ,cast (null as string) as  phone_number
    ,coalesce(cus.create_date,ced.create_date,cusguid.create_date) as create_date
    ,cast(coalesce(cus.change_date,ced.change_date,cusguid.change_date) as string) as change_date 


    from 
    (select
    'customer' as event_type
    ,customer_id
    ,first_name
    ,gender
    ,last_name
    ,middle_name
    ,birthday
    ,cast(create_date as timestamp) create_date
    ,change_date
    from
    `bnile-cdw-prod.nileprod_o_warehouse.customer`
    )cus


    full outer join


    (select


    customer_id
    ,customer_guid
    ,cast(create_date as timestamp) create_date
    ,change_date
    from
    `bnile-cdw-prod.nileprod_o_warehouse.customer_guid` aa
    )cusguid

    on cus.customer_id=cusguid.customer_id

    full outer join  

    (
    select
    customer_id
    ,trim(lower(replace(replace(email_address,'?','@'),'@devmail.bluenile.com',''))) as email_address
    ,cast(create_date as timestamp) create_date
    ,change_date
    from
    `bnile-cdw-prod.nileprod_o_warehouse.customer_email_address`
    )ced

    on cus.customer_id=ced.customer_id)
    where email_address is not null
    )


    UNION ALL


    (select
     event_type
    ,cast(basket_id as string) as basket_id 
    ,cast(customer_id as string) as customer_id
    ,first_name
    ,middle_name
    ,last_name
    ,null as gender
    ,null as birthday
    ,customer_guid
    ,email_address
    ,phone_number
    ,create_date
    ,cast(change_date as string)

    from

    (

    select 
    'email_subscription_change_log' as event_type
    ,e.basket_id
    ,cast (null as string) as first_name
    ,cast (null as string)  as middle_name
    ,cast (null as string) as last_name
    ,e.change_guid as customer_guid
    ,cast (null as string) as phone_number
    ,e.email_address
    ,null  as customer_id
    ,cast(e.create_date as timestamp) create_date
    ,null  as change_date
    from
    `bnile-cdw-prod.nileprod_o_warehouse.email_subscription_change_log`  e)
    )
    )

    )
    union all
    (
      select 
     event_type
    ,basket_id
    ,customer_id
    ,first_name
    ,middle_name
    ,last_name
    ,null as gender
    ,null as birthday
    ,customer_guid
    ,email_address
    ,phone_number
    ,null as last_4_digits
    ,null  as bill_to_street_address_1
    ,null  as bill_to_street_address_2
    ,null  as bill_to_street_address_3
    ,null  as bill_to_country_name
    ,null as bill_to_country_code
    ,null as bill_to_city
    ,null as bill_to_state
    ,null  as bill_to_postal_code
    ,null  as ship_to_street_address_1
    ,null  as ship_to_street_address_2
    ,null  as ship_to_street_address_3
    ,null  as ship_to_country_name
    ,null  as ship_to_country_code
    ,null  as ship_to_city
    ,null  as ship_to_state
    ,null as ship_to_postal_code
    ,safe_cast (create_date as timestamp) create_date
    ,change_date
    ,null as v_to_v_flag

    from
    (

    select distinct 
    'basket' as event_type,
    first_value(cast (customer_id as string)) over
    (partition by email_address order by case when customer_id is not null then 1 else 0 end desc, change_date desc,create_date desc,
    BASKET_COMPLETE_DATE desc, BASKET_STATUS_DATE desc, CUSTOMER_SUBMIT_DATE desc ) customer_id,
    cast (basket_id as string) basket_id,
    email_address,
    first_value(phone_number) over
    (partition by email_address order by case when phone_number is not null then 1 else 0 end desc, change_date desc,create_date desc,
    BASKET_COMPLETE_DATE desc, BASKET_STATUS_DATE desc, CUSTOMER_SUBMIT_DATE desc,phone_number ) phone_number,
    customer_guid,
    first_value(first_name) over
    (partition by email_address order by case when first_name is not null then 1 else 0 end desc, change_date desc,create_date desc,
    BASKET_COMPLETE_DATE desc, BASKET_STATUS_DATE desc, CUSTOMER_SUBMIT_DATE desc ) first_name,
    first_value(last_name) over
    (partition by email_address order by case when last_name is not null then 1 else 0 end desc, change_date desc,create_date desc,
    BASKET_COMPLETE_DATE desc, BASKET_STATUS_DATE desc, CUSTOMER_SUBMIT_DATE desc ) last_name,
    middle_name,
    cast (change_date as string) change_date ,
    create_date
    from `bnile-cdw-prod.nileprod_o_warehouse.basket`
    where  email_address is not null 

    union all

    select distinct 
    'basket' as event_type,
    first_value(cast (customer_id as string)) over
    (partition by phone_number order by case when customer_id is not null then 1 else 0 end desc, change_date desc,create_date desc,
    BASKET_COMPLETE_DATE desc, BASKET_STATUS_DATE desc, CUSTOMER_SUBMIT_DATE desc ) customer_id,
    cast (basket_id as string) basket_id,
    cast (Null as string) as email_address,
    phone_number,
    customer_guid,
    first_value(first_name) over
    (partition by phone_number order by case when first_name is not null then 1 else 0 end desc, change_date desc,create_date desc,
    BASKET_COMPLETE_DATE desc, BASKET_STATUS_DATE desc, CUSTOMER_SUBMIT_DATE desc ) first_name,
    first_value(last_name) over
    (partition by phone_number order by case when last_name is not null then 1 else 0 end desc, change_date desc,create_date desc,
    BASKET_COMPLETE_DATE desc, BASKET_STATUS_DATE desc, CUSTOMER_SUBMIT_DATE desc ) last_name,
     middle_name,
    cast (change_date as string) change_date ,
    create_date
    from `bnile-cdw-prod.nileprod_o_warehouse.basket`
    where  phone_number is not null and email_address is null
    ) )

    UNION ALL
        (with orders_data as
    (select
    'orders' as event_type
    ,ead.email_address
    ,cast (pof.basket_id as string) basket_id	
    ,pof.first_name as first_name
    ,last_4_digits
    ,bad.Phone_number
    ,bad.bill_to_street_address_1
    ,bad.bill_to_street_address_2
    ,bad.bill_to_street_address_3
    ,bad.bill_to_country_name
    ,bad.bill_to_country_code
    ,bad.bill_to_city
    ,bad.bill_to_state
    ,bad.bill_to_postal_code
    ,sad.ship_to_street_address_1
    ,sad.ship_to_street_address_2
    ,sad.ship_to_street_address_3
    ,sad.ship_to_country_name
    ,sad.ship_to_country_code
    ,sad.ship_to_city
    ,sad.ship_to_state
    ,sad.ship_to_postal_code
    ,pof.ORDER_DATE_KEY
    ,pof.data_capture_timestamp_utc
    ,cast (pof.create_date as timestamp) as create_date	
    ,cast(pof.change_date as timestamp) as change_date
    ,pof.v_to_v_flag
    from

    (select 
     email_address_key
    ,first_name
    ,basket_id
    ,bill_to_address_key
    ,ship_to_address_key
    ,aa.create_date as change_date
    ,aa.last_update_date as create_date
    ,aa.ORDER_DATE_KEY 
    ,aa.data_capture_timestamp_utc
    ,case when sd.advertiser_name = 'Vendor to Vendor Sales' then 'Y' else null end as v_to_v_flag
    from `bnile-cdw-prod.dssprod_o_warehouse.product_order_fact` aa
    left join `bnile-cdw-prod.dssprod_o_warehouse.source_dim` sd on
    aa.source_key=sd.source_key 
    )pof

    FULL OUTER JOIN 

    (select 
     REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com')
    ,'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com') as email_address
    ,email_address_key
    from `bnile-cdw-prod.dssprod_o_warehouse.email_address_dim`) ead

    on ead.email_address_key=pof.email_address_key

    FULL OUTER JOIN 

    (select phone_number
    ,address_key as bill_key
    ,street_address_1 as bill_to_street_address_1
    ,street_address_2 as bill_to_street_address_2
    ,street_address_3 as bill_to_street_address_3
    ,country_name as bill_to_country_name
    ,country_code as bill_to_country_code
    ,city as bill_to_city
    ,state as bill_to_state
    ,postal_code as bill_to_postal_code
    from `bnile-cdw-prod.dssprod_o_warehouse.address_dim` ) bad

    on pof.bill_to_address_key=bad.bill_key

    FULL OUTER JOIN 

    (
    select
    address_key as ship_key
    ,street_address_1 as ship_to_street_address_1
    ,street_address_2 as ship_to_street_address_2
    ,street_address_3 as ship_to_street_address_3
    ,country_name as ship_to_country_name
    ,country_code as ship_to_country_code
    ,city as ship_to_city
    ,state as ship_to_state
    ,postal_code as ship_to_postal_code
    from `bnile-cdw-prod.dssprod_o_warehouse.address_dim` ) sad

    on pof.ship_to_address_key=sad.ship_key

    FULL OUTER JOIN 

    (select 
    basket_id
    ,cast(last_4_digits as string) as last_4_digits
    from `bnile-cdw-prod.dssprod_o_warehouse.payment_fact`)pf

    on  pof.basket_id =pf.basket_id )	


    select 
    event_type
    ,basket_id
    ,cast (null  as string) customer_id
    ,first_name
    ,cast (null as string)  as middle_name
    ,cast(null as string)  as last_name
    ,cast(null as string) as  gender
    ,cast(null as string) as birthday
    ,cast(null as string) as customer_guid
    ,email_address
    ,phone_number
    ,last_4_digits
    ,bill_to_street_address_1
    ,bill_to_street_address_2
    ,bill_to_street_address_3
    ,bill_to_country_name
    ,bill_to_country_code
    ,bill_to_city
    ,bill_to_state
    ,bill_to_postal_code
    ,ship_to_street_address_1
    ,ship_to_street_address_2
    ,ship_to_street_address_3
    ,ship_to_country_name
    ,ship_to_country_code
    ,ship_to_city
    ,ship_to_state
    ,ship_to_postal_code
    ,create_date
    ,cast(change_date as string) change_date
    ,v_to_v_flag

    from
    (select 
    distinct 
    event_type,
    email_address,
    basket_id,

    first_value (first_name) over (partition by email_address order by case when first_name is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc, change_date desc, data_capture_timestamp_utc desc , first_name)  first_name,

    first_value (last_4_digits) over (partition by email_address order by case when last_4_digits is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc,change_date desc, data_capture_timestamp_utc desc , last_4_digits)  last_4_digits,

    first_value (Phone_number) over (partition by email_address order by case when Phone_number is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc,change_date desc, data_capture_timestamp_utc desc , Phone_number)  Phone_number,

    first_value (bill_to_street_address_1) over (partition by email_address order by case when bill_to_street_address_1 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , bill_to_street_address_1)  bill_to_street_address_1,

    first_value (bill_to_street_address_2) over (partition by email_address order by case when bill_to_street_address_2 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_street_address_2)  bill_to_street_address_2,

    first_value (bill_to_street_address_3) over (partition by email_address order by case when bill_to_street_address_3 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_street_address_3)  bill_to_street_address_3,

    first_value (bill_to_country_name) over (partition by email_address order by case when bill_to_country_name is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , bill_to_country_name)  bill_to_country_name,

    first_value (bill_to_country_code) over (partition by email_address order by case when bill_to_country_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_country_code)  bill_to_country_code,

    first_value (bill_to_city) over (partition by email_address order by case when bill_to_city is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_city)  bill_to_city,

    first_value (bill_to_state) over (partition by email_address order by case when bill_to_state is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc, change_date desc, data_capture_timestamp_utc desc , bill_to_state)  bill_to_state,

    first_value (bill_to_postal_code) over (partition by email_address order by case when bill_to_postal_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_postal_code)  bill_to_postal_code,

    first_value (ship_to_street_address_1) over (partition by email_address order by case when ship_to_street_address_1 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , ship_to_street_address_1)  ship_to_street_address_1,

    first_value (ship_to_street_address_2) over (partition by email_address order by case when ship_to_street_address_2 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_street_address_2)  ship_to_street_address_2,

    first_value (ship_to_street_address_3) over (partition by email_address order by case when ship_to_street_address_3 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_street_address_3)  ship_to_street_address_3,

    first_value (ship_to_country_name) over (partition by email_address order by case when ship_to_country_name is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , ship_to_country_name)  ship_to_country_name,

    first_value (ship_to_country_code) over ( partition by email_address order by case when ship_to_country_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_country_code)  ship_to_country_code,

    first_value (ship_to_city) over (partition by email_address order by case when ship_to_city is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_city)  ship_to_city,

    first_value (ship_to_state) over (partition by email_address order by case when ship_to_state is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc, change_date desc, data_capture_timestamp_utc desc , ship_to_state)  ship_to_state,

    first_value (ship_to_postal_code) over (partition by email_address order by case when ship_to_postal_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_postal_code)  ship_to_postal_code,

    create_date,
    change_date,
     v_to_v_flag

    from

    orders_data

    where email_address is not null

    union all
    (
    select 
    distinct 
    event_type,
    cast (Null as string)  as email_address,
    basket_id,

    first_value (first_name) over (partition by Phone_number order by case when first_name is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc,change_date desc, data_capture_timestamp_utc desc , first_name)  first_name,

    first_value (last_4_digits) over (partition by Phone_number order by case when last_4_digits is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc,change_date desc, data_capture_timestamp_utc desc , last_4_digits)  last_4_digits,

    Phone_number,

    first_value (bill_to_street_address_1) over (partition by Phone_number order by case when bill_to_street_address_1 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , bill_to_street_address_1)  bill_to_street_address_1,

    first_value (bill_to_street_address_2) over (partition by Phone_number order by case when bill_to_street_address_2 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_street_address_2)  bill_to_street_address_2,

    first_value (bill_to_street_address_3) over (partition by Phone_number order by case when bill_to_street_address_3 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_street_address_3)  bill_to_street_address_3,

    first_value (bill_to_country_name) over (partition by Phone_number order by case when bill_to_country_name is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , bill_to_country_name)  bill_to_country_name,

    first_value (bill_to_country_code) over (partition by Phone_number order by case when bill_to_country_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_country_code)  bill_to_country_code,

    first_value (bill_to_city) over (partition by Phone_number order by case when bill_to_city is not null then 1 else 0 end desc, ORDER_DATE_KEY desc,   
    create_date desc, change_date desc, data_capture_timestamp_utc desc , bill_to_city)  bill_to_city,

    first_value (bill_to_state) over (partition by Phone_number order by case when bill_to_state is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc, change_date desc, data_capture_timestamp_utc desc , bill_to_state)  bill_to_state,

    first_value (bill_to_postal_code) over (partition by Phone_number order by case when bill_to_postal_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , bill_to_postal_code)  bill_to_postal_code,

    first_value (ship_to_street_address_1) over (partition by Phone_number order by case when ship_to_street_address_1 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , ship_to_street_address_1)  ship_to_street_address_1,

    first_value (ship_to_street_address_2) over (partition by Phone_number order by case when ship_to_street_address_2 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_street_address_2)  ship_to_street_address_2,

    first_value (ship_to_street_address_3) over (partition by Phone_number order by case when ship_to_street_address_3 is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_street_address_3)  ship_to_street_address_3,

    first_value (ship_to_country_name) over (partition by Phone_number order by case when ship_to_country_name is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc, change_date desc, data_capture_timestamp_utc desc , ship_to_country_name)  ship_to_country_name,

    first_value (ship_to_country_code) over ( partition by Phone_number order by case when ship_to_country_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_country_code)  ship_to_country_code,

    first_value (ship_to_city) over (partition by Phone_number order by case when ship_to_city is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_city)  ship_to_city,

    first_value (ship_to_state) over (partition by Phone_number order by case when ship_to_state is not null then 1 else 0 end desc, ORDER_DATE_KEY desc, 
    create_date desc, change_date desc, data_capture_timestamp_utc desc , ship_to_state)  ship_to_state,

    first_value (ship_to_postal_code) over (partition by Phone_number order by case when ship_to_postal_code is not null then 1 else 0 end desc, 
    ORDER_DATE_KEY desc, create_date desc,change_date desc, data_capture_timestamp_utc desc , ship_to_postal_code)  ship_to_postal_code,

    create_date,
    change_date,
    v_to_v_flag

    from
    orders_data

    where email_address is null and phone_number is not null))
    )


    UNION ALL
    (
    select 
    event_type
    ,null as basket_id
    ,null as customer_id
    ,first_name
    ,null as middle_name
    ,last_name
    ,gender
    ,null as birthday
    ,null as customer_guid
    ,contact_email_address as email_address
    ,Case When Trim(mobile_phone) = '-' Then work_phone  Else mobile_phone End AS Phone_Number
    ,null as last_4_digits
    ,null as bill_to_street_address_1
    ,null as bill_to_street_address_2
    ,null as bill_to_street_address_3
    ,null as bill_to_country_name
    ,null as bill_to_country_code
    ,null as bill_to_city
    ,null as bill_to_state
    ,null as bill_to_postal_code
    ,null as ship_to_street_address_1
    ,null as ship_to_street_address_2
    ,null as ship_to_street_address_3
    ,ship_to_country  as ship_to_country_name
    ,null as ship_to_country_code
    ,null as ship_to_city
    ,null as ship_to_state
    ,null as ship_to_postal_code
    ,cast(create_date as timestamp) create_date
    ,cast (change_date as string) change_date
    ,null as v_to_v_flag
    from

    (select 
    'freshdesk_contact' as event_type
    ,REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(contact_email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com')
    ,'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com') as contact_email_address
	 ,SPLIT(contact_name,' ')[safe_OFFSET(0)] first_name
    ,case when array_length (split(contact_name,' ')) >1  then  reverse(SPLIT(reverse(contact_name),' ')[safe_OFFSET(0)]) 
    else null
    end last_name
    ,case  ship_to_country when '-' then Null else ship_to_country end ship_to_country
    ,case when gender in ('-','N/A') then Null else gender end gender
    ,case  mobile_phone when '-' then Null else   mobile_phone end mobile_phone
    ,case  work_phone when '-' then Null else   work_phone end work_phone
    ,create_date
    ,last_update_date as change_date
    from `bnile-cdw-prod.dssprod_o_customer.freshdesk_contact` fd
    where contact_email_address is not null
    ) ) 


    union all

     (
    select 
     event_type
    ,null as basket_id
    ,null as customer_id
    ,first_name
    ,null as middle_name
    ,last_name
    ,cast (null as string) as gender
    ,cast (null as string) birthday
    ,customer_guid
    ,email_address
    ,null as phone_number
    ,null as last_4_digits
    ,null as bill_to_street_address_1
    ,null  as bill_to_street_address_2
    ,null  as bill_to_street_address_3
    , null as bill_to_country_name
    ,null as bill_to_country_code
    ,null as bill_to_city
    ,null as bill_to_state
    ,null  as bill_to_postal_code
    ,null  as ship_to_street_address_1
    ,null  as ship_to_street_address_2
    ,null  as ship_to_street_address_3
    ,CUSTOMER_LOCATION_COUNTRY as ship_to_country_name
    ,null  as ship_to_country_code
    ,null  as ship_to_city
    ,CUSTOMER_LOCATION_STATE as ship_to_state
    ,null as ship_to_postal_code
    ,cast (create_date as timestamp) create_date
    ,cast (LAST_UPDATE_DATE as string) LAST_UPDATE_DATE
    ,null as v_to_v_flag

    from

    (SELECT 

    'freshdesk_ticket' as event_type,
    CONTACT_ID as email_address, 
    first_value (CUSTOMER_LOCATION_COUNTRY) over (partition by CONTACT_ID order by case when CUSTOMER_LOCATION_COUNTRY is not null then 1 else 0 end desc,
    LAST_UPDATE_DATE desc, create_date desc,  CREATED_TIME desc) CUSTOMER_LOCATION_COUNTRY ,
    first_value (CUSTOMER_LOCATION_STATE) over (partition by CONTACT_ID order by case when CUSTOMER_LOCATION_STATE is not null then 1 else 0 end desc, 
    LAST_UPDATE_DATE desc, create_date desc,  CREATED_TIME desc) CUSTOMER_LOCATION_STATE ,
    CUSTOMER_GUID,
    first_value (first_name) over (partition by CONTACT_ID order by case when first_name is not null then 1 else 0 end desc, LAST_UPDATE_DATE desc, 
    create_date desc,  CREATED_TIME desc, first_name) first_name ,
    first_value (last_name) over (partition by CONTACT_ID order by case when last_name is not null then 1 else 0 end desc, LAST_UPDATE_DATE desc, 
    create_date desc,  CREATED_TIME desc,last_name) last_name ,
    CREATE_DATE, 
    LAST_UPDATE_DATE

    FROM 
    (
    select 
    REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(CONTACT_ID)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com')
    ,'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com') as CONTACT_ID,
    case when CUSTOMER_LOCATION_COUNTRY like '-' then Null else CUSTOMER_LOCATION_COUNTRY end CUSTOMER_LOCATION_COUNTRY,
    case when CUSTOMER_LOCATION_STATE like '-' then Null else CUSTOMER_LOCATION_STATE end CUSTOMER_LOCATION_STATE,
    SPLIT(full_name,' ')[safe_OFFSET(0)] first_name,
    case when array_length (split(full_name,' ')) >1  
    then reverse(SPLIT(reverse( full_name),' ')[safe_OFFSET(0)]) 
    else null
    end last_name,
    CUSTOMER_GUID,
    LAST_UPDATE_DATE, 
    create_date, 
    case when regexp_contains (CREATED_TIME, r'/')  then safe.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', CREATED_TIME )
    when regexp_contains (CREATED_TIME, r'-')  then safe.PARSE_DATETIME('%Y-%m-%d %H:%M:%S',CREATED_TIME )
    end 
    CREATED_TIME,
    EXTERNAL_OR_INTERNAL_TICKET
    from `bnile-cdw-prod.dssprod_o_customer.freshdesk_ticket`
    where CONTACT_ID is not null
    and ifnull(EXTERNAL_OR_INTERNAL_TICKET, 'a') <>  'Internal Ticket (Blue Nile Communication))'
    )
    union distinct
    SELECT 
    'freshdesk_ticket' as event_type, 
    CUSTOMER_EMAIL_ADDRESS, 
    first_value (CUSTOMER_LOCATION_COUNTRY) over (partition by CUSTOMER_EMAIL_ADDRESS order by case when CUSTOMER_LOCATION_COUNTRY is not null then 1 else 0 end desc, 
    LAST_UPDATE_DATE desc, create_date desc,  CREATED_TIME desc) CUSTOMER_LOCATION_COUNTRY ,
    first_value (CUSTOMER_LOCATION_STATE) over (partition by CUSTOMER_EMAIL_ADDRESS order by case when CUSTOMER_LOCATION_STATE is not null then 1 else 0 end desc,  
    LAST_UPDATE_DATE desc, create_date desc,  CREATED_TIME desc) CUSTOMER_LOCATION_STATE ,
    CUSTOMER_GUID,
    first_value (first_name) over (partition by CUSTOMER_EMAIL_ADDRESS order by case when first_name is not null then 1 else 0 end desc, 
    LAST_UPDATE_DATE desc, create_date desc,  CREATED_TIME desc,first_name ) first_name ,
    first_value (last_name) over (partition by CUSTOMER_EMAIL_ADDRESS order by case when last_name is not null then 1 else 0 end desc, 
    LAST_UPDATE_DATE desc, create_date desc, CREATED_TIME desc, first_name) last_name ,
    CREATE_DATE, 
    LAST_UPDATE_DATE,
    FROM 
    (
    select 
     REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(CUSTOMER_EMAIL_ADDRESS)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com')
    ,'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com') as CUSTOMER_EMAIL_ADDRESS,
    case when CUSTOMER_LOCATION_COUNTRY like '-' then Null else CUSTOMER_LOCATION_COUNTRY end CUSTOMER_LOCATION_COUNTRY,
    case when CUSTOMER_LOCATION_STATE like '-' then Null else CUSTOMER_LOCATION_STATE end CUSTOMER_LOCATION_STATE,
    SPLIT(CUSTOMER_NAME ,' ')[safe_OFFSET(0)] first_name,
    case when array_length (split(CUSTOMER_NAME,' ')) >1  
    then  reverse(SPLIT(reverse( CUSTOMER_NAME),' ')[safe_OFFSET(0)]) 
    else null
    end as last_name,
    CUSTOMER_GUID,
    LAST_UPDATE_DATE, 
    create_date, 
    case when regexp_contains (CREATED_TIME, r'/')  then safe.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', CREATED_TIME )
    when regexp_contains (CREATED_TIME, r'-')  then safe.PARSE_DATETIME('%Y-%m-%d %H:%M:%S',CREATED_TIME )
    end 
    CREATED_TIME,
    from `bnile-cdw-prod.dssprod_o_customer.freshdesk_ticket`
    where CUSTOMER_EMAIL_ADDRESS is not null
    and ifnull(EXTERNAL_OR_INTERNAL_TICKET, 'a') <>  'Internal Ticket (Blue Nile Communication))'
    )
     )
    )



    UNION ALL
    (select 
    'site_event' as event_type
    ,null as basket_id
    ,null as customer_id
    ,CUSTOMER_FIRST_NAME
    ,Null as CUSTOMER_MIDDLE_NAME
    ,CUSTOMER_LAST_NAME
    ,CUSTOMER_GENDER
    ,null as birthday
    ,event_guid

    ,email_address
    ,CUSTOMER_PHONE_NUMBER
    ,null as last_4_digits
    ,null as bill_to_street_address_1	
    ,null as bill_to_street_address_2	
    ,null as bill_to_street_address_3
    ,null as bill_to_country_name
    ,null as bill_to_country_code
    ,null as bill_to_city	
    ,null as bill_to_state	
    , null as bill_to_postal_code
    ,MAILING_ADDR_STREET_1
    ,MAILING_ADDR_STREET_2
    ,MAILING_ADDR_STREET_3
    ,MAILING_ADDR_COUNTRY
    ,ship_to_country_code
    ,MAILING_ADDR_CITY
    ,MAILING_ADDR_STATE
    ,MAILING_ADDR_ZIP
    ,cast (event_date as timestamp) event_date	
    ,CAST(EVENT_DATE AS STRING) AS EVENT_DATE_1
    ,null as v_to_v_flag

    from

    (
    select 
    distinct
    lower(trim(email_address)) email_address,
    event_guid,
    first_value (customer_first_name) over ( partition by lower(trim(email_address)) order by case when customer_first_name is not null then 1 else 0 end desc, 
    event_date desc, data_capture_timestamp_utc desc, customer_first_name) customer_first_name,
    first_value (customer_last_name) over ( partition by lower(trim(email_address)) order by case when customer_last_name is not null then 1 else 0 end desc, event_date desc, 
    data_capture_timestamp_utc desc, customer_last_name) customer_last_name,
    first_value (customer_gender) over ( partition by lower(trim(email_address)) order by case when customer_gender is not null then 1 else 0 end desc, event_date desc, 
    data_capture_timestamp_utc desc, customer_gender) customer_gender,
    first_value (customer_phone_number) over ( partition by lower(trim(email_address)) order by case when customer_phone_number is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, customer_phone_number) customer_phone_number,
    first_value (MAILING_ADDR_STREET_1) over ( partition by lower(trim(email_address)) order by case when MAILING_ADDR_STREET_1 is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, MAILING_ADDR_STREET_1) MAILING_ADDR_STREET_1,
    first_value (MAILING_ADDR_STREET_2) over ( partition by lower(trim(email_address)) order by case when MAILING_ADDR_STREET_2 is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, MAILING_ADDR_STREET_2) MAILING_ADDR_STREET_2,
    first_value (MAILING_ADDR_STREET_3) over ( partition by lower(trim(email_address)) order by case when MAILING_ADDR_STREET_3 is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, MAILING_ADDR_STREET_3) MAILING_ADDR_STREET_3,
    first_value (MAILING_ADDR_CITY) over ( partition by lower(trim(email_address)) order by case when MAILING_ADDR_CITY is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, MAILING_ADDR_CITY) MAILING_ADDR_CITY,
    first_value (MAILING_ADDR_STATE) over ( partition by lower(trim(email_address)) order by case when MAILING_ADDR_STATE is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, MAILING_ADDR_STATE) MAILING_ADDR_STATE,
    first_value (MAILING_ADDR_COUNTRY) over ( partition by lower(trim(email_address)) order by case when MAILING_ADDR_COUNTRY is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, MAILING_ADDR_COUNTRY) MAILING_ADDR_COUNTRY,
    first_value (MAILING_ADDR_ZIP) over ( partition by lower(trim(email_address)) order by case when MAILING_ADDR_ZIP is not null then 1 else 0 end desc, event_date 
    desc,data_capture_timestamp_utc desc, MAILING_ADDR_ZIP ) MAILING_ADDR_ZIP,
    first_value(SHIP_TO_COUNTRY_CODE) over ( partition by lower(trim(email_address)) order by case when SHIP_TO_COUNTRY_CODE is not null then 1 else 0 end desc,
    EVENT_DATE desc, data_capture_timestamp_utc desc,  SHIP_TO_COUNTRY_CODE ) SHIP_TO_COUNTRY_CODE,
    event_date
    from
   ( 
    select * except(email_address),
    REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com')
    ,'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com')  as email_address
    from 
    `bnile-cdw-prod.nileprod_o_warehouse.site_event`
    where email_address is not null)

    )
    )
    union all

    (select 
    event_type
    ,null as basket_id
    ,null as customer_id
    ,null as CUSTOMER_FIRST_NAME
    ,null as CUSTOMER_MIDDLE_NAME
    ,null as CUSTOMER_LAST_NAME
    ,null as CUSTOMER_GENDER
    ,null as birthday
    ,guid
    ,email_address
    ,null as CUSTOMER_PHONE_NUMBER
    ,null as last_4_digits
    ,null as bill_to_street_address_1	
    ,null as bill_to_street_address_2	
    ,null as bill_to_street_address_3
    ,null as bill_to_country_name
    ,null as bill_to_country_code
    ,null as bill_to_city	
    ,null as bill_to_state	
    , null as bill_to_postal_code
    ,null as MAILING_ADDR_STREET_1
    ,null as MAILING_ADDR_STREET_2
    ,null as MAILING_ADDR_STREET_3
    ,null as MAILING_ADDR_COUNTRY
    ,null as ship_to_country_code
    ,null as MAILING_ADDR_CITY
    , null as MAILING_ADDR_STATE
    , null as MAILING_ADDR_ZIP
    ,CREATE_DATE
    ,change_date
    ,null as v_to_v_flag

    from

    (select 
    distinct
    'click_fact' as event_type,
    cf.email_address,
    cast(cf.CREATE_DATE as timestamp) CREATE_DATE , 
    cast (cf.LAST_UPDATE_DATE as string) change_date, 
    gcf.guid

    from
    (select distinct cf.guid_key,cf.email_address, cf.CREATE_DATE , cf.LAST_UPDATE_DATE  from `bnile-cdw-prod.dssprod_o_warehouse.click_fact`  cf)cf 
    inner join `bnile-cdw-prod.up_tgt_ora_tables.guid_dim_cf` gcf on gcf.guid_key=cf.guid_key)

    )

	 union all
	-- adding extra PII (by sethu 06/07/2021) --

  (select
	'customer_event' as event_type
	,cast(null as string) basket_id
	,cast(null as string) customer_id
	,cast(null as string) first_name
	,cast(null as string) middle_name
	,cast(null as string) last_name
	,first_value( gender) over (partition by email_address order by  case when gender is not null then 1 else 0 end desc, event_date desc, gender) gender
	,first_value( birthday) over (partition by email_address order by  case when birthday is not null then 1 else 0 end desc, event_date desc, birthday) as birthday
	,cast(null as string) customer_guid
	,email_address
	,cast(null as string) phone_number
	,cast(null as string) last_4_digits
	,cast(null as string) bill_to_street_address_1
	,cast(null as string) bill_to_street_address_2
	,cast(null as string) bill_to_street_address_3
	,cast(null as string) bill_to_country_name
	,cast(null as string) bill_to_country_code
	,cast(null as string) bill_to_city
	,cast(null as string) bill_to_state
	,cast(null as string) bill_to_postal_code
	,cast(null as string) as ship_to_street_address_1
	,cast(null as string) as ship_to_street_address_2
	,cast(null as string) as ship_to_street_address_3
	,cast(null as string) as ship_to_country_name
	,cast(null as string) as ship_to_country_code
	,cast(null as string) as ship_to_city
	,cast(null as string) as ship_to_state
	,first_value( postal_code) over (partition by email_address order by  case when postal_code is not null then 1 else 0 end desc, event_date desc, postal_code) as 
    ship_to_postal_code
	,timestamp(event_date) as create_date
	,cast(event_date as string) as change_date
	,cast(null as string) as v_to_v_flag
  from

  (select
	  gender
	,FORMAT_DATE("%m/%d/%Y",  date(birthday)) as birthday
	,email_address
	,case when postal_code like '-' then Null else postal_code end postal_code
  ,timestamp(event_date) as event_date
	from
	`bnile-cdw-prod.up_stg_customer.stg_customer_event_fact`
	where email_address is not null and (gender is not null or postal_code is not null or birthday is not null)) 
  )

	union all

	(select
	'email_survey' as event_type
	,cast(basket_id as string) as basket_id
	,cast(null as string) as customer_id
	,cast(null as string) as first_name
	,cast(null as string) as middle_name
	,cast(null as string) as last_name
	,first_value(gender) over (partition by email_address order by case when gender is not null then 1 else 0 end desc, event_date desc, gender) gender
	,first_value(FORMAT_DATE("%m/%d/%Y",  date(birth_date)) )
	over (partition by email_address order by case when birth_date is not null then 1 else 0 end desc, event_date desc, birth_date)  birthday
	,cast(null as string) as customer_guid
	,email_address
	,cast(null as string) as phone_number
	,cast(null as string) as last_4_digits
	,cast(null as string) as bill_to_street_address_1
	,cast(null as string) as bill_to_street_address_2
	,cast(null as string) as bill_to_street_address_3
	,cast(null as string) as bill_to_country_name
	,cast(null as string) as bill_to_country_code
	,cast(null as string) as bill_to_city
	,cast(null as string) as bill_to_state
	,cast(null as string) as bill_to_postal_code
	,cast(null as string) as ship_to_street_address_1
	,cast(null as string) as ship_to_street_address_2
	,cast(null as string) as ship_to_street_address_3
	,cast(null as string) as ship_to_country_name
	,cast(null as string) as ship_to_country_code
	,cast(null as string) as ship_to_city
	,cast(null as string) as ship_to_state
	,cast(null as string) as ship_to_postal_code
	,timestamp(event_date) as create_date
	,cast(event_date as string) as change_date
	,cast(null as string) as v_to_v_flag
	from
	`bnile-cdw-prod.up_stg_customer.stg_email_survey_summary`
	where email_address is not null and (gender is not null   or birth_date  is not null))

	union all

	(
	 select
	'backbone' as event_type
	,null as basket_id
	,null as customer_id
	,null as first_name
	,null as middle_name
	,null as last_name
	,gender
	,birth_date as birthday
	,null as customer_guid
	,email_address
	,phone_number
	,null as last_4_digits
	,null as bill_to_street_address_1
	,null as bill_to_street_address_2
	,null as bill_to_street_address_3
	,null as bill_to_country_name
	,null as bill_to_country_code
	,null as bill_to_city
	,null as bill_to_state
	,null as bill_to_postal_code
	,null as ship_to_street_address_1
	,null as ship_to_street_address_2
	,null as ship_to_street_address_3
	,null as ship_to_country_name
	,null as ship_to_country_code
	,null as ship_to_city
	,null as ship_to_state
	,null as ship_to_postal_code
	,event_date as create_date
	,cast(event_date as string)as change_date
	,null as v_to_v_flag

	from

	((
	select
	 first_value (gender) over ( partition by email_address order by case when gender is not null then 1 else 0 end desc, event_date desc, gender) gender
   ,first_value (FORMAT_DATE("%m/%d/%Y",  date(birth_date))) 
   over ( partition by email_address order by case when birth_date is not null then 1 else 0 end desc, event_date desc, birth_date) as birth_date
	,email_address 
	,first_value (phone_number) 
   over ( partition by email_address order by case when phone_number is not null and phone_number not like '-' then 1 else 0 end desc, event_date desc, phone_number)
   as phone_number
	,event_date
	from
	`bnile-cdw-prod.up_stg_customer.stg_backbone`
	where email_address is not null  and (gender is not null or birth_date  is not null ))
	union all  
	(
	select
	 first_value (gender) over ( partition by phone_number order by case when gender is not null then 1 else 0 end desc, event_date desc, gender) gender
   ,first_value (FORMAT_DATE("%m/%d/%Y",  date(birth_date))) 
   over ( partition by phone_number order by case when birth_date is not null then 1 else 0 end desc, event_date desc, birth_date) as birth_date
	,email_address 
	,phone_number
	,event_date
	from
	`bnile-cdw-prod.up_stg_customer.stg_backbone`
	where email_address is  null  and  phone_number is not null and (gender is not null or birth_date  is not null )))
	)
	union all

	(select
	'review_pii' as event_type
	,null as basket_id
	,null as customer_id
	,null as first_name
	,null as middle_name
	,null as last_name
	,gender
	,null as birthday
	,null as customer_guid
	,reviewer_email_address as email_address
	,null as phone_number
	,null as last_4_digits
	,null as bill_to_street_address_1
	,null as bill_to_street_address_2
	,null as bill_to_street_address_3
	,null as bill_to_country_name
	,null as bill_to_country_code
	,null as bill_to_city
	,null as bill_to_state
	,null as bill_to_postal_code
	,null as ship_to_street_address_1
	,null as ship_to_street_address_2
	,null as ship_to_street_address_3
	,null as ship_to_country_name
	,reviewer_country as ship_to_country_code
	,null as ship_to_city
	,null as ship_to_state
	,null as ship_to_postal_code
	,timestamp(event_date) as create_date
	,cast(event_date as string) as change_date
	,null as v_to_v_flag
	from
	`bnile-cdw-prod.up_stg_customer.stg_review_pii`
	where reviewer_email_address is not null and (gender is not null   or reviewer_country  is not null ))
    
    union all
    (
	select
	'TheKnotWWData' as event_type
	,null as basket_id
	,null as customer_id
	,firstname as first_name
	,null as middle_name
	,lastname as last_name
	,null as gender
	,null as birthday
	,null as customer_guid
	,email as email_address
	,null as phone_number
	,null as last_4_digits
	,null as bill_to_street_address_1
	,null as bill_to_street_address_2
	,null as bill_to_street_address_3
	,null as bill_to_country_name
	,null as bill_to_country_code
	,null as bill_to_city
	,null as bill_to_state
	,null as bill_to_postal_code
	,null as ship_to_street_address_1
	,null as ship_to_street_address_2
	,null as ship_to_street_address_3
	,null as ship_to_country_name
	,null as ship_to_country_code
	,null as ship_to_city
	,null as ship_to_state
	,null as ship_to_postal_code
	,timestamp(dateoptined) as create_date
	,cast(dateoptined as string) as change_date
	,null as v_to_v_flag
	from `bnile-cdw-prod.theknotww_data.stg_theknotww_data`  
    )
  )

    where 
    coalesce(trim(email_address),'9999') not in('-') 
    --and coalesce(trim(email_address),'9999') is not null
    and coalesce(trim(email_address),'@') like '%@%' and coalesce(trim(email_address),'.') like '%.%'
                and coalesce(trim(email_address),'9999') not like '%jminsure.com'
                and coalesce(trim(email_address),'9999') not like 'apayment@bluenile.com' 
                and coalesce(trim(email_address),'9999') not like 'cntmpayment@bluenile.com'
                and coalesce(trim(email_address),'9999') not like '%test%bluenile%'
                and coalesce(trim(email_address),'9999') not like '%@test.com'
                and coalesce(trim(email_address),'9999') not in ('noemail@noemail.com'));



    ---- Query for master_bn_table ----

    CREATE TABLE IF NOT EXISTS
    `bnile-cdw-prod.up_stg_customer.master_bn_table`(bnid	STRING
    ,phone_number	STRING		
    ,email_address	STRING			
    ,created_date	TIMESTAMP		
    ,updated_date	TIMESTAMP		
    );

    insert into `bnile-cdw-prod.up_stg_customer.master_bn_table`

    select 
    cc.bnid,
    cc.phone_number,
    cc.email_address,
    cc.created_date,
    cc.updated_date
    --cc.v_to_v_flag
    from  
    (
    select 
    concat('BN',GENERATE_UUID()) as bnid,
    aa.phone_number,
    null as email_address,
    current_timestamp as created_date,
    current_timestamp as updated_date
    -- v_to_v_flag 
    --NULL  as email_address,
    from
    (select distinct phone_number from `bnile-cdw-prod.up_stg_customer.master_event_stage` where email_address  is  null 
    and phone_number is not null  
    AND  PHONE_NUMBER NOT IN(select distinct COALESCE(phone_number,'AAAAAAAA') from `bnile-cdw-prod.up_stg_customer.master_event_stage`
    WHERE EMAIL_ADDRESS IS NOT NULL)
    )aa where aa.phone_number  not in(select distinct phone_number from `bnile-cdw-prod.up_stg_customer.master_bn_table` where email_address is null and phone_number is not null)
    union distinct
    select 
    concat('BN',GENERATE_UUID()) as bnid,
    null as phone_number,
    lower(trim(bb.email_address)) as email_address,
    current_timestamp as created_date,
    current_timestamp as updated_date
    --v_to_v_flag
    --NULL  as email_address,
    from
    (select distinct REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com'),'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com') as email_address from `bnile-cdw-prod.up_stg_customer.master_event_stage` where email_address  is not null )bb
    where lower(trim(bb.email_address)) not in(select distinct lower(trim(email_address)) from `bnile-cdw-prod.up_stg_customer.master_bn_table` where email_address is not null)
    )cc;

    ----- insert daily data and create bn_test_key strore in master_bn_test_key---------

    insert into `bnile-cdw-prod.up_stg_customer.master_bn_test_key`
    select bnid,
        mod(row_num,105)+1 as bn_test_key,
        row_num
    from 
        (
        SELECT
        bnid,
        ROW_NUMBER() OVER() + (
                                SELECT
                                    coalesce(MAX(row_num),
                                    0)
                                FROM
                                    `bnile-cdw-prod.up_stg_customer.master_bn_test_key`) AS row_num



        FROM
        `bnile-cdw-prod.up_stg_customer.master_bn_table`
        WHERE
        bnid NOT IN (
        SELECT
            DISTINCT bnid
        FROM
            `bnile-cdw-prod.up_stg_customer.master_bn_test_key`)



        );



    ----- Query for bnid_guid_mapping -----

    drop table if exists `bnile-cdw-prod.up_stg_customer.bnid_guid_mapping`;

    CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.up_stg_customer.bnid_guid_mapping` as

    (select distinct aa.bnid,aa.customer_guid,aa.event_type,
    cast(aa.event_date as timestamp) as event_date,aa.created_date,aa.updated_date from 
    (select  bnid,s.customer_guid,s.event_type
    ,s.event_date,b.created_date,b.updated_date
    from
    (select distinct bnid,email_address,created_date,updated_date from `bnile-cdw-prod.up_stg_customer.master_bn_table`) b 
    join
    (
    select distinct event_type,email_address,customer_guid
    ,event_date
    from
    `bnile-cdw-prod.up_stg_customer.master_event_stage`
    where  email_address is not null and customer_guid is not null)s
    on lower(trim(s.email_address)) = lower(trim(b.email_address)))aa);

    insert into `bnile-cdw-prod.up_stg_customer.bnid_guid_mapping`

    select aa.bnid,aa.customer_guid,aa.event_type,
    cast(aa.event_date as timestamp) as event_date,aa.created_date,aa.updated_date from 
    (select  bnid,s.customer_guid,s.event_type
    ,s.event_date,b.created_date,b.updated_date
    from
    (select bnid,email_address,phone_number,created_date,updated_date from `bnile-cdw-prod.up_stg_customer.master_bn_table` where email_address is null and phone_number is not null ) b 
    join
    (
    select event_type,email_address,customer_guid,phone_number
    ,event_date
    from
    `bnile-cdw-prod.up_stg_customer.master_event_stage`
    where  email_address is null and phone_number is not null and customer_guid is not null)s
    on s.phone_number = b.phone_number)aa;	


    ---- Query for bnid_pii_mapping ---

    drop table if exists `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping`;

    CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping` as
    (select 
    distinct 
    bnid,
    aa.event_type,
    aa.event_date,
    aa.email_address,
    aa.first_name,
    aa.last_name,
    aa.birthday,
    aa.gender,
    aa.phone_number,
    aa.last_4_digits,
    aa.bill_to_street_address_1,
    aa.bill_to_street_address_2,
    aa.bill_to_street_address_3,
    aa.bill_to_country_name,
    aa.bill_to_country_code,
    aa.bill_to_city,
    aa.bill_to_state,
    aa.bill_to_postal_code,
    aa.ship_to_street_address_1,
    aa.ship_to_street_address_2,
    aa.ship_to_street_address_3,
    aa.ship_to_country_name,
    aa.ship_to_country_code,
    aa.ship_to_city,
    aa.ship_to_state,
    aa.ship_to_postal_code,
    aa.v_to_v_flag

    from
    (SELECT DISTINCT 
            REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com'),'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyahoo.com$*','yahoo.com') as email_address,
            event_type,
            FIRST_VALUE(v_to_v_flag) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN v_to_v_flag is NULL then 0 else 1 END DESC, event_date desc) AS v_to_v_flag,
            FIRST_VALUE(first_name) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN first_name is NULL then 0 else 1 END DESC, event_date desc) AS first_name,
            FIRST_VALUE(last_name) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN last_name is NULL then 0 else 1 END DESC, event_date desc) AS last_name,
            FIRST_VALUE(birthday) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN birthday is NULL then 0 else 1 END DESC, event_date desc) AS birthday,
            FIRST_VALUE(phone_number) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN phone_number is NULL then 0 else 1 END DESC, event_date desc) AS phone_number,
            FIRST_VALUE(gender) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN gender is NULL then 0 else 1 END DESC, event_date desc) AS gender,
            FIRST_VALUE(last_4_digits) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN last_4_digits is NULL or last_4_digits='' then 0 else 1 END DESC, event_date desc) AS last_4_digits,
            FIRST_VALUE(bill_to_street_address_1) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_street_address_1 is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_street_address_1,
            FIRST_VALUE(bill_to_street_address_2) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_street_address_2 is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_street_address_2,
            FIRST_VALUE(bill_to_street_address_3) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_street_address_3 is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_street_address_3,
            FIRST_VALUE(bill_to_country_name) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_country_name is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_country_name,
            FIRST_VALUE(bill_to_country_code) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_country_code is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_country_code,
            FIRST_VALUE(bill_to_city) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_city is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_city,
            FIRST_VALUE(bill_to_state) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_state is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_state,
            FIRST_VALUE(bill_to_postal_code) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN bill_to_postal_code is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_postal_code,
            FIRST_VALUE(ship_to_street_address_1) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN ship_to_street_address_1 is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_street_address_1,
            FIRST_VALUE(ship_to_street_address_2) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN ship_to_street_address_2 is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_street_address_2,
            FIRST_VALUE(ship_to_street_address_3) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN ship_to_street_address_3 is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_street_address_3,
            FIRST_VALUE(ship_to_country_name) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN ship_to_country_name is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_country_name,
            FIRST_VALUE(ship_to_country_code) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN ship_to_country_code is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_country_code,
            FIRST_VALUE(ship_to_city) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN ship_to_city is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_city,
            FIRST_VALUE(ship_to_state) OVER(PARTITION BY email_address,event_type ORDER BY CASE	 WHEN ship_to_state is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_state,
            FIRST_VALUE(ship_to_postal_code) OVER(PARTITION BY email_address,event_type ORDER BY CASE WHEN ship_to_postal_code is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_postal_code,
            event_date

    FROM   `bnile-cdw-prod.up_stg_customer.master_event_stage` where email_address is not null)aa
    inner join
    (select bnid,email_address from `bnile-cdw-prod.up_stg_customer.master_bn_table`)bb 
    on lower(trim(aa.email_address))=lower(trim(bb.email_address)) where aa.email_address is not null);


    insert into `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping`

    select 
    distinct 
    bnid,
    aa.event_type,
    aa.event_date,
    aa.email_address,
    aa.first_name,
    aa.last_name,
    aa.birthday,
    aa.gender,
    aa.phone_number,
    aa.last_4_digits,
    aa.bill_to_street_address_1,
    aa.bill_to_street_address_2,
    aa.bill_to_street_address_3,
    aa.bill_to_country_name,
    aa.bill_to_country_code,
    aa.bill_to_city,
    aa.bill_to_state,
    aa.bill_to_postal_code,
    aa.ship_to_street_address_1,
    aa.ship_to_street_address_2,
    aa.ship_to_street_address_3,
    aa.ship_to_country_name,
    aa.ship_to_country_code,
    aa.ship_to_city,
    aa.ship_to_state,
    aa.ship_to_postal_code,
    aa.v_to_v_flag

    from
    (SELECT DISTINCT 
            email_address,
            event_type,
            FIRST_VALUE(v_to_v_flag) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN v_to_v_flag is NULL then 0 else 1 END DESC, event_date desc) AS v_to_v_flag,
            FIRST_VALUE(first_name) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN first_name is NULL then 0 else 1 END DESC, event_date desc) AS first_name,
            FIRST_VALUE(last_name) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN last_name is NULL then 0 else 1 END DESC, event_date desc) AS last_name,
            FIRST_VALUE(birthday) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN birthday is NULL then 0 else 1 END DESC, event_date desc) AS birthday,
            FIRST_VALUE(phone_number) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN phone_number is NULL then 0 else 1 END DESC, event_date desc) AS phone_number,
            FIRST_VALUE(gender) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN gender is NULL then 0 else 1 END DESC, event_date desc) AS gender,
            FIRST_VALUE(last_4_digits) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN last_4_digits is NULL then 0 else 1 END DESC, event_date desc) AS last_4_digits,
            FIRST_VALUE(bill_to_street_address_1) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_street_address_1 is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_street_address_1,
            FIRST_VALUE(bill_to_street_address_2) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_street_address_2 is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_street_address_2,
            FIRST_VALUE(bill_to_street_address_3) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_street_address_3 is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_street_address_3,
            FIRST_VALUE(bill_to_country_name) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_country_name is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_country_name,
            FIRST_VALUE(bill_to_country_code) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_country_code is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_country_code,
            FIRST_VALUE(bill_to_city) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_city is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_city,
            FIRST_VALUE(bill_to_state) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_state is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_state,
            FIRST_VALUE(bill_to_postal_code) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN bill_to_postal_code is NULL then 0 else 1 END DESC, event_date desc) AS bill_to_postal_code,
            FIRST_VALUE(ship_to_street_address_1) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN ship_to_street_address_1 is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_street_address_1,
            FIRST_VALUE(ship_to_street_address_2) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN ship_to_street_address_2 is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_street_address_2,
            FIRST_VALUE(ship_to_street_address_3) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN ship_to_street_address_3 is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_street_address_3,
            FIRST_VALUE(ship_to_country_name) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN ship_to_country_name is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_country_name,
            FIRST_VALUE(ship_to_country_code) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN ship_to_country_code is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_country_code,
            FIRST_VALUE(ship_to_city) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN ship_to_city is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_city,
            FIRST_VALUE(ship_to_state) OVER(PARTITION BY phone_number,event_type ORDER BY CASE	 WHEN ship_to_state is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_state,
            FIRST_VALUE(ship_to_postal_code) OVER(PARTITION BY phone_number,event_type ORDER BY CASE WHEN ship_to_postal_code is NULL then 0 else 1 END DESC, event_date desc) AS ship_to_postal_code,
            event_date

    FROM   `bnile-cdw-prod.up_stg_customer.master_event_stage` where email_address is null and phone_number is not null )aa
    inner join
    (select bnid,phone_number,email_address from `bnile-cdw-prod.up_stg_customer.master_bn_table`)bb 
    on trim(aa.phone_number)=trim(bb.phone_number) where bb.email_address is null and  bb.phone_number is not null;





    ----- Query for bnid_pii_mapping_cleaned ----- 


     drop table if exists `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned`;

   CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned` as

     (select
    bb.event_type,
    bb.bnid,
    bb.email_address,
    bb.first_name,
    bb.last_name,
    case when bb.gender like '-'  then null when  length(bb.gender)>1  then null else bb.gender end as  gender ,
    case when bb.gender is null or bb.gender in('-') then cc.gender else bb.gender end as derived_gender,
    case when bb.phone_number like '-' then null when  bb.phone_number like '%.0' then split(bb.phone_number, ".")[offset(0)] 
    else bb.phone_number end as phone_number,
    bb.country_code,
    bb.extension,
    bb.phone,
    case when bb.mobile like '-' then null when bb.mobile like '' then null when  bb.mobile like '%.0' then split(bb.mobile, ".")[offset(0)] 
    else bb.mobile end as mobile,
    case when bb.last_4_digits is not null  then split(bb.last_4_digits, ".")[offset(0)]  else null end as last_4_digits,
    bb.birthday,
    bb.age,
    nullif(bill_to_street,'') as bill_to_street,
    bb.bill_to_city,
    bb.bill_to_state,
    bb.bill_to_country_name,
    bb.bill_to_country_code,

    case when REGEXP_contains (bb.bill_to_postal_code_modified,r"^\d{5}(?:[-]\d{4})?$") then bb.bill_to_postal_code_modified
    else bb.bill_to_postal_code end bill_to_postal_code,
    nullif(ship_to_street,'') as ship_to_street,
    bb.ship_to_city,
    bb.ship_to_state,
    bb.ship_to_country_name,
    bb.ship_to_country_code,
    case when REGEXP_contains (bb.ship_to_postal_code_modified,r"^\d{5}(?:[-]\d{4})?$") then bb.ship_to_postal_code_modified
    else bb.ship_to_postal_code end ship_to_postal_code,

    bb.first_name_is_valid,
    bb.last_name_is_valid,
    bb.email_is_valid,
    bb.v_to_v_flag,
    bb.event_date
    from
    (
    select
    distinct
    aa.event_type,
    aa.bnid,
    REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(aa.email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com'),'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyyahoo.com$','yahoo.com'),'yyahoo.com$','yahoo.com') as email_address,
    aa.first_name,
    aa.last_name,
    aa.gender,
    aa.phone_number,
    aa.country_code,
    replace(replace(replace(replace(replace(replace(aa.phone,' ',''),')',''),'(',''),'e',''),'-',''),'t','') as phone,
    replace(replace(replace(replace(replace(aa.mobile,' ',''),')',''),'(',''),'e',''),'-','') as mobile,
    replace(replace(replace(replace(replace(replace(replace(aa.extension,' ',''),')',''),'(',''),'e',''),'-',''),'t.',''),'t','') as extension,
    aa.last_4_digits,
    aa.birthday,
    aa.age,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(`bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.bill_to_street),',!,',','),',!',''),'  ',' '),'#$*','Number '),'St$*','Street'),'Rd$*','Road'),'Apt$*','Apartment'),'Dr$*','Drive'),'Ln$*','Lane'),'Ct$*','City'),'Ave$*','Avenue'),',$',''),',,*',','),',',','),'!*',''),'Roa$*','Road'),'Driveive$*','Drive'),'Roadd$*','Road'),'Avenuenue$*','Avenue'),'Avenuenu$*','Avenue'),'Streetree$*','Stree'), 'BLK$*','Block'),'Blk$*','Block'),'Ctrl$*','Centeral'),'Distr$*','District'),'distr$*','District')
    , 'Dist$*', 'District')) bill_to_street,
    aa.first_name_is_valid,
    aa.last_name_is_valid,
    aa.email_is_valid,
    aa.v_to_v_flag, 

    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.bill_to_city) as bill_to_city, 
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.bill_to_state) as bill_to_state,
    aa.bill_to_country_name,
     bill_to_country_code,

     aa.bill_to_POSTAL_CODE,
     --check the length of postalcode of 9 and digits
     case when length(trim(bill_to_postal_code_modified)) = 9 and ( bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')
     or regexp_contains (bill_to_country_name,r"^[U]S" )  or bill_to_country_code in ('US','USA')) 
     and regexp_contains(substring(trim(bill_to_postal_code_modified),0,5), '^[0-9]*$') 
     and regexp_contains(substring(trim(bill_to_postal_code_modified),6,9), '^[0-9]*$')
     then
     concat (left(trim( bill_to_postal_code_modified ),5),'-',right(trim(bill_to_postal_code_modified),4))

     --check the length of postal code of 4 and digits , adding the zero infront of them
     when length(trim(bill_to_postal_code_modified)) = 4 and ( bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or
     regexp_contains(bill_to_country_name,r"^[U]S" )  or bill_to_country_code in ('US','USA')) and 
     regexp_contains(substring(trim(bill_to_postal_code_modified),0,4), '^[0-9]*$') then concat ('0',left(trim( bill_to_postal_code_modified ),4))

     --check the postal code of 2 alphabets and 5 digits, taking only five digits
     when ( bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or regexp_contains (bill_to_country_name,r"^[U]S" ) 
     or bill_to_country_code in ('US','USA')) and regexp_contains (bill_to_postal_code,r"^[[:alpha:]]{2}(\s)?\d{5}$" )
     then right(trim(bill_to_postal_code),5)

     --check the length of postal code of 3 and digits , adding the two zeros infront of them

     when length(trim(bill_to_postal_code_modified)) = 3 and ( bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or
     regexp_contains(bill_to_country_name,r"^[U]S" )  or bill_to_country_code in ('US','USA')) and regexp_contains (bill_to_postal_code,r"\d{3}$" )
     then concat ('00',left(trim( bill_to_postal_code_modified ),3))

     else bill_to_postal_code_modified
     end bill_to_postal_code_modified,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(`bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.ship_to_street),',!,',','),',!',''),'  ',' '),'#$*','Number '),'St$*','Street'),'Rd$*','Road'),'Apt$*','Apartment'),'Dr$*','Drive'),'Ln$*','Lane'),'Ct$*','City'),'Ave$*','Avenue'),',$',''),',,*',','),',',','),'!*',''),'Roa$*','Road'),'Driveive$*','Drive'),'Roadd$*','Road'),'Avenuenue$*','Avenue'),'Avenuenu$*','Avenue'),'Streetree$*','Stree'), 'BLK$*','Block'),'Blk$*','Block'),'Ctrl$*','Centeral'),'Distr$*','District'),'distr$*','District')
    , 'Dist$*', 'District')) as ship_to_street,

    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.ship_to_city) as ship_to_city, 
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.ship_to_state) as ship_to_state,
     aa.ship_to_country_name,
     ship_to_country_code,
     aa.ship_to_POSTAL_CODE,

     --check the length of postalcode of 9 and digits
     case when length(trim(ship_to_postal_code_modified)) = 9 and ( ship_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')
     or regexp_contains (ship_to_country_name,r"^[U]S" )  or ship_to_country_code in ('US','USA')) 
     and regexp_contains(substring(trim(ship_to_postal_code_modified),0,5), '^[0-9]*$') 
     and regexp_contains(substring(trim(ship_to_postal_code_modified),6,9), '^[0-9]*$')
     then
     concat (left(trim( ship_to_postal_code_modified ),5),'-',right(trim(ship_to_postal_code_modified),4))

     --check the length of postal code of 4 and digits , adding the zero infront of them
     when length(trim(ship_to_postal_code_modified)) = 4 and ( ship_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or
     regexp_contains(ship_to_country_name,r"^[U]S" )  or ship_to_country_code in ('US','USA')) and 
     regexp_contains(substring(trim(ship_to_postal_code_modified),0,4), '^[0-9]*$') then concat ('0',left(trim( ship_to_postal_code_modified ),4))

     --check the postal code of 2 alphabets and 5 digits, taking only five digits
     when ( ship_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or regexp_contains (ship_to_country_name,r"^[U]S" ) 
     or ship_to_country_code in ('US','USA')) and regexp_contains (ship_to_postal_code,r"^[[:alpha:]]{2}(\s)?\d{5}$" )
     then right(trim(ship_to_postal_code),5)

     --check the length of postal code of 3 and digits , adding the two zeros infront of them

     when length(trim(ship_to_postal_code_modified)) = 3 and ( ship_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or
     regexp_contains(ship_to_country_name,r"^[U]S" )  or ship_to_country_code in ('US','USA')) and regexp_contains (ship_to_postal_code_modified,r"\d{3}$" )
     then concat ('00',left(trim( ship_to_postal_code_modified ),3))

     else ship_to_postal_code_modified
     end ship_to_postal_code_modified,

     aa.event_date

    from
    (SELECT
    event_type,
    bnid,
    email_address,
    --REGEXP_CONTAINS(email_address, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") AS email_is_valid,
    case when email_address not like '%@%.%'   -- Must contain at least one @ and one .
    or email_address not LIKE '%_@_%_.__%'
    or email_address like '%..%'        -- Cannot have two periods in a row
    or email_address like '%@%@%'       -- Cannot have two @ anywhere
    or email_address like '%.@%' or email_address like '%@.%' -- Cant have @ and . next to each other
    or email_address like '%.cm' or email_address like '%.co' -- Unlikely. Probably typos 
    or email_address like '%.or' or email_address like '%.ne' -- Missing last letter
    or LENGTH (email_address) >100
    or email_address LIKE '%.con'
    or email_address LIKE '%.ccom'
    or email_address LIKE '%.come'
    or email_address LIKE '%.cpm'
    or email_address LIKE '%.dom'
    or email_address LIKE '%.ccom'
    or email_address LIKE '%.comm'
    or email_address LIKE '%.cim'
    or email_address LIKE '%.xom'
    or email_address LIKE '%>'

    then false else true end as email_is_valid,
    v_to_v_flag, 
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(first_name) as first_name,
    case when REGEXP_CONTAINS(first_name,r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") then 'N'
    when REGEXP_CONTAINS(first_name, r'^[^A-Za-z]*$')  then 'N' 
    when first_name is null then first_name
    else 'Y'
    end as first_name_is_valid,
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(last_name) as last_name,
    case when REGEXP_CONTAINS(last_name,r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") then 'N'
    when REGEXP_CONTAINS(last_name, r'^[^A-Za-z]*$')  then 'N' 
    when last_name is null then last_name else 'Y'
    end as last_name_is_valid,
    birthday,
    date_diff(current_date,PARSE_DATE('%m/%d/%Y',birthday),year)  as age,
    phone_number,
    if(regexp_contains(replace(replace(lower(phone_number),'xt','x'),'ex','x'),'^[+]')=true,case when SPLIT(phone_number, '-')[safe_OFFSET(0)]=SPLIT(SPLIT(phone_number, '-')[safe_OFFSET(0)],' ')[safe_offset(0)] then
    SPLIT(phone_number, '-')[safe_OFFSET(0)] 
    ELSE SPLIT(phone_number, ' ')[safe_OFFSET(0)] end,null) as country_code,
        --SPLIT(phone_number, '-')[safe_OFFSET(1)] phone_with_ext,
        if(regexp_contains(replace(replace(lower(phone_number),'xt','x'),'ex','x'),'^[+]')=true,case when split(SPLIT(phone_number, '-')[safe_OFFSET(1)],'x')[safe_offset(0)]=split(SPLIT(phone_number, '-')[safe_OFFSET(1)],'x')[safe_offset(0)] 
        then split(SPLIT(phone_number, '-')[safe_OFFSET(1)],'x')[safe_offset(0)] else split(SPLIT(phone_number, ' ')[safe_OFFSET(1)],'x')[safe_offset(0)] end ,if(regexp_contains(replace(replace(lower(phone_number),'xt','x'),'ex','x'),'[x]$*')=true,SPLIT(phone_number, 'x')[safe_OFFSET(0)],null)) as phone,
        --SPLIT(phone_number, 'x')[safe_OFFSET(0)] part3,
        SPLIT(phone_number, 'x')[safe_OFFSET(1)] extension,
        case when regexp_contains(lower(phone_number),'[+]|x|xt|ex|ext')=false  then phone_number else  null end as mobile, 
    case when trim(gender)='Male' then 'M'
        when trim(gender)='Female' then 'F'
        when trim(gender)='0' then 'F'
        when trim(gender)='1' then 'M'
        when trim(gender)='male' then 'M'
        when trim(gender)='female' then 'F'
        when trim(gender)='m' then 'M'
        when trim(gender)='f' then 'F'
        else gender end as gender, 
    replace(last_4_digits,'.0','') as last_4_digits,
    cast(concat(ifnull(replace(bill_to_STREET_ADDRESS_1,'¿',''),'!'),',',ifnull(replace(bill_to_STREET_ADDRESS_2,'¿',''),'!'),',',ifnull(replace(bill_to_STREET_ADDRESS_3,'¿',''),'!')) as string) as bill_to_street,
    bill_to_CITY, 
    case when length(bill_to_STATE) = 1 then replace(replace(bill_to_STATE, "-",''),'.','')
    else replace(replace(bill_to_STATE, "-",''), '¿','')
    end bill_to_STATE,
    bill_to_country_name,
    bill_to_country_code, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
    (replace(replace(bill_to_POSTAL_CODE,'""',''),')',''),'(',''),'@',''),'`',''),'=',''),'&',''),',',''),':',''),';',''),'?',''),'*',''),'#',''), '$',''), '!',''),'.','') ,'"',''),'+',''),'%',''),"'",''), '[',''), ']',''), '^',''),'<',''),'”',''), '{',''), "\'", ''), '_','') bill_to_POSTAL_CODE,

     case 
           -- removing the extra zeros for the postal code
      when   (bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or regexp_contains (bill_to_country_name,r"^[U]S.*" ) or 
      bill_to_country_code in ('US','USA')) and REGEXP_contains (bill_to_postal_code, r'^[0-9]*$') 
      and length (cast((cast (bill_to_postal_code as numeric)) as string)) <= 5  and length (cast((cast (bill_to_postal_code as numeric)) as string)) >= 3 
      then cast (cast(bill_to_postal_code as numeric) as string)

-- five digits followed by three charecters converted to five digits

      when (bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA') or regexp_contains (bill_to_country_name,r"^[U]S.*" ) or       
      bill_to_country_code in ('US','USA')) and regexp_contains(bill_to_postal_code, r"^\d{5}[A-Ba-z\s]{1,3}$")
      then regexp_replace(bill_to_postal_code,r"[A-Ba-z\s]",'')
  --removing the special charecters   
      when REGEXP_contains (bill_to_postal_code,r"^\d{5}(?:[-]\d{4})?$") = false and  
      ( bill_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')  or regexp_contains (bill_to_country_name,r"^[U]S.*" )
      or bill_to_country_code in ('US','USA')
      ) and 
      -- regex for not taking the dates for cleanup
      not regexp_contains(trim(bill_to_postal_code), r'^[0-3]?[0-9]/[0-3]?[0-9]/(?:[0-9]{2})?[0-9]{2}$')
      then REGEXP_REPLACE(bill_to_postal_code,'[^0-9a-zA-Z]','') 
      else 
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
     (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
     (replace(replace(bill_to_postal_code,'""',''),')',''),'(',''),'@',''),'`',''),'=',''),'&',''),',',''),':','')
	 ,';',''),'?',''),'*',''),'#',''), '$',''), '!',''),'.','') ,'"',''),'+',''),'%',''),"'",''), '[',''), ']','')
	 , '^',''),'<',''),'”',''), '{',''),'_','')
      end
      bill_to_postal_code_modified,



     cast(concat(ifnull(ship_to_STREET_ADDRESS_1,'!'),',',ifnull(ship_to_STREET_ADDRESS_2,'!'),',',ifnull(ship_to_STREET_ADDRESS_3,'!')) as string) as ship_to_street,
     ship_to_CITY,
     case when length(ship_to_STATE) = 1 then replace(replace(bill_to_STATE, "-",''),'.','')
      else replace(replace(replace(ship_to_STATE, "-",''), '¿',''), '.','')
      end ship_to_STATE,
      ship_to_country_name,
      ship_to_country_code,
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
     (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
     (replace(replace(ship_to_POSTAL_CODE,'""',''),')',''),'(',''),'@',''),'`',''),'=',''),'&',''),',',''),':',''),';',''),'?',''),'*',''),'#',''), '$',''), '!',''),'.','') ,'"',''),'+',''),'%',''),"'",''), '[',''), ']',''), '^',''),'<',''),'”',''), '{',''),'_','')  as ship_to_POSTAL_CODE,

    --- creating the column with cleanup

      case 
      -- removing the extra zeros for the postal code
      when   (ship_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')  or regexp_contains (ship_to_country_name,r"^[U]S.*" ) or 
      ship_to_country_code in ('US','USA')) and REGEXP_contains (ship_to_postal_code, r'^[0-9]*$') 
      and length (cast((cast (ship_to_postal_code as numeric)) as string)) <= 5  and length (cast((cast (ship_to_postal_code as numeric)) as string)) >= 3 
      then cast (cast(ship_to_postal_code as numeric) as string)

-- five digits followed by three charecters converted to five digits

      when (ship_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')  or regexp_contains (ship_to_country_name,r"^[U]S.*" ) or       
      ship_to_country_code in ('US','USA')) and regexp_contains(ship_to_postal_code, r"^\d{5}[A-Ba-z\s]{1,3}$")
      then regexp_replace(ship_to_postal_code,r"[A-Ba-z\s]",'')

      -- removing the special charecters
      when REGEXP_contains (ship_to_postal_code,r"^\d{5}(?:[-]\d{4})?$") = false and  
      (ship_to_country_name in ('Virgin Islands','United States','Northern Mariana Islands','U.S. Virgin Islands','US','American Samoa','USA')  or regexp_contains (ship_to_country_name,r"^[U]S.*" )
      or ship_to_country_code in ('US','USA')) and 
      -- regex for not taking the dates for cleanup
      not regexp_contains(trim(ship_to_postal_code), r'^[0-3]?[0-9]/[0-3]?[0-9]/(?:[0-9]{2})?[0-9]{2}$')
      then REGEXP_REPLACE(ship_to_postal_code,'[^0-9a-zA-Z]','') 
      else 
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
     (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
     (replace(replace(ship_to_POSTAL_CODE,'""',''),')',''),'(',''),'@',''),'`',''),'=',''),'&',''),',',''),':',''),';',''),'?',''),'*',''),'#',''), '$',''), '!',''),'.','') ,'"',''),'+',''),'%',''),"'",''), '[',''), ']',''), '^',''),'<',''),'”',''), '{',''),'_','')
      end
      ship_to_postal_code_modified,
      event_date

     FROM `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping` )aa)bb 

    left join

    (select distinct f_name,gender
    from
    (
    SELECT trim(lower(first_name)) as f_name ,gender FROM `bnile-cdw-prod.dssprod_o_warehouse.map_name_gender`
    union  distinct  
    SELECT trim(lower(name)) as f_name,gender FROM `bnile-cdw-prod.up_tgt_ora_tables.glob_name_gender` where lower(name) not in(
    select distinct trim(lower(first_name)) from `bnile-cdw-prod.dssprod_o_warehouse.map_name_gender`)
    )aa)cc
    on lower(bb.first_name)=lower(cc.f_name));


    ---- Query for bnid_stage_table ----

    CREATE OR REPLACE TABLE  `bnile-cdw-prod.up_stg_customer.bnid_stage_table` as

    ( 
    select * EXCEPT(row_num) from
    (
    select 
    bnid
    ,email_address
    ,first_name
    ,last_name
    ,gender
    ,derived_gender
    ,birthday
    ,age
    ,phone_number
    ,country_code
    ,extension
    ,phone
    ,mobile
    ,case when last_4_digits is not null  then LPAD(last_4_digits,4,'0') else null end last_4_digits
    ,v_to_v_flag
    ,bill_to_street
    ,bill_to_city
    ,bill_to_state
    ,bill_to_country_name
    ,bill_to_country_code
    ,bill_to_postal_code
    ,ship_to_street
    ,ship_to_city
    ,ship_to_state
    ,ship_to_country_name
    ,ship_to_country_code
    ,ship_to_postal_code
    ,first_name_is_valid
    ,last_name_is_valid
    ,email_is_valid
    ,row_number() over(partition by bnid order by first_name desc,last_name desc) row_num
    from

    (SELECT DISTINCT
            first_value(bnid) over(partition by coalesce(email_address,phone_number) order by first_name desc,last_name desc ) as bnid,
              email_address,

            FIRST_VALUE(first_name) OVER(PARTITION BY bnid ORDER BY CASE WHEN first_name is NULL then 0 else 1 END DESC, first_name_priority asc) AS first_name,
            FIRST_VALUE(last_name) OVER(PARTITION BY bnid ORDER BY CASE WHEN last_name is NULL then 0 else 1 END DESC, last_name_priority asc) AS last_name,
            FIRST_VALUE(birthday) OVER(PARTITION BY bnid ORDER BY CASE WHEN birthday is NULL then 0 else 1 END DESC, birth_date_priority asc) AS birthday,
            FIRST_VALUE(age) OVER(PARTITION BY bnid ORDER BY CASE WHEN age is NULL then 0 else 1 END DESC, birth_date_priority asc) AS age,
            FIRST_VALUE(phone_number) OVER(PARTITION BY bnid ORDER BY CASE WHEN phone_number is NULL then 0 else 1 END DESC, phone_number_priority asc) AS phone_number,

            FIRST_VALUE(country_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN country_code is NULL then 0 else 1 END DESC, phone_number_priority asc) AS country_code,
            FIRST_VALUE(extension) OVER(PARTITION BY bnid ORDER BY CASE WHEN extension is NULL then 0 else 1 END DESC, phone_number_priority asc) AS extension,
            FIRST_VALUE(phone) OVER(PARTITION BY bnid ORDER BY CASE WHEN phone is NULL then 0 else 1 END DESC, phone_number_priority asc) AS phone,
            FIRST_VALUE(mobile) OVER(PARTITION BY bnid ORDER BY CASE WHEN mobile is NULL then 0 else 1 END DESC, phone_number_priority asc) AS mobile,

            FIRST_VALUE(gender) OVER(PARTITION BY bnid ORDER BY CASE WHEN gender is NULL then 0 else 1 END DESC, gender_priority asc) AS gender,
            FIRST_VALUE(derived_gender) OVER(PARTITION BY bnid ORDER BY CASE WHEN derived_gender is NULL then 0 else 1 END DESC, gender_priority asc) AS derived_gender,
            FIRST_VALUE(last_4_digits) OVER(PARTITION BY bnid ORDER BY CASE WHEN last_4_digits is NULL then 0 else 1 END DESC, Last_4_CC_priority asc) AS last_4_digits,

            FIRST_VALUE(bill_to_street) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_street is NULL then 0 else 1 END DESC, billing_address_priority asc) AS bill_to_street,
            FIRST_VALUE(bill_to_city) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_city is NULL then 0 else 1 END DESC, billing_address_priority asc) AS bill_to_city,
            FIRST_VALUE(bill_to_state) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_state is NULL then 0 else 1 END DESC, billing_address_priority asc) AS bill_to_state,
            FIRST_VALUE(bill_to_country_name) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_country_name is NULL then 0 else 1 END DESC, billing_address_priority asc) AS bill_to_country_name,
			FIRST_VALUE(bill_to_country_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_country_code is NULL then 0 else 1 END DESC, billing_address_priority asc) AS bill_to_country_code,
			FIRST_VALUE(bill_to_postal_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN bill_to_postal_code is NULL then 0 else 1 END DESC, billing_address_priority asc) AS bill_to_postal_code,

            FIRST_VALUE(ship_to_street) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_street is NULL then 0 else 1 END DESC, shipping_address_priority asc) AS ship_to_street,
            FIRST_VALUE(ship_to_city) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_city is NULL then 0 else 1 END DESC, shipping_address_priority asc) AS ship_to_city,
            FIRST_VALUE(ship_to_state) OVER(PARTITION BY bnid ORDER BY CASE	 WHEN ship_to_state is NULL then 0 else 1 END DESC, shipping_address_priority asc) AS ship_to_state,
            FIRST_VALUE(ship_to_country_name) OVER(PARTITION BY bnid ORDER BY CASE	 WHEN ship_to_country_name is NULL then 0 else 1 END DESC, shipping_address_priority asc) AS ship_to_country_name,
			FIRST_VALUE(ship_to_country_code) OVER(PARTITION BY bnid ORDER BY CASE	 WHEN ship_to_country_code is NULL then 0 else 1 END DESC, shipping_address_priority asc) AS ship_to_country_code,
			FIRST_VALUE(ship_to_postal_code) OVER(PARTITION BY bnid ORDER BY CASE WHEN ship_to_postal_code is NULL then 0 else 1 END DESC, shipping_address_priority asc) AS ship_to_postal_code,

			FIRST_VALUE(v_to_v_flag) OVER(PARTITION BY bnid ORDER BY CASE WHEN v_to_v_flag is NULL then 0 else 1 END DESC, default_priority asc) AS v_to_v_flag,
            FIRST_VALUE(first_name_is_valid) OVER(PARTITION BY bnid ORDER BY CASE WHEN first_name_is_valid is NULL then 0 else 1 END DESC, first_name_priority asc) AS first_name_is_valid,
            FIRST_VALUE(last_name_is_valid) OVER(PARTITION BY bnid ORDER BY CASE WHEN last_name_is_valid is NULL then 0 else 1 END DESC, last_name_priority asc) AS last_name_is_valid,
            FIRST_VALUE(email_is_valid) OVER(PARTITION BY bnid ORDER BY CASE WHEN email_is_valid is NULL then 0 else 1 END DESC, default_priority asc) AS email_is_valid,

        from
    (SELECT 
    distinct 
    *,
    case 
    when phone_number is not null and event_type='freshdesk_contact' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='phone_number') 
    when phone_number is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='phone_number') 
    when phone_number is not null and event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='phone_number') 
    when phone_number is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='phone_number')
    when phone_number is not null and event_type='backbone' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='backbone' and pii_field='phone_number')
    else null
    end  phone_number_priority,

    case 
    when first_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='first_name') 
    when first_name is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='first_name') 
    when first_name is not null and event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='first_name') 
    when first_name is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='first_name') 
    when first_name is not null and event_type like '%freshdesk%' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='first_name')  
    when first_name is not null and event_type = 'TheKnotWWData' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='TheKnotWWData' and pii_field='first_name')
    else null
    

    end  first_name_priority,

    case 
    when last_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='last_name') 
    when last_name is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='last_name') 
    when last_name is not null and event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='last_name') 
    when last_name is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='last_name')
    when last_name is not null and event_type like '%freshdesk%' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='last_name')
    when last_name is not null and event_type = 'TheKnotWWData' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='TheKnotWWData' and pii_field='last_name')
    else null
    end  last_name_priority,

    case 
    when birthday is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='birth_date') 
    when birthday is not null and event_type='customer_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_event' and pii_field='birth_date') 
    when birthday is not null and event_type='email_survey' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='email_survey' and pii_field='birth_date') 
    when birthday is not null and event_type='backbone' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='backbone' and pii_field='birth_date') 
    else null
    end  birth_date_priority,

    case 
    when gender is not null and event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_account' and pii_field='gender') 
    when gender is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='gender')
    when gender is not null and event_type='freshdesk_contact' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='gender')
    when gender is not null and event_type='customer_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_event' and pii_field='gender')
    when gender is not null and event_type='email_survey' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='email_survey' and pii_field='gender')
    when gender is not null and event_type='backbone' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='backbone' and pii_field='gender')
    when gender is not null and event_type='review_pii' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='review_pii' and pii_field='gender')
    else null
    end  gender_priority,

    case 
    when Last_4_digits is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='Last_4_CC') 
    else null
    end  Last_4_CC_priority,

    case 

    when bill_to_street is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_city is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_state is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_postal_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address')
    when bill_to_country_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    when bill_to_country_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='billing_address') 
    else null
    end  billing_address_priority,

    case 

    when ship_to_street is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_street is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')


    when ship_to_city is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_city is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')

    when ship_to_state is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_state is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')

    when ship_to_country_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_country_code is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')
    when ship_to_country_code is not null and event_type='review_pii' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='review_pii' and pii_field='shipping_address')

    when ship_to_country_name is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_country_name is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')

    when ship_to_postal_code is not null and event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='shipping_address') 
    when ship_to_postal_code is not null and event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='shipping_address')
    when ship_to_postal_code is not null and event_type='customer_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer_event' and pii_field='shipping_address')

    else null
    end  shipping_address_priority,

    case 
    when event_type='orders' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='orders' and pii_field='default') 
    when event_type='basket' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='basket' and pii_field='default') 
    when event_type='customer' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='customer' and pii_field='default') 
    when  event_type like '%freshdesk%' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='freshdesk_contact' and pii_field='default')
    when event_type='site_event' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='site_event' and pii_field='default')
    when  event_type='click_fact' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='click_fact' and pii_field='default')
    when event_type='email_subscription_change_log' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='email_subscription_change_log' and pii_field='default')
    when event_type = 'TheKnotWWData' then (select rank from `bnile-cdw-prod.up_stg_customer.pii_priority` where source_event='TheKnotWWData' and pii_field='default')
    else null
    end  default_priority

    from `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned`   order by phone_number_priority,first_name_priority,last_name_priority,birth_date_priority,gender_priority,Last_4_CC_priority,
    billing_address_priority,shipping_address_priority,default_priority asc)aa))
    where row_num=1 );





    ----- Query for final_bnid_table ----
    drop table if exists `bnile-cdw-prod.up_stg_customer.final_bnid_table`;
    CREATE TABLE IF NOT EXISTS `bnile-cdw-prod.up_stg_customer.final_bnid_table` as

    (
    select 
    bb.bnid,
    bb.email_address,
    bb.first_name,
    bb.last_name,
    bb.gender ,
    case when bb.gender is null or bb.gender in('-') then cc.gender else bb.gender end as derived_gender,
    bb.phone_number,
    bb.country_code,
    bb.extension,
    bb.phone,
    bb.mobile,
    bb.last_4_digits,
    bb.birthday,
    bb.age,
    nullif(bill_to_street,'') as bill_to_street,
    bb.bill_to_city,
    bb.bill_to_state,
	bill_to_country_name,
	bill_to_country_code,
    bb.bill_to_postal_code,
    nullif(ship_to_street,'') as ship_to_street,
    bb.ship_to_city,
    bb.ship_to_state,
	ship_to_country_name,
	ship_to_country_code,
    bb.ship_to_postal_code,
    bb.first_name_is_valid,
    bb.last_name_is_valid,
    bb.email_is_valid,
    bb.v_to_v_flag
    from
    (
    select
    distinct
    aa.bnid,
    REGEXP_replace( REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(REGEXP_replace(trim(lower(aa.email_address)),'gmial.com$','gmail.com'),'gimal.com$','gmail.com'),'yahooo.com$','yahoo.com'),'yyahooo.com$','yahoo.com'),'yyyahoo.com$','yahoo.com'),'yyahoo.com$','yahoo.com') as email_address,
    aa.first_name,
    aa.last_name,
    aa.gender,
    aa.phone_number,
    aa.country_code,
    aa.extension,
    aa.phone,
    aa.mobile,
    aa.last_4_digits,
    aa.birthday,
    aa.age,
    aa.first_name_is_valid,
    aa.last_name_is_valid,
    aa.email_is_valid,
    aa.v_to_v_flag, 
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(`bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.bill_to_street),',!,',','),',!',''),'  ',' '),'#$*','Number '),'St$*','Street'),'Rd$*','Road'),'Apt$*','Apartment'),'Dr$*','Drive'),'Ln$*','Lane'),'Ct$*','City'),'Ave$*','Avenue'),',$',''),',,*',','),',',','),'!*',''),'Roa$*','Road'),'Driveive$*','Drive'),'Roadd$*','Road'),'Avenuenue$*','Avenue'),'Avenuenu$*','Avenue'),'Streetree$*','Stree')) as bill_to_street,
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.bill_to_city) as bill_to_city, 
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.bill_to_state) as bill_to_state,
	 bill_to_country_name,
	 bill_to_country_code,
    aa.bill_to_POSTAL_CODE,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(`bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.ship_to_street),',!,',','),',!',''),'  ',' '),'#$*','Number '),'St$*','Street'),'Rd$*','Road'),'Apt$*','Apartment'),'Dr$*','Drive'),'Ln$*','Lane'),'Ct$*','City'),'Ave$*','Avenue'),',$',''),',,*',','),',',','),'!*',''),'Roa$*','Road'),'Driveive$*','Drive'),'Roadd$*','Road'),'Avenuenue$*','Avenue'),'Avenuenu$*','Avenue'),'Streetree$*','Stree')) as ship_to_street,
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.ship_to_city) as ship_to_city, 
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(aa.ship_to_state) as ship_to_state,
	ship_to_country_name,
	ship_to_country_code,
    aa.ship_to_POSTAL_CODE
    from
    (SELECT
    bnid,
    email_address,
    --REGEXP_CONTAINS(email_address, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") AS email_is_valid, 
    case when email_address not like '%@%.%'   -- Must contain at least one @ and one .
    or email_address not LIKE '%_@_%_.__%'
    or email_address like '%..%'        -- Cannot have two periods in a row
    or email_address like '%@%@%'       -- Cannot have two @ anywhere
    or email_address like '%.@%' or email_address like '%@.%' -- Cant have @ and . next to each other
    or email_address like '%.cm' or email_address like '%.co' -- Unlikely. Probably typos 
    or email_address like '%.or' or email_address like '%.ne' -- Missing last letter
    or LENGTH (email_address) >100
    or email_address LIKE '%.con'
    or email_address LIKE '%.ccom'
    or email_address LIKE '%.come'
    or email_address LIKE '%.cpm'
    or email_address LIKE '%.dom'
    or email_address LIKE '%.ccom'
    or email_address LIKE '%.comm'
    or email_address LIKE '%.cim'
    or email_address LIKE '%.xom'
    or email_address LIKE '%>'
    then 'false' else 'true' end as email_is_valid,
    v_to_v_flag, 
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(first_name) as first_name,
    case when REGEXP_CONTAINS(first_name,r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") then 'N'
    when REGEXP_CONTAINS(first_name, r'^[^A-Za-z]*$')  then 'N' 
    when first_name is null then first_name
    else 'Y'
    end as first_name_is_valid,
    `bnile-cdw-prod.up_tgt_ora_tables.initcap`(last_name) as last_name,
    case when REGEXP_CONTAINS(last_name,r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") then 'N'
    when REGEXP_CONTAINS(last_name, r'^[^A-Za-z]*$')  then 'N' 
    when last_name is null then last_name else 'Y'
    end as last_name_is_valid,
    birthday,
    date_diff(current_date,PARSE_DATE('%m/%d/%Y',birthday),year)  as age,
    phone_number, 
    country_code,
    extension,
    phone,
    mobile,
    case when trim(gender)='Male' then 'M'
        when trim(gender)='Female' then 'F'
        when trim(gender)='0' then 'F'
        when trim(gender)='1' then 'M'
        when trim(gender)='male' then 'M'
        when trim(gender)='female' then 'F'
        when trim(gender)='m' then 'M'
        when trim(gender)='f' then 'F'
        else gender end as gender, 
    replace(last_4_digits,'.0','') as last_4_digits,
    bill_to_street,
    bill_to_CITY, 
    bill_to_STATE,
    bill_to_country_name,
	bill_to_country_code,
    bill_to_POSTAL_CODE,
    ship_to_street,
    ship_to_CITY, 
    ship_to_STATE,
	ship_to_country_name,
	ship_to_country_code,
    ship_to_POSTAL_CODE

    FROM `bnile-cdw-prod.up_stg_customer.bnid_stage_table`)aa)bb 

    left join

    (select distinct f_name,gender
    from
    (
    SELECT trim(lower(first_name)) as f_name ,gender FROM `bnile-cdw-prod.dssprod_o_warehouse.map_name_gender`
    union  distinct  
    SELECT trim(lower(name)) as f_name,gender FROM `bnile-cdw-prod.up_tgt_ora_tables.glob_name_gender` where lower(name) not in(
    select distinct trim(lower(first_name)) from `bnile-cdw-prod.dssprod_o_warehouse.map_name_gender`)
    )aa)cc

    on lower(bb.first_name)=lower(cc.f_name));


    ------------------up_email_address_dim-------------------

    CREATE TABLE IF NOT EXISTS
    `bnile-cdw-prod.up_stg_customer.up_email_address_dim`
    (up_email_address_key int64
    ,email_address	STRING
    ,bnid STRING
    );

    insert into `bnile-cdw-prod.up_stg_customer.up_email_address_dim`

    select 
    ROW_NUMBER() OVER(order by email_address) + (select coalesce(max(up_email_address_key),1000000) from  `bnile-cdw-prod.up_stg_customer.up_email_address_dim`) AS up_email_address_key,
    email_address,
    bnid
    from `bnile-cdw-prod.up_stg_customer.final_bnid_table` where email_address is not null and email_address not in(
    select distinct email_address from  `bnile-cdw-prod.up_stg_customer.up_email_address_dim`);



    --Final Dedup Implementation

    ------------------------Phone Number Dedup --------------------------------------------------------------------------
    -- See https://confluence.bluenile.com/pages/viewpage.action?spaceKey=UP&title=De+duplication+Rules  for rules
    -- Rule 1 - Capture orders where name is input in one column ( first name)


    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_phone_base1` AS
    SELECT
    bnid,
    phone,
    first_name,
    MIN(event_date) AS first_event_date
    FROM (
        SELECT
            DISTINCT bnid,
            ifnull(phone,
            mobile) phone,
            first_name,
            event_date
        FROM
            `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned`   -- Check dataset path
        WHERE
            phone_number IS NOT NULL
            AND first_name IS NOT NULL
            AND last_name IS NULL 
        )
    WHERE
    phone IS NOT NULL
    AND LENGTH(phone) <> 0
    GROUP BY
    bnid,
    phone,
    first_name 
    ;

    -- create summary counts
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_phone_cnt1` AS
    SELECT
    *
    FROM (
    SELECT
        bnid,
        phone,
        first_name,
        first_event_date,
        COUNT(DISTINCT bnid) OVER(PARTITION BY phone, first_name) AS match_cnt,
        ROW_NUMBER() OVER(PARTITION BY phone, first_name ORDER BY first_event_date ASC) AS row_num,
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_phone_base1` )
    WHERE
    match_cnt>1
    AND match_cnt<=9
    ORDER BY
    phone,
    first_name ;


    --create map1

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_phone_map1` AS
    -- Create dup map
    SELECT
    primary.bnid AS primary_bnid,
    primary.first_event_date AS p_first_event_date,
    dup.bnid duplicate_bnid,
    dup.first_event_date AS d_first_event_date,
    'phoneruleset1' AS rule
    FROM (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_phone_cnt1`
    WHERE
        row_num=1) primary
    RIGHT JOIN (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_phone_cnt1`
    WHERE
        row_num>1) dup
    ON
    primary.phone=dup.phone
    AND primary.first_name=dup.first_name;

    -----------------------------------------------------------
    -- Rule 2 - Phone + first+last name - 
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_phone_base2` AS
    SELECT
    bnid,
    phone,
    first_name,
    last_name,
    MIN(event_date) as first_event_date
    FROM (
    SELECT
        DISTINCT bnid,
        ifnull(phone,
        mobile) phone,
        first_name,
        last_name,
        event_date
    FROM
        `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned`
    WHERE
        phone_number IS NOT NULL
        AND first_name IS NOT NULL
        AND last_name IS NOT NULL )
    WHERE
    phone IS NOT NULL
    AND LENGTH(phone) <> 0
    GROUP BY
    bnid,
    phone,
    first_name,
    last_name;


    -- summary cnts 
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_phone_cnt2` AS
    SELECT
    *
    FROM (
    SELECT
        bnid,
        phone,
        first_name,
        last_name,
        first_event_date,
        COUNT(DISTINCT bnid) OVER(PARTITION BY phone, first_name, last_name) AS match_cnt,
        ROW_NUMBER() OVER(PARTITION BY phone, first_name, last_name ORDER BY first_event_date ASC) AS row_num,
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_phone_base2` )
    WHERE
    match_cnt>1
    AND match_cnt<=9
    ORDER BY
    phone,
    first_name,
    last_name;

    -----------------------------------------------------------------

    -- phone 2 - Create dup map
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_phone_map2` AS
    SELECT
    primary.bnid AS primary_bnid,
    primary.first_event_date AS p_first_event_date,
    dup.bnid duplicate_bnid,
    dup.first_event_date AS d_first_event_date,
    'phoneruleset2' AS rule
    FROM (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_phone_cnt2`
    WHERE
        row_num=1) primary
    RIGHT JOIN (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_phone_cnt2`
    WHERE
        row_num>1) dup
    ON
    primary.phone=dup.phone
    AND primary.first_name=dup.first_name
    AND primary.last_name=dup.last_name ;


    -------------------------------------------------------------------
    -- Rule 3 CC base

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_cc_base1` AS
    SELECT
    bnid,
    last_4_digits,
    bill_to_POSTAL_CODE,
    ship_to_POSTAL_CODE,
    first_name,
    min(event_date) as first_event_date
    from
    (
    SELECT
    DISTINCT bnid,
    last_4_digits,
    bill_to_POSTAL_CODE,
    ship_to_POSTAL_CODE,
    first_name,
    event_date
    FROM
    `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned`
    WHERE
    last_4_digits IS NOT NULL
    AND bill_to_POSTAL_CODE IS NOT NULL
    AND ship_to_POSTAL_CODE IS NOT NULL
    AND first_name IS NOT NULL )
    GROUP BY
    bnid,
    last_4_digits,
    bill_to_POSTAL_CODE,
    ship_to_POSTAL_CODE,
    first_name
    ;


    -- CC1 counts

    CREATE OR REPLACE TABLE 
    `bnile-cdw-prod.up_stg_dedup.dedup_cc_cnt1` AS
    SELECT
    *
    FROM (
    SELECT
        bnid,
        last_4_digits,
        bill_to_POSTAL_CODE,
        ship_to_POSTAL_CODE,
        first_name,
        first_event_date,
        COUNT(DISTINCT bnid) OVER(PARTITION BY last_4_digits, bill_to_POSTAL_CODE, ship_to_POSTAL_CODE, first_name) AS match_cnt,
        ROW_NUMBER() OVER(PARTITION BY last_4_digits, bill_to_POSTAL_CODE, ship_to_POSTAL_CODE, first_name ORDER BY first_event_date ASC) AS row_num,
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_cc_base1` )
    WHERE
    match_cnt>1
    AND match_cnt<=9
    ORDER BY
    last_4_digits,
    bill_to_POSTAL_CODE,
    first_name ;

    --CC dedup map
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_cc_map1` AS
    SELECT
    primary.bnid AS primary_bnid,
    primary.first_event_date AS p_first_event_date,
    dup.bnid duplicate_bnid,
    dup.first_event_date AS d_first_event_date,
    'ccruleset1' AS rule
    FROM (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_cc_cnt1`
    WHERE
        row_num=1) primary
    RIGHT JOIN (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_cc_cnt1`
    WHERE
        row_num>1) dup
    ON
    primary.last_4_digits =dup.last_4_digits
    AND primary.bill_to_POSTAL_CODE =dup.bill_to_POSTAL_CODE
    AND primary. ship_to_POSTAL_CODE =dup.ship_to_POSTAL_CODE
    AND primary.first_name =dup.first_name;

    -------------------------------------------------------------------------
    --- Address Rule 1-----------

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_addr_base1` AS
    SELECT 
    bnid,
    bill_to_street,
    bill_to_POSTAL_CODE,
    ship_to_street,
    ship_to_POSTAL_CODE,
    first_name,
    min(event_date) as first_event_date
    FROM
    (
    SELECT
    DISTINCT bnid,
    bill_to_street,
    bill_to_POSTAL_CODE,
    ship_to_street,
    ship_to_POSTAL_CODE,
    first_name,
    event_date
    FROM
    `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned`
    WHERE
    bill_to_street IS NOT NULL
    AND bill_to_POSTAL_CODE IS NOT NULL
    AND ship_to_street IS NOT NULL
    AND ship_to_POSTAL_CODE IS NOT NULL
    AND first_name IS NOT NULL)
    GROUP BY
    bnid,
    bill_to_street,
    bill_to_POSTAL_CODE,
    ship_to_street,
    ship_to_POSTAL_CODE,
    first_name
    ;

    -- create addr dedup counts
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_addr_cnt1` AS
    SELECT
    *
    FROM (
    SELECT
        bnid,
        bill_to_street,
        bill_to_POSTAL_CODE,
        ship_to_street,
        ship_to_POSTAL_CODE,
        first_name,
        first_event_date,
        COUNT(DISTINCT bnid) OVER(PARTITION BY bill_to_street, bill_to_POSTAL_CODE, ship_to_street, ship_to_POSTAL_CODE, first_name) AS match_cnt,
        ROW_NUMBER() OVER(PARTITION BY bill_to_street, bill_to_POSTAL_CODE, ship_to_street, ship_to_POSTAL_CODE, first_name ORDER BY first_event_date) AS row_num,
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_addr_base1` )
    WHERE
    match_cnt>1
    AND match_cnt<=9
    ;

    -- address dedup map
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_addr_map1` AS
    SELECT
    primary.bnid AS primary_bnid,
    primary.first_event_date AS p_first_event_date,
    dup.bnid duplicate_bnid,
    dup.first_event_date AS d_first_event_date,
    'addressruleset1' AS rule
    FROM (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_addr_cnt1`
    WHERE
        row_num=1) primary
    RIGHT JOIN (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_addr_cnt1`
    WHERE
        row_num>1) dup
    ON
    primary.bill_to_street = dup.bill_to_street
    AND primary.bill_to_POSTAL_CODE = dup.bill_to_POSTAL_CODE
    AND primary.ship_to_street = dup.ship_to_street
    AND primary. ship_to_POSTAL_CODE = dup.ship_to_POSTAL_CODE
    AND primary.first_name = dup.first_name
    ;

    ----------------------------------------------------------------------------
    -- GUID based dedup

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_guid_base1` AS
    SELECT 
    bnid,
    first_name,
    last_name,
    guid,
    min(event_date) as first_event_date
    from
    (
    SELECT
    a.bnid,
    first_name,
    last_name,
    customer_guid AS guid,
    A.event_date
    FROM (
    SELECT
        bnid,
        first_name,
        last_name,
        MIN(event_date) AS event_date
    FROM
        `bnile-cdw-prod.up_stg_customer.bnid_pii_mapping_cleaned`
    GROUP BY
        bnid,
        first_name,
        last_name
    HAVING
        first_name IS NOT NULL
        AND last_name IS NOT NULL ) A
    INNER JOIN
    `bnile-cdw-prod.up_stg_customer.bnid_guid_mapping` B
    ON
    a.bnid=b.bnid)
    GROUP BY
    bnid,
    first_name,
    last_name,
    guid
    ;

    --counts
    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_guid_cnt1` AS
    SELECT
    *
    FROM (
    SELECT
        bnid,
        guid,
        first_name,
        last_name,
        first_event_date,
        COUNT(DISTINCT bnid) OVER(PARTITION BY guid, first_name, last_name) AS match_cnt,
        ROW_NUMBER() OVER(PARTITION BY guid, first_name, last_name ORDER BY first_event_date) AS row_num,
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_guid_base1` )
    WHERE
    match_cnt>1
    AND match_cnt<=9
    ORDER BY
    guid,
    first_name,
    last_name 
    ;

    -- map

    CREATE OR REPLACE TABLE
    `bnile-cdw-prod.up_stg_dedup.dedup_guid_map1` AS
    SELECT
    primary.bnid AS primary_bnid,
    primary.first_event_date AS p_first_event_date,
    dup.bnid duplicate_bnid,
    dup.first_event_date AS d_first_event_date,
    'guidruleset1' AS rule
    FROM (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_guid_cnt1`
    WHERE
        row_num=1) primary
    RIGHT JOIN (
    SELECT
        *
    FROM
        `bnile-cdw-prod.up_stg_dedup.dedup_guid_cnt1`
    WHERE
        row_num>1) dup
    ON
    primary.guid=dup.guid
    AND primary.first_name=dup.first_name
    AND primary.last_name=dup.last_name 
    ;

    --- combine the maps
    create or replace table `bnile-cdw-prod.up_stg_dedup.bnid_dedup_map` as

    select distinct primary_bnid, p_first_event_date, duplicate_bnid, d_first_event_date, rule
    from
    (
    select * from `bnile-cdw-prod.up_stg_dedup.dedup_phone_map1`
    UNION ALL
    select * from `bnile-cdw-prod.up_stg_dedup.dedup_phone_map2`
    UNION ALL
    select * from `bnile-cdw-prod.up_stg_dedup.dedup_cc_map1`
    UNION ALL
    select * from `bnile-cdw-prod.up_stg_dedup.dedup_addr_map1`
    UNION ALL
    select * from `bnile-cdw-prod.up_stg_dedup.dedup_guid_map1`
    )
    where primary_bnid<>duplicate_bnid
    order by primary_bnid;'''
    return bql