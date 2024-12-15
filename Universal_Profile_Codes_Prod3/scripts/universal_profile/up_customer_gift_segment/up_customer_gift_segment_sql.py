
def up_customer_gift_segment(QUERY_PARAMS):

    project_name = QUERY_PARAMS["project_name"]
    customer_data = QUERY_PARAMS["dataset_name1"]
    dataset_name_dssprod = QUERY_PARAMS["dataset_name_dssprod"]

    bql = f"""
        create or replace table {project_name}.{customer_data}.up_customer_gift_segment as
        
        with base as 
        /* Transaction data (jewelry purchases) with customer's gender, product_gender, shipping and billing address */
        (
        SELECT distinct 
        ed.EMAIL_ADDRESS, 
        embd.bnid, 
        uph.email_address_key,
        uph.order_date, 
        uph.basket_id, 
        pof.ship_to_address_key, 
        pof.bill_to_address_key, 
        uph.offer_id, 
        bncf.first_name, 
        bncf.last_name, 
        lower(trim(bncf.gender)) as gender, 
        lower(trim(bncf.derived_gender)) as derived_gender,
        lower(trim(coalesce(bncf.gender, bncf.derived_gender))) as gender_final, 
        bncf.age,
        od.OFFER_KEY, 
        od.name, 
        od.MERCH_CATEGORY_ROLLUP, 
        lower(substr(trim(od.TARGET_GENDER),1,1)) as offer_target_gender

        FROM 
            (
            Select *except(offer_id), coalesce(offer_id, setting_offer_id) as offer_id 
            from `{project_name}.{customer_data}.up_purchase_history` where return_date is null) uph   /* purchase_history as base table for transactions */
            left join `{project_name}.{dataset_name_dssprod}.product_order_fact` pof 	          /* shipping and billing address key taken from product_order_fact */
            on pof.email_address_key= uph.email_address_key
            and parse_DATE('%Y%m%d',CAST(pof.order_date_key AS string))= uph.order_date
            and uph.basket_id=pof.basket_id
            and pof.offer_key=uph.offer_id
            left join `{project_name}.{dataset_name_dssprod}.email_address_dim` ed 
            on uph.email_address_key = ed.EMAIL_ADDRESS_KEY 
            left join
                (
                select distinct email_address, bnid 
                from `{project_name}.{customer_data}.email_customer_profile`
                ) embd 
                /* email_address to bnid mapping from ecp table*/
                on embd.email_address = ed.email_address
            left join `{project_name}.{customer_data}.bn_customer_profile` bncf on embd.bnid = bncf.bnid 
            /*customer's gender from bncf table*/
            left join `{project_name}.{dataset_name_dssprod}.offer_dim` od on od.OFFER_KEY = uph.offer_id
            /*product_gender and category from offer_dim table*/
            where od.MERCH_CATEGORY_ROLLUP in ('Diamond Jewelry','Other Jewelry', 'Bands')
            /*filtering for only jewelry purchases*/
        )

        /*Tagging each transaction as either gift or self_gift transaction based on rule set */

        ,trans_level_tb as
        (
        select *, extract(year from order_date) as order_yr

        -- if both rule1 and rule2 are false and shipping and billing address are same(and non-null), then that transaction is tagged as self_gift_transaction

        , case when product_gender_rule_flag = 0 and address_rule_flag = 0 and ship_to_address_key is not null and bill_to_address_key is not null
          then 1 else 0 end as self_gifter_flag
          
        -- if either of rule1 or rule2 is true, then that transaction is tagged as gift_transaction 
         
        , case when product_gender_rule_flag = 1 or address_rule_flag = 1 
          then 1 else 0 end as gifter_flag 
        from
            (
            select *
            /*Rule 1 :If customer's gender and product's target gender are opposite, then rule1 holds and transaction is a gift transaction */

            ,case when gender_final= 'm' and offer_target_gender = 'f' then 1 
                  when gender_final= 'f' and offer_target_gender = 'm' then 1
                  else 0 end as product_gender_rule_flag
                  
            /*Rule 2 :If shipping_address and billing address are different, then rule2 holds and transaction is a gift transaction */ 
                 
            ,case when ship_to_address_key != bill_to_address_key then 1 
                  else 0 end as address_rule_flag
            from base
            )
        )

        /* rolling transaction level tagging data on bnid to get tagging on bnid level */
        ,bnid_level_tb as 
        (
        select distinct bnid, bnid_product_gender_rule_flag, bnid_address_rule_flag,bnid_self_gifter_flag,bnid_gifter_flag,
        trans_cnt_product_gender_differ, trans_cnt_address_differ, trans_cnt_self_gifter, trans_cnt_gifter
        ,total_transactions
        from
            (
            select *
            ,max(product_gender_rule_flag) over (partition by bnid) as bnid_product_gender_rule_flag
            ,max(address_rule_flag) over (partition by bnid) as bnid_address_rule_flag
            ,max(self_gifter_flag) over (partition by bnid) as bnid_self_gifter_flag
            ,max(gifter_flag) over (partition by bnid) as bnid_gifter_flag
            ,sum(product_gender_rule_flag) over (partition by bnid) as trans_cnt_product_gender_differ
            ,sum(address_rule_flag) over (partition by bnid) as trans_cnt_address_differ
            ,sum(self_gifter_flag) over (partition by bnid) as trans_cnt_self_gifter
            ,sum(gifter_flag) over (partition by bnid) as trans_cnt_gifter

            ,count(*) over (partition by bnid) as total_transactions
            from trans_level_tb
            where bnid is not null
            )
        )

        /*Tagging of active BNIDs in different windows
        Active definition- 
          1) BNID must be subscribed as of today(current_date)
          2) BNID must have opened any email in that particular window 
        Two windows are taken- 1)last_1_year 2)after_1_year but before_2_year*/

        /*BNIDs who opened any email in last 2 years*/
        , open_email as
        (
        select * 
        from
            (
            SELECT distinct ecp.bnid, parse_DATE('%Y%m%d',CAST(date_key as string)) as activity_date
            FROM 
                (Select distinct email_address_key, date_key 
                from `{project_name}.{dataset_name_dssprod}.email_open_fact`) eof
            join `{project_name}.{dataset_name_dssprod}.email_address_dim` ead
            on eof.email_address_key= ead.email_address_key
            join (select distinct bnid, email_address 
            from `{project_name}.{customer_data}.email_customer_profile`) ecp
            on ead.email_address = ecp.email_address
            WHERE parse_DATE('%Y%m%d',CAST(date_key as string)) between date_sub('2020-06-17', interval 1 year) and '2020-06-16'
            /*email open data before 2020-06-17 taken from dssprod */
            )
            union distinct
            (
                SELECT distinct eof.bnid, eof.activity_date
                FROM `{project_name}.o_warehouse.email_open_fact` eof
            )
        )

        /*BNIDs active in last 1 year window*/
        ,active_1year_bnid as
        (
        Select distinct oe.bnid
        From 
            (select * from open_email where activity_date between date_sub(current_date(), interval 1 year) and current_date() ) oe
            join 
            (Select distinct bnid, subscribe_flag, from `{project_name}.{customer_data}.email_customer_profile` where subscribe_flag='Y' ) ecp
            on oe.bnid=ecp.bnid
        )

        /*BNIDs active after_1_year but before 2 year window */
        ,active_btw_1and2_year_bnid as
        (
        Select distinct oe.bnid
        From
            (select * from open_email 
            where activity_date < date_sub(current_date(), interval 1 year) and activity_date >= date_sub(current_date(), interval 2 year)  ) oe
            join 
            (Select distinct bnid, subscribe_flag from `{project_name}.{customer_data}.email_customer_profile` where subscribe_flag='Y' ) ecp
            on oe.bnid=ecp.bnid
        )

        /* Final output table with tagging */
        ,final_output as
        (
        select *except(row_num)
        from
          (
          select *, row_number() over (partition by bnid order by subscribe_flag desc, email_address) as row_num
          from
            (
            select distinct
            bnid,
            email_address,
            subscribe_flag,
            bnid_gifter_flag as gifter_flag,
            bnid_self_gifter_flag as self_gifter_flag,
            active_1year_flag,
            active_btw_1and2year_flag,
            from
              (
              select bnlt.*, ss.email_address, ss.subscribe_flag,
                case when ac_1yr.bnid is not null then 1 else 0 end as active_1year_flag,
                case when ac_1and2yr.bnid is not null then 1 else 0 end as active_btw_1and2year_flag,

              from bnid_level_tb bnlt
              left join active_1year_bnid ac_1yr on bnlt.bnid = ac_1yr.bnid
              left join active_btw_1and2_year_bnid ac_1and2yr on bnlt.bnid = ac_1and2yr.bnid
              left join (Select distinct bnid, email_address, subscribe_flag from `{project_name}.{customer_data}.email_customer_profile`) ss on bnlt.bnid = ss.bnid
              )
            )
          )
        where row_num = 1
        )
        
        select * from final_output""".format(
        project_name,
        customer_data,
        dataset_name_dssprod
        )
    return bql
