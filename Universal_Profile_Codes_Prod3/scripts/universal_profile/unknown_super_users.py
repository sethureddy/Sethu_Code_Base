def unknown_users_data_aggregation(params):
    project_name = params["o_project"]
    customer_dataset_name = params["o_customer_dataset"]
    stage_dataset_name = params["o_stg_dataset"]
    execution_date = params["EXECUTION_DATE"]
    output_table_name1 = params["output_table"]

    main_query = unknown_users_data(project_name,customer_dataset_name,stage_dataset_name,execution_date,output_table_name1)
    return main_query

def unknown_users_data(project_name,customer_dataset_name,stage_dataset_name,execution_date,output_table_name1):
    bql1 = f'''
    drop table if exists `{project_name}.{stage_dataset_name}.{output_table_name1}`;
    create table `{project_name}.{stage_dataset_name}.TBN_superusers_intermediate` as select * from(
    with browse as(
    SELECT
    guid, 
    cf.date_key, 
    cf.guid_key, 
    offer_key, 
    bnid,
    Primary_browse_segment, 
    secondary_browse_segment,
    Total_browse_count, 
    page_group_name,
    activity_set_code
    FROM ( 
    Select  
    count(guid_key) over (partition by guid_key) as Total_browse_count ,guid_key,  date_key, offer_key, time_key, ip_key,PAGE_GROUP_KEY, uri_key
    from  `{project_name}.dssprod_o_warehouse.click_fact` 
              where parse_date('%Y%m%d', cast(date_key AS string)) between 
              DATE_SUB(parse_date('%Y%m%d', cast({execution_date} AS string)),interval (select time_interval_in_days from `{project_name}.up_stg_customer.unknown_super_users_dim`) day) 
              and parse_date('%Y%m%d', cast({execution_date} AS string))) cf
    inner join (Select distinct guid_key, guid from `{project_name}.up_tgt_ora_tables.guid_dim_cf`) gd
    on cf.guid_key=gd.guid_key
    Left join (Select distinct customer_guid, bnid 
    from `{project_name}.up_stg_customer.bnid_guid_mapping` 
    ) bgm
    on  gd.guid=bgm.customer_guid
    Left join (Select distinct guid_key,EMAIL_ADDRESS_KEY 
    from `{project_name}.dssprod_o_warehouse.email_guid_map` ) egm
    on cf.guid_key= egm.guid_key
    LEFT JOIN `{project_name}.dssprod_o_warehouse.page_group_dim` pgd
               ON cf.PAGE_GROUP_KEY = pgd.PAGE_GROUP_KEY
    where bnid is null and egm.EMAIL_ADDRESS_KEY is null),
    category_flag as ( select *,Case WHEN (
                                (
                                    lower(primary_browse_segment) = 'engagement'
                                    AND lower(secondary_browse_segment) IN ('byor', 'byo3sr', 'preset', 'general')
                                    )
                                OR (
                                    lower(primary_browse_Segment) = 'diamond'
                                    AND lower(secondary_browse_segment) = 'loose_diamond'
                                    )
                                OR
                                    (
                                    lower(primary_browse_segment) = 'education'
                                AND lower(secondary_browse_segment) in ('engagement', 'diamond')
                                    )
                                )
                    THEN 1
                    ELSE 0
                    END AS Engagement_flag
                    ,case when (
                                        
                                    (
                                    lower(primary_browse_segment) in ('wedding_band','other_jewelry','misc_jewelry','diamond_jewelry')
                                    )
                                OR
                                    (
                                    lower(primary_browse_segment) = 'education'
                                   AND lower(secondary_browse_segment) in ('other_jewelry')
                                    )
                                )
              THEN 1
              ELSE 0
              END as short_conversion_flag
              ,CASE 
                        WHEN page_group_name LIKE '%chat%'
                        THEN 1
                        ELSE 0
                        END AS chat_flag
              ,CASE 
                        WHEN (activity_set_code LIKE '%catalog%' or  page_group_name LIKE '%catalog%')
                        THEN 1
                        ELSE 0
                        END AS catalog_flag
              ,CASE 
                        WHEN activity_set_code LIKE '%review%' or
                        (page_group_name like '%review%' and page_group_name not like '%preview%')
                        THEN 1
                        ELSE 0
                        END AS review_flag
              ,CASE 
                        WHEN activity_set_code LIKE '%detail%'
                        THEN 1
                        ELSE 0
                        END AS detail_flag
             ,CASE 
                        WHEN activity_set_code LIKE '%basket%'
                        THEN 1
                        ELSE 0
                        END AS basket_flag
    From browse),
    mastertable as (
    Select *, SC+LC as category_count, Total_browse_count*Days_appeared as importance_score
    From (
    select
    guid, guid_key,
                        max(Total_browse_count) as total_browse_count, 
                        count(distinct date_key) as days_appeared,
                        date_diff(parse_date('%Y%m%d',cast({execution_date} as string)),parse_date('%Y%m%d',cast(max(date_key)as string)), month) as months_elapsed_since_appeared,
                        max(Engagement_flag) as lc
                       ,max(short_conversion_flag)  as sc
                       ,max(basket_flag) as basket_flag
                       ,sum(chat_flag) as chat_count
                       ,sum(review_flag) as review_count
                       ,sum(detail_flag) as detail_view_count
                       ,sum(catalog_flag) as catalog_view_count
    From  category_flag
    group by guid, guid_key
    ))  
    Select * , case when basket_flag=1 then 'High'
                    when basket_flag=0 and (
                    (Total_browse_count>=(SELECT browse_count_suoi1 FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) or detail_view_count>=(SELECT detail_count_suoi1 FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) ) 
                    or
                    (Days_appeared>(SELECT day_cutoff_suoi FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) and (Total_browse_count>=(SELECT browse_count_suoi2 FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) or Detail_view_count>=(SELECT detail_count_suoi2 FROM `{project_name}.up_stg_customer.unknown_super_users_dim` )))
                    or 
                    (Days_appeared>(SELECT day_cutoff_suoi FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) and review_count>=(SELECT review_count_suoi FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) )
                    ) then 'Medium'
                    else 'Low'
                    end as propensity_phase
    From (
    Select *
    From mastertable
    where days_appeared>(SELECT day_cutoff_UOI FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) and Category_count>0 and (importance_score>(SELECT importance_score FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) or Total_browse_count>(SELECT browse_count_uoi FROM `{project_name}.up_stg_customer.unknown_super_users_dim` ) )
    ---UOI
    )
    );
    '''.format(project_name,customer_dataset_name,stage_dataset_name,execution_date,output_table_name1)
    return bql1