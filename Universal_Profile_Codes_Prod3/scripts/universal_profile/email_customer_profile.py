def bn_dna_data(params):
    project_name = params["project_name"]
    main_query = bn_dna_data_query(project_name)
    return main_query


def bn_cust_dna(params):
    project_name = params["project_name"]
    main_query = bn_cust_dna_query(project_name)
    return main_query


def email_customer_profile(params):
    project_name = params["project_name"]
    main_query = email_customer_profile_query(project_name)
    return main_query


def email_customer_profile_query(project_name):
    bql3 = f'''
    drop table if exists `{project_name}.up_stg_customer.email_customer_profile_stg`;
    create table `{project_name}.up_stg_customer.email_customer_profile_stg` as
    select distinct b.email_address
    ,a.bnid
    ,a.up_email_address_key
    ,a.email_address_test_key
    ,a.bn_test_key
    ,case when a.subscribe_flag = 'Y'
            and ifnull(fraud_flag,'N') = 'N' and ifnull(never_send_flag, 'N') = 'N'
            and ifnull(bad_address_flag,'N') = 'N' and undeliverable_date is null then 'Y'
          else 'N' 
        end email_contactable_flag
    ,a.primary_site
    ,a.primary_hostname
    ,ifnull(a.primary_language_code,'en-us') as primary_language_code
    ,b.country_code
    ,b.phone
    ,a.primary_currency_code
    ,a.region
    ,a.queens_english_flag
    --Customer Store Proximity to dma zip additions----5/26/2021
    ,a.primary_dma_zip
    ,a.primary_dma_source 
    ,a.nearest_store_1_webroom_key
    ,a.nearest_store_2_webroom_key
    ,a.nearest_store_3_webroom_key
    --------------------------------------------------
    ,dna.cust_dna  as cust_dna
    ,a.subscribe_flag
    ,a.email_open_last_dt
    ,a.last_subscribe_date
    ,a.browse_last_dt
    ,a.never_send_flag
    ,a.fraud_flag
    ,a.bad_address_flag
    ,a.undeliverable_date
    ,a.sweepstakes_entry_last_dt
    ,a.first_promo_subscribe_date
    ,a.pending_order_flag
    ,a.order_cnt
    ,a.order_last_dt
    ,a.birth_date 
    ,a.wedding_date
    ,case when c.bucket='LC' then 'Y' else 'N' end as pj_lc_flag 
    ,sc.e_sc_most_viewed_offer_id_1 as e_sc_most_viewed_offer_id_1
    ,lc.e_lc_most_viewed_setting_OID_1 as e_lc_most_viewed_setting_OID_1
    ,lc.e_lc_most_viewed_diamond_sku_1 as e_lc_most_viewed_diamond_sku_1
    ,a.customer_segment_exact
    from `{project_name}.o_customer.up_explicit_attributes` a 
    join
    `{project_name}.o_customer.up_customer_attributes` b 
    on a.up_email_address_key =b.up_email_address_key 
    left join 
    `{project_name}.o_customer.bn_pj_tagging` c
    on a.bnid=c.bnid and c.bucket='LC'
    left join `{project_name}.up_stg_customer.bn_cust_dna` dna
    on dna.up_email_address_key=a.up_email_address_key
	left join `{project_name}.o_customer.bn_lc_pj_attributes` lc
    on a.bnid=lc.bnid
    left join `{project_name}.o_customer.bn_sc_pj_attributes` sc
    on a.bnid=sc.bnid
    ;
    '''.format(project_name)
    return bql3


def bn_cust_dna_query(project_name):
    bql2 = f'''
    drop table if exists `{project_name}.up_stg_customer.bn_cust_dna`;
    create table `{project_name}.up_stg_customer.bn_cust_dna`  as
    select bnid,up_email_address_key,
    concat(
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=1),  ":",      ifnull(buyer,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=2),  ":",      ifnull(pj_lc,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=3),  ":",      ifnull(pj_j4l,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=4),  ":",      ifnull(pj_sc,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=5),  ":",      ifnull(pj_ppa,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=6),  ":",      ifnull(eng_prop,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=7),  ":",      ifnull(band_prop,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=8),  ":",      ifnull(oj_prop,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=9),  ":",      ifnull(dj_prop,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=10), ":",      ifnull(email_status,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=11), ":",      ifnull(cast(last_site as STRING),''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=12), ":",      ifnull(account_status,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=13), ":",      ifnull(r_e,''), "|",    
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=14), ":",      ifnull(f_e,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=15), ":",      ifnull(m_e,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=16), ":",      ifnull(r_j,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=17), ":",      ifnull(f_j,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=18), ":",      ifnull(m_j,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=19), ":",      ifnull(gender,''),"|",    
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=20), ":",      ifnull(region,''),"|",     
    (SELECT key FROM `{project_name}.o_customer.customer_dna_dim` where customer_dna_key=21), ":",      ifnull(email_sub,'')
    ) as cust_dna
    from `{project_name}.up_stg_customer.bn_dna_data`
    '''.format(project_name)
    return bql2


def bn_dna_data_query(project_name):
    bql1 = f'''
    drop table if exists `{project_name}.up_stg_customer.bn_dna_data` ;
    create table `{project_name}.up_stg_customer.bn_dna_data` as 
    select bc.bnid, ex.up_email_address_key,
           case when be.order_cnt>0 then 'B' else 'P' end as buyer,
           coalesce(safe.substr(lc.i_lc_propensity_overall_phase,0,1),'') as pj_lc,
           '' as pj_j4l,
           coalesce(safe.substr(sc.i_sc_propensity_overall_phase,0,1),'') as pj_sc,
           '' as pj_ppa,
           '' as eng_prop,

           coalesce((select coalesce(safe.substr(sccp.propensity_phase,0,1),'') as pj_sccp_wedding 
            from 
            (
            select row_number() over (partition by bnid order by model_run_date desc) rn,propensity_phase, bnid
            from `bnile-cdw-prod.o_customer.bn_sc_category_propensity` 
            where  product_category="wedding" 
            order by model_run_date desc
            )sccp where rn=1 and sccp.bnid=bc.bnid),'') as band_prop,

           coalesce((select coalesce(safe.substr(sccp.propensity_phase,0,1),'') as pj_sccp_oj
            from 
            (
            select row_number() over (partition by bnid order by model_run_date desc) rn,propensity_phase, bnid
            from `bnile-cdw-prod.o_customer.bn_sc_category_propensity` 
            where  product_category="oj" 
            order by model_run_date desc
            )sccp where rn=1 and sccp.bnid=bc.bnid),'') as oj_prop,   

           coalesce((select coalesce(safe.substr(sccp.propensity_phase,0,1),'') as pj_sccp_dj
            from 
            (
            select row_number() over (partition by bnid order by model_run_date desc) rn,propensity_phase, bnid
            from `bnile-cdw-prod.o_customer.bn_sc_category_propensity` 
            where  product_category="dj" 
            order by model_run_date desc
            )sccp where rn=1 and sccp.bnid=bc.bnid),'') as dj_prop,

           CASE
             WHEN DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y-%m-%d', CAST(be.email_open_last_dt AS STRING))),DAY)
             <90 THEN 'A'
           ELSE
             'L'
           END  AS email_status,
           ifnull(DATE_DIFF(CURRENT_DATE(),(PARSE_DATE('%Y-%m-%d', CAST(be.browse_last_dt AS STRING))),DAY),0)as 
           last_site,
           be.account_holder_flag as account_status,
           case when br.rfm_e_r=3 then 'H' when br.rfm_e_r=2 then 'M' else 'L' end as r_e ,
           case when br.rfm_e_f=3 then 'H' when br.rfm_e_f=2 then 'M' else 'L' end as f_e,
           case when br.rfm_e_m=3 then 'H' when br.rfm_e_m=2 then 'M' else 'L' end as m_e,
           case when br.rfm_j_r=3 then 'H' when br.rfm_j_r=2 then 'M' else 'L' end as r_j,
           case when br.rfm_j_f=3 then 'H' when br.rfm_j_f=2 then 'M' else 'L' end as f_j,
           case when br.rfm_j_m=3 then 'H' when br.rfm_j_m=2 then 'M' else 'L' end as m_j,
           ifnull(bc.derived_gender,'U') as gender,
           be.region,
             ex.subscribe_flag as email_sub
    from 
    `{project_name}.o_customer.bn_customer_profile` bc
    join
    `{project_name}.o_customer.bn_explicit_attributes` be
    on bc.bnid =be.bnid 
    left join `{project_name}.o_customer.bn_rfm` br		
    on bc.bnid=br.bnid
    left join `{project_name}.o_customer.up_explicit_attributes` ex
    on ex.bnid=be.bnid
    left join `bnile-cdw-prod.o_customer.bn_lc_pj_attributes` lc
    on bc.bnid=lc.bnid
    left join `bnile-cdw-prod.o_customer.bn_sc_pj_attributes` sc
    on bc.bnid=sc.bnid
    ;
    '''.format(project_name)
    return bql1