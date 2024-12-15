def address(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	table_name = params["table_name"]
	
	bql = f"""with address_base as (
            SELECT bnid,bill_to_street ,bill_to_postal_code,ship_to_street ,ship_to_postal_code ,first_name,MIN(event_date) AS               first_event_date
            FROM (
            SELECT DISTINCT bnid,bill_to_street,bill_to_postal_code,ship_to_street                                        ,ship_to_postal_code,first_name,last_name,event_date
            FROM `{project_name}.{stg_data}.{table_name}`   
            WHERE bill_to_street IS NOT NULL AND bill_to_postal_code is not null and ship_to_street is not null 
                  and ship_to_postal_code is not null and first_name is not null
                 )
            GROUP BY bnid,bill_to_street ,bill_to_postal_code ,ship_to_street ,ship_to_postal_code ,first_name 

            ),address_match as (
            SELECT *
            FROM (
            SELECT bnid,address_base.bill_to_street ,address_base.bill_to_postal_code,address_base.ship_to_street ,
                   address_base.ship_to_postal_code   ,first_name,first_event_date,
                   ROW_NUMBER() OVER(PARTITION BY  bill_to_street ,bill_to_postal_code,ship_to_street, ship_to_postal_code,first_name ORDER BY first_event_date ASC) AS row_num1
            FROM address_base)
 
            ),address_input as (
            select distinct bnid,bill_to_street,bill_to_postal_code , ship_to_street ,ship_to_postal_code,first_name         ,first_event_date  
            from address_match 
            where row_num1 = 1 )

            select bnid,bill_to_street,bill_to_postal_code,ship_to_street,ship_to_postal_code,first_name,first_event_date,        
            from (
                    SELECT *,row_number() over (partition by bnid order by first_event_date) as row_num
                    FROM address_input 
                 )
            where row_num = 1""".format(
		project_name,
		stg_data,
		table_name,
	)
	return bql


def email_base1(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	table_name = params["table_name"]
	
	
	bql = f"""with email_base1_input as (
            select bnid,email_address,ship_to_postal_code,first_name,last_name,first_event_date 
            from (
            select *,row_number() over(partition by bnid order by first_event_date) as row_num1
            from (
            select bnid,email_address,ship_to_postal_code,first_name,last_name,min(event_date ) as first_event_date
            from (
            SELECT distinct bnid,email_address,ship_to_postal_code,last_4_digits ,gender,first_name,last_name,event_date   
            FROM `{project_name}.{stg_data}.{table_name}`
            where email_address is not null and (first_name is not null or last_name is not null) and ship_to_postal_code  is not null      
            )
            group by bnid,email_address,ship_to_postal_code,first_name,last_name
            ))
            where row_num1 = 1
            )

            select bnid,email_address,ship_to_postal_code,first_name,last_name,first_event_date
            from (
                     select *,row_number() over (partition by email_address,ship_to_postal_code,first_name,last_name) as row_num
                     from email_base1_input 
                     where first_name is not null and last_name is not null 
                 )
            where row_num = 1""".format(
		project_name,
		stg_data,
		table_name,
	)
	return bql


def email_base2(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	table_name = params["table_name"]
	
	bql = f"""with email_base2_input as (
        select bnid,email_address,bill_to_postal_code,bill_to_street,first_name,last_name,first_event_date
        from (
        select *,row_number() over(partition by bnid order by first_event_date) as row_num1
        from (
        select bnid,email_address,bill_to_postal_code,bill_to_street,first_name,last_name,min(event_date ) as first_event_date
        from (
        SELECT distinct bnid,email_address,bill_to_street,bill_to_postal_code ,gender,first_name,last_name,event_date
        FROM {project_name}.{stg_data}.{table_name}
        where email_address is not null and (first_name is not null or last_name is not null) and bill_to_street is not null
        and bill_to_postal_code is not null
        )
        group by bnid,email_address,bill_to_postal_code,bill_to_street,first_name,last_name
        ))
        where row_num1 = 1
        )

        select bnid,email_address,bill_to_postal_code,bill_to_street,first_name,first_event_date
        from (
        select *,row_number() over (partition by email_address,bill_to_street,first_name) as row_num
        from email_base2_input
        where first_name is not null and last_name is null
        )""".format(
		project_name,
		stg_data,
		table_name,
	)
	return bql


def payment(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	table_name = params["table_name"]
	
	bql = f"""with payment_base as (
            SELECT bnid,last_4_digits,bill_to_POSTAL_CODE,ship_to_postal_code,bill_to_street,first_name,min(event_date) as first_event_date
            from
            (
            SELECT DISTINCT bnid,last_4_digits,bill_to_POSTAL_CODE,ship_to_postal_code,bill_to_street,first_name,event_date
            FROM {project_name}.{stg_data}.{table_name}
            WHERE last_4_digits IS NOT NULL AND bill_to_POSTAL_CODE IS NOT NULL
            AND ship_to_POSTAL_CODE IS NOT NULL AND first_name IS NOT NULL
            )
            GROUP BY bnid,last_4_digits,bill_to_POSTAL_CODE,ship_to_postal_code,bill_to_street,first_name

            ),payment_match as
            (
            SELECT *
            FROM (
            SELECT bnid,last_4_digits,bill_to_POSTAL_CODE,ship_to_postal_code,bill_to_street,first_name,first_event_date,
            ROW_NUMBER() OVER(PARTITION BY last_4_digits, bill_to_POSTAL_CODE, ship_to_postal_code,bill_to_street, first_name ORDER BY first_event_date ASC) AS row_num1,
            FROM
            payment_base )
            ORDER BY last_4_digits,bill_to_POSTAL_CODE,first_name



            ),payment_input as (
            select distinct bnid,last_4_digits ,bill_to_postal_code ,ship_to_postal_code,bill_to_street,first_name,first_event_date
            from payment_match
            where row_num1 = 1
            )

            select bnid,last_4_digits,bill_to_postal_code,ship_to_postal_code,bill_to_street,first_name,first_event_date
            from (
            SELECT *,row_number() over (partition by bnid order by first_event_date) as row_num
            FROM payment_input
            )
            where row_num = 1""".format(
		project_name,
		stg_data,
		table_name,
	)
	return bql

def phone_base1(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	table_name = params["table_name"]
	
	bql = f"""with phone_no_base1 as (
            SELECT bnid,phone,first_name,MIN(event_date) AS first_event_date
            FROM (
            SELECT DISTINCT bnid,ifnull(phone,mobile) phone,first_name,event_date
            FROM `{project_name}.{stg_data}.{table_name}`   
            WHERE phone_number IS NOT NULL AND first_name IS NOT NULL AND last_name IS NULL 
                 )
            WHERE phone IS NOT NULL AND LENGTH(phone) <> 0
            GROUP BY bnid,phone,first_name 

            ),phone_no_base1_match as (
            SELECT *
            FROM (
            SELECT bnid,phone,first_name,first_event_date,
                   ROW_NUMBER() OVER(PARTITION BY phone, first_name ORDER BY first_event_date ASC) AS row_num1
            FROM phone_no_base1 )
            ORDER BY phone,first_name

            ),phone_no_base1_input as (
            select distinct bnid,phone,first_name,first_event_date  from phone_no_base1_match
            where row_num1 = 1
            )

            select bnid,phone,first_name,first_event_date 
            from (
                    SELECT *,row_number() over (partition by bnid order by first_event_date) as row_num
                    FROM phone_no_base1_input 
                  )
            where row_num = 1""".format(
		project_name,
		stg_data,
		table_name,
	)
	return bql
	
def phone_base2(params):

	project_name = params["project_name"]
	stg_data = params["stg_data"]
	table_name = params["table_name"]
	
	bql = f"""with phone_no_base2 as (
            SELECT bnid,phone,first_name,last_name,MIN(event_date) AS first_event_date
            FROM (
            SELECT DISTINCT bnid,ifnull(phone,mobile) phone,first_name,last_name,event_date
            FROM `{project_name}.{stg_data}.{table_name}`   
            WHERE phone_number IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL 
                 )
            WHERE phone IS NOT NULL AND LENGTH(phone) <> 0
            GROUP BY bnid,phone,first_name,last_name 

            ),phone_no_base2_match as (
            SELECT *
            FROM (
            SELECT bnid,phone,first_name,last_name,first_event_date,
                   ROW_NUMBER() OVER(PARTITION BY phone, first_name,last_name ORDER BY first_event_date ASC) AS row_num1
            FROM phone_no_base2  )
            ORDER BY phone,first_name,last_name

            ),phone_no_base2_input as (
            select distinct bnid,phone,first_name,last_name,first_event_date  from phone_no_base2_match
            where row_num1 = 1
            )

            select bnid,phone,first_name,last_name,first_event_date
            from (
                    SELECT *,row_number() over (partition by bnid order by first_event_date) as row_num
                    FROM phone_no_base2_input 
                  )
            where row_num = 1""".format(
		project_name,
		stg_data,
		table_name,
	)
	return bql