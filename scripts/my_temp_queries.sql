/*
    Quality check queries.
    List of useful queries for data validation and exploration.
*/
/*
use DataWarehouse;
GO

-- Query to check for nulls or duplicates in primary key
-- Expectations: No results
select 
    cst_id,
    count(*) as number_of_ocurrences
from bronze.crm_cust_info
GROUP BY cst_id
having count(*) > 1 or cst_id is null;
GO

-- In case of duplicate values, do a ranking to see which records are the latest, assuming there is a timestamp column.
-- This query can be done using the ROW_NUMBER() function over a partition by the primary key and ordered by a timestamp column.
-- Execute this query only if duplicates were found in the previous query.
select
    cst_id,
    cst_create_date,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_latest_record
from bronze.crm_cust_info
WHERE cst_id IN (
    select 
        cst_id
    from bronze.crm_cust_info
    GROUP BY cst_id
    having count(*) > 1 or cst_id is null
);

select
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_latest_record
from bronze.crm_cust_info
where cst_id is null

-- Query to check for unwanted spaces in string values in a given column.
-- Expectations: No results
SELECT 
    cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

-- Query to check data standarization and consistency for low cardinality columns
-- Expectations: No results
-- First find out the range of values
select distinct cst_marital_status from bronze.crm_cust_info; 
-- Second, do the
select cst_marital_status
from bronze.crm_cust_info
where cst_marital_status not in ('M', 'S');

-- Interesante: Baraa va haciendo una query que muestra la tabla ya corregida gradualmente con las cosas que va encontrando
-- Y después lo inserta en la tabla equivalente de silver
insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    select
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        case 
            when upper(trim(cst_marital_status)) = 'S' then 'Single'
            when upper(trim(cst_marital_status)) = 'M' then 'Married'
            else 'n/a' -- typical way to handle nulls for DWH projects
        end as cst_marital_status, -- Nota que se reescribe la col
        case
            when upper(trim(cst_gndr)) = 'F' then 'Female'
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            else 'n/a' -- typical way to handle nulls for DWH projects
        end as cst_gndr,
        cst_create_date
    from (
        select
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_latest_record
        from bronze.crm_cust_info
        where cst_id is NOT NULL
    ) as t
    WHERE flag_latest_record = 1;

select * from bronze.crm_prd_info;

-- Check for duplicates or nulls in pk - None!
select prd_id, count(*) from bronze.crm_prd_info GROUP BY prd_id having count(*) > 1 or prd_id is null;
*/
-- Check for duplicates or nulls in prd_key
-- select prd_key, count(*) from bronze.crm_prd_info GROUP BY prd_key having count(*) > 1 or prd_key is null;

-- Queries to manipulate strings
-- Task: Query to decompose the strings from prd_key into 2 different columns: prd_category and prd_key 
SELECT
    TRIM(prd_key) as old_prd_key,
    SUBSTRING(prd_key, 1, 5) as prd_category,
    SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key
from bronze.crm_prd_info;

-- Task: Query to change the format of prd_category. Replace '-' with '_'
-- So it matches with the format in table erp_px_cat_g1v2, col id
SELECT
    TRIM(prd_key) as old_prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as prd_category,
    SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key -- OJO aquí LEN()
from bronze.crm_prd_info;

-- Task: Query to remove the first 3 chars of some strings starting with NAS
SELECT  
    cid,
    case
        when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
        else cid
    end as new_cid
from bronze.erp_cust_az12

-- Task: Query to check for any unmatched data after applying the above transformation.
-- Interesting query to compare data between 2 different tables
-- Outcome: List of values from bronze.crm_prd_info which do not exist in bronze.erp_px_cat_g1v2
SELECT REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as prd_category
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
    (select distinct id from bronze.erp_px_cat_g1v2); 

-- Task: Check for nulls or negative numbers in the col prd_cost  
select prd_id, prd_cost from bronze.crm_prd_info where prd_cost < 0 or prd_cost is null;
--Task: Replace the nulls with 0 => ISNULL function
select
    prd_id,
    ISNULL(prd_cost, 0) as prd_cost
 from bronze.crm_prd_info;

 -- Task: Check the range of values of a given column
 select distinct prd_line from bronze.crm_prd_info;
 -- Task: Replace those chars with something more readable. Simple value mapping => CASE WHEN THEN END
 select
/*    case
        when prd_line='M' then 'Mountain' 
        when prd_line='R' then 'Road'
        when prd_line='S' then 'Other Sales'
        when prd_line='T' then 'Touring'
        else 'n/a' -- NULL case
    end as prd_line
*/
    case prd_line -- Alternative case syntax more effective
        when 'M' then 'Mountain'
        when 'R' then 'Road'
        when 'S' then 'Other Sales'
        when 'T' then 'Touring'
        else 'n/a'
    end as prd_line
from bronze.crm_prd_info;

-- Task: Check that end date is not earlier than start date
SELECT
    DATEDIFF(day, prd_start_dt, prd_end_dt) as datediff
from bronze.crm_prd_info
where DATEDIFF(day, prd_start_dt, prd_end_dt) < 0;

-- Task: Query to cast the data type. From DATETIME to DATE
SELECT
    CAST(prd_start_dt as DATE) as prd_start_dt
from bronze.crm_prd_info;

-- Task: CAST a column from int to date
SELECT
    case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
    else CAST(CAST(sls_order_dt as varchar) as date)
    end as new_sls_order_dt
from bronze.crm_sales_details