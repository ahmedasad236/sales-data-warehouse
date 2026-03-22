-- Check for nulls or duplicates in primary key
-- Exception: No records

select 
COUNT(*)
from bronze.crm_cst_info
group by cst_id 
having COUNT(*) > 1 or cst_id  is null

-----

-- Check for unwanted spaces in string columns
select cst_key
from bronze.crm_cst_info
where cst_key != trim(cst_key)

-----

-- Check for valid values in categorical columns
select distinct cst_marital_status
from bronze.crm_cst_info cci

select distinct cst_gndr
from bronze.crm_cst_info cci