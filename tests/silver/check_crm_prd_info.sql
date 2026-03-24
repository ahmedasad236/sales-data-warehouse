/**
 * This script performs data quality checks on the silver.crm_prd_info table.
 * It checks for nulls or duplicates in the primary key, unwanted spaces in string columns,
 * and valid values in categorical columns.
 */

-- Check for nulls or duplicates in primary key
-- Exception: No records

select 
COUNT(*)
from silver.crm_prd_info
group by prd_id 
having COUNT(*) > 1 or prd_id  is null

-----

-- Check for unwanted spaces in string columns
select prd_key
from silver.crm_prd_info
where prd_key != trim(prd_key)

select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)

-----

-- check form NULLS or Negative values in prd_cost
select prd_cost
from silver.crm_prd_info
where prd_cost is null or prd_cost < 0
-----

-- check for invalid date orders
select prd_id, prd_start_dt, prd_end_dt
from silver.crm_prd_info
where  prd_start_dt > prd_end_dt

-----

-- Data Standardization: Check for valid values in prd_line
select distinct prd_line
from silver.crm_prd_info