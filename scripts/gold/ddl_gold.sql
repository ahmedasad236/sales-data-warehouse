
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
drop view if exists gold.dim_customers;
create view gold.dim_customers as 
select
row_number() over (order by cst_id) as customer_key,
cci.cst_id as customer_id,
cci.cst_key as customer_number,
cci.cst_first_name as first_name,
cci.cst_last_name as last_name,
ela.cntry as country,
cci.cst_marital_status as marital_status,
case when cci.cst_gndr != 'n/a' then cci.cst_gndr -- CRM is the master for gender info
else coalesce(ecaz.gen, 'n/a')
end as gender,
ecaz.bdate as birthdate,
cci.cst_create_date as create_date

from silver.crm_cst_info cci
left join silver.erp_cust_az12 ecaz on ecaz.cid  = cci.cst_key
left join silver.erp_loc_a101 ela on cci.cst_key  = ela.cid;


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
drop view if exists gold.dim_products;
create view gold.dim_products as
select
row_number() over (order by cpi.prd_start_dt) as product_key,
cpi.prd_id as product_id,
cpi.prd_key as product_number,
cpi.prd_nm as product_name,
cpi.cat_id as category_id,
epcgv.cat as category,
epcgv.subcat subcategory,
epcgv.maintenance,
cpi.prd_cost as cost,
cpi.prd_line as product_line,
cpi.prd_start_dt as start_date
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epcgv
on cpi.cat_id = epcgv.id 
where cpi.prd_end_dt is null; -- Filter out all historical data


-- =============================================================================
-- Create Fact: gold.fact_sales
-- =============================================================================
drop view if exists gold.fact_sales;
create view gold.fact_sales as
select
csd.sls_ord_num as order_number,
dc.customer_key,
dp.product_key,
csd.sls_order_dt as order_date,
csd.sls_ship_dt as ship_date,
csd.sls_due_dt as due_date,
csd.sls_sales as sales_amount,
csd.sls_quantity as quantity,
csd.sls_price as price
from silver.crm_sales_details csd
left join gold.dim_products dp 
on csd.sls_prd_key  = dp.product_number
left join gold.dim_customers dc 
on csd.sls_cust_id = dc.customer_id;