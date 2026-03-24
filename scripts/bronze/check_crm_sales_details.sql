
/** 
This script is used to check the data quality of the crm_sales_details table in the bronze layer.
*/

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details csd 
where csd.sls_ord_num != trim(csd.sls_ord_num)


----

-- check for Invalid dates
select nullif(sls_order_dt, 0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
or length(sls_order_dt::text) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101


select nullif(sls_ship_dt, 0) as sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0
or length(sls_ship_dt::text) != 8
or sls_ship_dt > 20500101
or sls_ship_dt < 19000101

select nullif(sls_due_dt, 0) as sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0
or length(sls_due_dt::text) != 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101

-- check for invalid date orders
select sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt
from bronze.crm_sales_details
where  sls_order_dt > sls_ship_dt
or sls_order_dt > sls_due_dt
or sls_ship_dt > sls_due_dt

-------

-- check for negative values in sls_sales, sls_quantity, sls_price
select sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_price * sls_quantity
or sls_sales is null or sls_price is null or sls_quantity is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by 1, 2, 3 desc