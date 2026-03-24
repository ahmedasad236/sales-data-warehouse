

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
from silver.crm_sales_details csd 
where csd.sls_ord_num != trim(csd.sls_ord_num)


----

-- check for invalid date orders
select sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt
from silver.crm_sales_details
where  sls_order_dt > sls_ship_dt
or sls_order_dt > sls_due_dt
or sls_ship_dt > sls_due_dt

-------

-- check for negative values in sls_sales, sls_quantity, sls_price
select sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_price * sls_quantity
or sls_sales is null or sls_price is null or sls_quantity is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by 1, 2, 3 desc