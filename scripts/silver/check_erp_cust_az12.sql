select cid from silver.erp_cust_az12
where cid like 'NAS%'

select bdate from silver.erp_cust_az12
where bdate > now()::date
or bdate < '1900-01-01'

select distinct gen from silver.erp_cust_az12