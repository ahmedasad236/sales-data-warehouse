
select cid from bronze.erp_cust_az12
where cid like 'NAS%'

select bdate from bronze.erp_cust_az12
where bdate > now()::date
or bdate < '1900-01-01'

select distinct gen from bronze.erp_cust_az12