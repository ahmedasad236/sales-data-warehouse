
select *
from bronze.erp_px_cat_g1v2 epcgv 
where id not in (select distinct cat_id from silver.crm_prd_info cpi )

select *
from bronze.erp_px_cat_g1v2 epcgv 
where trim(cat) != cat

select *
from bronze.erp_px_cat_g1v2 epcgv 
where trim(subcat) != subcat

select *
from bronze.erp_px_cat_g1v2 epcgv 
where trim(maintenance) != maintenance

select distinct cat from bronze.erp_px_cat_g1v2
order by cat


select distinct subcat from bronze.erp_px_cat_g1v2
order by subcat

select distinct maintenance from bronze.erp_px_cat_g1v2