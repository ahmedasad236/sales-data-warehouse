/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    step_start_time TIMESTAMP;
    step_end_time TIMESTAMP;
BEGIN
    -- Record total batch start time
    batch_start_time := clock_timestamp();
    
    RAISE NOTICE '========================================================================';
    RAISE NOTICE 'STARTING BATCH EXECUTION: PROCEDURE silver.load_silver()';
    RAISE NOTICE '========================================================================';

    ---------------------------------------------------------------------------
    -- 1. Load silver.crm_cst_info
    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.crm_cst_info';
    step_start_time := clock_timestamp();
    
    RAISE NOTICE '  >> Truncating Table: silver.crm_cst_info';
    TRUNCATE TABLE silver.crm_cst_info;

    RAISE NOTICE '  >> Inserting Data into: silver.crm_cst_info';
    INSERT INTO silver.crm_cst_info (
        cst_id, cst_key, cst_first_name, cst_last_name, cst_gndr, cst_marital_status, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        trim(cst_first_name) as cst_first_name,
        trim(cst_last_name) as cst_last_name,
        case when upper(trim(cst_gndr)) = 'M' then 'Male'
             when upper(trim(cst_gndr)) = 'F' then 'Female'
             else 'n/a' 
        end as cst_gndr,
        case when upper(trim(cst_marital_status)) = 'M' then 'Married'
             when upper(trim(cst_marital_status)) = 'S' then 'Single'
             else 'n/a' 
        end as cst_marital_status,
        cst_create_date
    FROM (
        SELECT *,
        row_number() over(partition by cci.cst_id order by cci.cst_create_date desc) as flag_last
        FROM bronze.crm_cust_info cci
    ) as cst_partition
    WHERE cst_id is not null and flag_last = 1;

    step_end_time := clock_timestamp();
    RAISE NOTICE '  >> SUCCESS! Duration for silver.crm_cst_info: %', (step_end_time - step_start_time);


    ---------------------------------------------------------------------------
    -- 2. Load silver.crm_prd_info
    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.crm_prd_info';
    step_start_time := clock_timestamp();

    RAISE NOTICE '  >> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '  >> Inserting Data into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info  (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
        prd_id,
        replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
        substring(prd_key, 7, length(prd_key)) as prd_key,
        prd_nm,
        coalesce(prd_cost, 0) as prd_cost,
        case UPPER(trim(prd_line))
            when 'M' then 'Mountain'
            when 'R' then 'Road'
            when 'S' then 'Other Sales'
            when 'T' then 'Touring'
            else 'n/a'
        end as prd_line,
        prd_start_dt::date as prd_start_dt,
        ((lead(prd_start_dt) over(partition by prd_key order by prd_start_dt))::date - 1) as prd_end_dt
    FROM bronze.crm_prd_info cpi;

    step_end_time := clock_timestamp();
    RAISE NOTICE '  >> SUCCESS! Duration for silver.crm_prd_info: %', (step_end_time - step_start_time);


    ---------------------------------------------------------------------------
    -- 3. Load silver.crm_sales_details
    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.crm_sales_details';
    step_start_time := clock_timestamp();

    RAISE NOTICE '  >> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '  >> Inserting Data into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt = 0 or length(sls_order_dt::text) != 8 then null 
             else sls_order_dt::varchar::date end as sls_order_dt,
        case when sls_ship_dt = 0 or length(sls_ship_dt::text) != 8 then null 
             else sls_ship_dt::varchar::date end as sls_ship_dt,
        case when sls_due_dt = 0 or length(sls_due_dt::text) != 8 then null 
             else sls_due_dt::varchar::date end as sls_due_dt,
        case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
             then sls_quantity * abs(sls_price) 
             else sls_sales end as sls_sales,
        sls_quantity,
        case when sls_price is null or sls_price <= 0
             then sls_sales / nullif(sls_quantity, 0)
             else sls_price end as sls_price
    FROM bronze.crm_sales_details csd;

    step_end_time := clock_timestamp();
    RAISE NOTICE '  >> SUCCESS! Duration for silver.crm_sales_details: %', (step_end_time - step_start_time);


    ---------------------------------------------------------------------------
    -- 4. Load silver.erp_cust_az12
    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.erp_cust_az12';
    step_start_time := clock_timestamp();

    RAISE NOTICE '  >> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '  >> Inserting Data into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT 
        case when cid like 'NAS%' then substring(cid, 4) else cid end cid,
        case when bdate > NOW()::date then null else bdate end bdate,
        case when UPPER(Trim(gen)) in ('F', 'FEMALE') then 'Female'
             when UPPER(Trim(gen)) in ('M', 'MALE') then 'Male'
             else 'n/a'
        end gen
    FROM bronze.erp_cust_az12;

    step_end_time := clock_timestamp();
    RAISE NOTICE '  >> SUCCESS! Duration for silver.erp_cust_az12: %', (step_end_time - step_start_time);


    ---------------------------------------------------------------------------
    -- 5. Load silver.erp_loc_a101
    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.erp_loc_a101';
    step_start_time := clock_timestamp();

    RAISE NOTICE '  >> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '  >> Inserting Data into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT 
        replace(cid, '-', '') cid,
        case when trim(cntry) = 'DE' then 'Germany'
             when trim(cntry) in ('US', 'USA') then 'United States'
             when trim(cntry) = '' or cntry is null then 'n/a'
             else trim(cntry)
        end cntry
    FROM bronze.erp_loc_a101 ela;

    step_end_time := clock_timestamp();
    RAISE NOTICE '  >> SUCCESS! Duration for silver.erp_loc_a101: %', (step_end_time - step_start_time);


    ---------------------------------------------------------------------------
    -- 6. Load silver.erp_px_cat_g1v2
    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.erp_px_cat_g1v2';
    step_start_time := clock_timestamp();

    RAISE NOTICE '  >> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '  >> Inserting Data into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT 
        id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2 epcgv;

    step_end_time := clock_timestamp();
    RAISE NOTICE '  >> SUCCESS! Duration for silver.erp_px_cat_g1v2: %', (step_end_time - step_start_time);

    ---------------------------------------------------------------------------
    -- End of Procedure Total Logging
    ---------------------------------------------------------------------------
    batch_end_time := clock_timestamp();
    RAISE NOTICE '========================================================================';
    RAISE NOTICE 'EXECUTION COMPLETED: PROCEDURE silver.load_silver()';
    RAISE NOTICE 'TOTAL DURATION: %', (batch_end_time - batch_start_time);
    RAISE NOTICE '========================================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '========================================================================';
        RAISE NOTICE '!!! ERROR DURING EXECUTION OF silver.load_silver() !!!';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'SQL State: %', SQLSTATE;
        RAISE NOTICE '========================================================================';
        -- Re-raise the error to ensure the calling application knows it failed and the transaction rolls back
        RAISE;
END;
$$;

call silver.load_silver();