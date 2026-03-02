/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_row_count INT;
    v_total_start_time TIMESTAMP;
    v_step TEXT; -- Tracks the current operation for error logging
BEGIN
    -- Record total start time
    v_total_start_time := clock_timestamp();

    RAISE NOTICE '============================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '============================================';

    ----------------------------------------------------------------
    -- 1. Loading CRM Tables
    ----------------------------------------------------------------
    RAISE NOTICE '--------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '--------------------------------------------';

    -- CRM_CUST_INFO
    v_step := 'Loading CRM_CUST_INFO'; -- Update step before execution
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/mnt/datasets/source_crm/cust_info.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Loaded % rows. Duration: %', v_row_count, v_duration;

    -- CRM_PRD_INFO
    v_step := 'Loading CRM_PRD_INFO';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/mnt/datasets/source_crm/prd_info.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Loaded % rows. Duration: %', v_row_count, v_duration;

    -- CRM_SALES_DETAILS
    v_step := 'Loading CRM_SALES_DETAILS';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/mnt/datasets/source_crm/sales_details.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Loaded % rows. Duration: %', v_row_count, v_duration;

    ----------------------------------------------------------------
    -- 2. Loading ERP Tables
    ----------------------------------------------------------------
    RAISE NOTICE '--------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '--------------------------------------------';

    -- ERP_CUST_AZ12
    v_step := 'Loading ERP_CUST_AZ12';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM '/mnt/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Loaded % rows. Duration: %', v_row_count, v_duration;

    -- ERP_LOC_A101
    v_step := 'Loading ERP_LOC_A101';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM '/mnt/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Loaded % rows. Duration: %', v_row_count, v_duration;

    -- ERP_PX_CAT_G1V2
    v_step := 'Loading ERP_PX_CAT_G1V2';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM '/mnt/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Loaded % rows. Duration: %', v_row_count, v_duration;
	
    ----------------------------------------------------------------
    -- Final Summary
    ----------------------------------------------------------------
    v_duration := clock_timestamp() - v_total_start_time;
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Bronze Layer Loaded Successfully.';
    RAISE NOTICE 'Total Duration: %', v_duration;
    RAISE NOTICE '============================================';

EXCEPTION
    WHEN OTHERS THEN
        -- If any error occurs, this block runs
        RAISE NOTICE '============================================';
        RAISE NOTICE '!!! ERROR OCCURRED !!!';
        RAISE NOTICE 'Failed at Step: %', v_step;
        RAISE NOTICE 'Error Message : %', SQLERRM;
        RAISE NOTICE 'Error Code    : %', SQLSTATE;
        RAISE NOTICE '============================================';
        
        -- Re-raise the error so the calling application/job knows it failed
        RAISE;
END;
$$;

call bronze.load_bronze();