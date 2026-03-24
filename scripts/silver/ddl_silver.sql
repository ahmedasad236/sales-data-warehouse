CREATE TABLE IF NOT EXISTS silver.crm_cst_info (
    cst_id int,
    cst_key varchar(30),
    cst_first_name varchar(30),
    cst_last_name varchar(30),
    cst_marital_status varchar(30),
    cst_gndr varchar(30),
    cst_create_date date,
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.crm_prd_info (
    prd_id int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt TIMESTAMP,
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.crm_sales_details (
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.crm_prd_info (
    prd_id int,
    prd_key varchar(50),
    cat_id varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt Date,
    prd_end_dt Date,
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_cust_az12 (
    cid varchar(50),
    bdate date,
    gen varchar(50),
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_loc_a101 (
    cid varchar(50),
    cntry varchar(50),
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2 (
    id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50),
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);