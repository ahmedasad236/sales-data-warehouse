CREATE TABLE IF NOT EXISTS bronze.crm_cst_info (
    cst_id int,
    cst_key varchar(30),
    cst_first_name varchar(30),
    cst_last_name varchar(30),
    cst_marital_status char,
    cst_gndr char,
    cst_create_date date
);

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
    prd_id int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);

CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
    cid varchar(50),
    bdate date,
    gen varchar(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
    cid varchar(50),
    cntry varchar(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
    id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50)
);