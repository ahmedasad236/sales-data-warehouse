/*
=============================================================
Create Schemas for Medallion Architecture
=============================================================
Script Purpose:
    This script sets up the organizational layers (schemas) 
    within the existing 'warehouse_db'.
*/

-- In Postgres, we just create schemas within the current DB
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Optional: Add a comment to verify it ran in the logs
COMMENT ON SCHEMA bronze IS 'Raw data layer';
COMMENT ON SCHEMA silver IS 'Cleansed data layer';
COMMENT ON SCHEMA gold   IS 'Reporting data layer';