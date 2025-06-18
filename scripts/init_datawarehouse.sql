-- 📦 Initialize Data Warehouse Setup

-- 1. Connect to the 'master' database to manage DB-level operations.
-- 2. If 'DataWarehouse' exists, set it to SINGLE_USER and drop it (for clean re-creation).
-- 3. Create a new 'DataWarehouse' database.
-- 4. Create separate schemas: 'bronze', 'silver', and 'gold' to follow the Medallion architecture.


USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
