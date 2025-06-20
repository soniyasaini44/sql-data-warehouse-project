/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('silver.accounts', 'U') IS NOT NULL
    DROP TABLE silver.accounts;
GO

CREATE TABLE silver.accounts (
    account VARCHAR(100),
    sector VARCHAR(50),
    year_established INT,
    revenue DECIMAL(12,2),
    employees INT,
    office_location VARCHAR(100),
    subsidiary_of VARCHAR(100),
    created_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;
GO

CREATE TABLE silver.products (
    product VARCHAR(100),
    series VARCHAR(50),
    sales_price DECIMAL(10,2),
    created_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.sales_pipeline', 'U') IS NOT NULL
    DROP TABLE silver.sales_pipeline;
GO

CREATE TABLE silver.sales_pipeline (
    opportunity_id VARCHAR(20),
    sales_agent VARCHAR(100),
    product VARCHAR(100),
    account VARCHAR(100),
    deal_stage VARCHAR(50),
    engage_date DATE,
    close_date DATE,
    close_value DECIMAL(12,2),
    created_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.sales_teams', 'U') IS NOT NULL
    DROP TABLE silver.sales_teams;
GO

CREATE TABLE silver.sales_teams (
    sales_agent VARCHAR(100),
    manager VARCHAR(100),
    regional_office VARCHAR(50),
    created_date DATETIME DEFAULT GETDATE()
);
GO


