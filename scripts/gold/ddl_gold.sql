/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_info', 'V') IS NOT NULL
    DROP VIEW gold.dim_info;
GO

CREATE VIEW silver.dim_full AS
WITH unique_combinations AS (
    SELECT DISTINCT
        a.account,
        a.sector,
        a.year_established,
        a.revenue,
        a.employees,
        a.office_location,
        a.subsidiary_of,
        p.product,
        p.series,
        p.sales_price,
        s.sales_agent,
        s.manager,
        s.regional_office
    FROM silver.accounts a
    INNER JOIN silver.sales_pipeline sp
        ON a.account = sp.account
    INNER JOIN silver.products p
        ON sp.product = p.product
    INNER JOIN silver.sales_teams s
        ON sp.sales_agent = s.sales_agent
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY account, product, sales_agent) AS dim_id,
    *
FROM unique_combinations;


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sp.opportunity_id,
    sp.account,
    sp.product,
    sp.sales_agent,
    sp.engage_date,
    sp.close_date,
    sp.close_value,
    sp.deal_stage,
    a.sector,
    p.series,
    s.regional_office
FROM silver.sales_pipeline sp
INNER JOIN silver.accounts a      
    ON sp.account = a.account
INNER JOIN silver.products p      
    ON sp.product = p.product
INNER JOIN silver.sales_teams s   
    ON sp.sales_agent = s.sales_agent;
GO
