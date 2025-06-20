/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks for Null or duplicate primary key in the 'silver' layer.
Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- ====================================================================
-- Checking 'silver.accounts'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    account,
    COUNT(*) 
FROM silver.accounts
GROUP BY account
HAVING COUNT(*) > 1 OR account IS NULL;

-- ====================================================================
-- Checking 'silver.products'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    product,
    COUNT(*) 
FROM silver.products
GROUP BY product
HAVING COUNT(*) > 1 OR product IS NULL;

-- ====================================================================
-- Checking 'silver.sales_pipeline'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    opportunity_id,
    COUNT(*) 
FROM silver.sales_pipeline
GROUP BY opportunity_id
HAVING COUNT(*) > 1 OR opportunity_id IS NULL;

-- ====================================================================
-- Checking 'silver.sales_teams'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    sales_agent,
    COUNT(*) 
FROM silver.sales_teams
GROUP BY sales_agent
HAVING COUNT(*) > 1 OR sales_agent IS NULL;
