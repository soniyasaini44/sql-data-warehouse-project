/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. 

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- ====================================================================
-- Checking 'gold.dim_info'
-- ====================================================================
-- Check for Uniqueness of dim_id in gold.dim_info
-- Expectation: No results 
SELECT 
    dim_id,
    COUNT(*) AS duplicate_count
FROM gold.dim_info
GROUP BY dim_id
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check for Uniqueness of opportunity_id in gold.fact_sales
-- Expectation: No results 
SELECT 
    opportunity_id,
    COUNT(*) AS duplicate_count
FROM gold.fact_sales
GROUP BY opportunity_id
HAVING COUNT(*) > 1;

