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

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		-- Loading silver.accounts
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.accounts';
		TRUNCATE TABLE silver.accounts;
		PRINT '>> Inserting Data Into: silver.accounts';
	    WITH CTE AS (
          SELECT *,
           ROW_NUMBER() OVER (PARTITION BY account ORDER BY (SELECT NULL)) AS rn
           FROM bronze.accounts
        )
			INSERT INTO silver.accounts (
				account,
				sector,
				year_established,
				revenue,
				employees,
				office_location,
				subsidiary_of
			)
			SELECT
				account,
				sector,
				year_established,
				revenue,
				employees,
				office_location,
				subsidiary_of
			
			FROM CTE 
			WHERE rn = 1;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.products
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.products';
		TRUNCATE TABLE silver.products;
		PRINT '>> Inserting Data Into: silver.products';
		WITH CTE AS (
          SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product ORDER BY (SELECT NULL)) AS rn
           FROM bronze.products
        )
			INSERT INTO silver.products (
				product,
				series,
				sales_price
			)
			SELECT
				product,
				series,
				sales_price
			
			FROM CTE 
			WHERE rn = 1;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.sales_pipeline
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.sales_pipeline';
		TRUNCATE TABLE silver.sales_pipeline;
		PRINT '>> Inserting Data Into: silver.sales_pipeline';
	    WITH CTE AS (
          SELECT *,
           ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY (SELECT NULL)) AS rn
           FROM bronze.sales_pipeline
        )
			INSERT INTO silver.sales_pipeline (
				opportunity_id,
				sales_agent,
				product,
				account,
				deal_stage,
				engage_date,
				close_date,
				close_value
			)
			SELECT
				opportunity_id,
				sales_agent,
				product,
				account,
				deal_stage,
				engage_date,
				close_date,
				close_value
				
			FROM CTE 
			WHERE rn = 1;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.sales_teams
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.sales_teams';
		TRUNCATE TABLE silver.sales_teams;
		PRINT '>> Inserting Data Into: silver.sales_teams';
	    WITH CTE AS (
          SELECT *,
           ROW_NUMBER() OVER (PARTITION BY sales_agent ORDER BY (SELECT NULL)) AS rn
           FROM bronze.sales_teams
        )
			INSERT INTO silver.sales_teams (
				sales_agent,
				manager,
				regional_office
			)
			SELECT
				sales_agent,
				manager,
				regional_office
				
			FROM CTE 
			WHERE rn = 1;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END


