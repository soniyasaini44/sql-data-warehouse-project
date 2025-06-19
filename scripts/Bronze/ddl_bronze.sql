/*
=========================================================================================================
DDL Script: Create bronze tables
Purpose: Loads raw CSV files into bronze schema tables using BULK INSERT. Tracks load time for each table 
and handles errors using TRY...CATCH.
=========================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
   DECLARE @Start_time DATETIME, @End_time DATETIME, @Batch_start_time DATETIME, @Batch_end_time DATETIME;
   BEGIN TRY
        SET @Batch_start_time = GETDATE();
        PRINT '=========================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '=========================';
        
        SET @Start_time = GETDATE();
        PRINT '>> Inserting data into : bronze.accounts';
        BULK INSERT bronze.accounts
        FROM 'C:\Users\chand\Downloads\archive\accounts.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second,@start_time,@End_time) AS NVARCHAR) + ' Seconds';
        PRINT '---------------------------';

        SET @Start_time = GETDATE();
        PRINT '>> Inserting data into : bronze.products';
        BULK INSERT bronze.products
        FROM 'C:\Users\chand\Downloads\archive\products.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second,@start_time,@End_time) AS NVARCHAR) + ' Seconds';
        PRINT '----------------------------';
        
        SET @Start_time = GETDATE();
        PRINT '>> Inserting data into : bronze.sales_pipeline';
        BULK INSERT bronze.sales_pipeline
        FROM 'C:\Users\chand\Downloads\archive\sales_pipeline.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second,@start_time,@End_time) AS NVARCHAR) + ' Seconds';
        PRINT '------------------------';

        SET @Start_time = GETDATE();
        PRINT '>> Inserting data into : bronze.sales_teams';
        BULK INSERT bronze.sales_teams
        FROM 'C:\Users\chand\Downloads\archive\sales_teams.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second,@start_time,@End_time) AS NVARCHAR) + ' Seconds';
        PRINT '------------------------';
    
        SET @Batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading bronze layer is completed';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @Batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        PRINT '=============================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================';
    END CATCH
END;
