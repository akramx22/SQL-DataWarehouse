/*
This script creates a procedure that load full data from all csv files (CRM and ERP) into tables created in the ddl script.
First tables will be truncated to become fully empty, then inserting data with the BULK INSERT for a quick full load.

Example of execution: 
EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY
		SET @batch_start = GETDATE();
		PRINT '==========================================';
		PRINT '>> LOADING BRONZE LAYER';
		PRINT '==========================================';

		PRINT '==========================================';
		PRINT '>> LOADING CRM TABLES';
		PRINT '==========================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting data into table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\akram\DATA ENGINEERING\First Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.crm_cust_info loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting data into table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\akram\DATA ENGINEERING\First Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.crm_prd_info loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting data into table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\akram\DATA ENGINEERING\First Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.crm_sales_details loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		

		PRINT '==========================================';
		PRINT '>> LOADING ERP TABLES';
		PRINT '==========================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting data into table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\akram\DATA ENGINEERING\First Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.erp_cust_az12 loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting data into table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\akram\DATA ENGINEERING\First Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.erp_loc_a101 loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting data into table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\akram\DATA ENGINEERING\First Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.erp_px_cat_g1v2 loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end = GETDATE();
		PRINT '---------------------------------------------------------------------------';
		PRINT 'Bronze Layer Loaded Successfully!';
		PRINT 'Bronze Layer Loaded in: ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '---------------------------------';
		PRINT 'ERROR IN LOADING FILES';
		PRINT 'Error : ' + ERROR_MESSAGE();
		PRINT 'Error : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error : ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '---------------------------------';
	END CATCH
END
