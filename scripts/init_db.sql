/*
================================================================
DATABASE INITIALIZATION
================================================================

PURPOSE: This script create a new database called DataWarehouse.
If the database already exist it'll be dropped and create a new one instead, with
creating three schemas, bronze, silver and gold within the database.

WARNING: Running this script while the database is already existing will delete all
the data permanently, ensure that you have a propre backup before running this script
or the database is empty.
*/

USE master;
GO

IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
