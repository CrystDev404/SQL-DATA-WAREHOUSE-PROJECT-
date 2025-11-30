/*
=================================================================
Create Database and schemas
=================================================================
Script ppurpose:

This script creates a new database name 'DataWareHouse' The scripts set up three schemas with the database:'bronse', 'silver' and 'gold'
Note that it is good practice to always check the existanece of a database before creating one. The code below did not show a check.

Other users are advie to peroform a check to ptreven database drop and lost data by runnning these script

WARNING
Running this script will drop the entire 'DataWareHouse' database if it exists. All data in the database will be permanently deleted. 
Proceed with caustion and ensure you have a proper backups before running this script 
*/

CREATE DATABASE DataWareHouse;

USE DataWareHouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO 
