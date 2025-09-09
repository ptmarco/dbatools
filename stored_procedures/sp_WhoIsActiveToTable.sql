USE dba_database
GO

SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF OBJECT_ID('sp_WhoiIsActiveToTable','P') IS NULL
	EXEC(N'CREATE PROCEDURE dbo.sp_WhoiIsActiveToTable AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_WhoiIsActiveToTable
    @retention_days 		INT 			= 30					,
    @destination_database 	SYSNAME 		= 'dba_database'		,
	@destination_schema 	VARCHAR(500)	= 'dbo'					,
	@destination_table 		VARCHAR(500)	= 'WhoIsActive'			,
	@stop_datetime 			DATETIME		= '2025-08-31 06:00:00'	,		
    @delay_time             NCHAR(8)        = '00:01:00'			
--WITH ENCRIPTION
AS
/*
====================================================================================================================
Description:    sp_WhoIsActive_to_Table
                Logs sp_whoisactive activity to a table
Author:         Marco Assis
Tested On:      SQL Server 2012+
Notes:
    
Parameters / Default Value:
    @retention_days			30 (days)				-- Purge records older than @retention_days
    @destination_database	'dba_database'			-- database name (must exist)
	@destination_schema 	'dbo'					-- Schema name (must exist)
	@destination_table 		'WhoIsActive'			-- Table name
    @stop_datetime			'2025-08-31 06:00:00' 	-- When to stop
	@delay_time				'00:00:30' 				-- (pause time between runs)

Returns:
    table

Examples:
	-- Simple example
	EXEC dba_database..sp_WhoiIsActiveToTable
			@destination_database   = 'dba_database',
			@destination_schema     = 'dbo',
			@destination_table      = 'WhoIsActive',
			@stop_datetime          = '2025-08-22 16:20:00:00'	,		
			@delay_time             = '00:00:20',
			@retention_days         = 15;
	
	-- Dinamic stop_time for "today" at 01:30
	DECLARE @stop_datetime DATETIME;
	SET @stop_datetime = DATEADD(MINUTE, 90, GETDATE());  -- Adds 90 minutes to current tim

	-- Optional: Print to verify stop_time
	PRINT 'Calculated stop_time: ' + CONVERT(VARCHAR, @stop_datetime, 120);

	EXEC dbo.sp_WhoIsActive_to_Table
		@stop_datetime = @stop_datetime,
		@delay = '00:01:00';

====================================================================================================================
Change History
v1
18.08.25    Marco Assis     Initial Build
22/08/25	Marco Assis		Several minor improvments
====================================================================================================================
License:
    GNU General Public License v3.0
    https://github.com/ptmarco/dbatools/blob/master/LICENSE

Github:
    https://github.com/ptmarco/dbatools/

You can contact me by e-mail at marcoassis@gmail.com
====================================================================================================================
*/

/**** Environment ****/
SET NOCOUNT ON;

/**** Local Parameters ****/
DECLARE 
        @schema VARCHAR(MAX),
        @SQL NVARCHAR(4000),
        @parameters NVARCHAR(500),
        @exists BIT;

SET @destination_table = @destination_database + N'.' + @destination_schema + N'.' + @destination_table;

--create the logging table 
IF OBJECT_ID(@destination_table,'U') IS NULL
    BEGIN
		PRINT N'Table ' + @destination_table + N' does not exist. Creating table';
        EXEC dbo.sp_WhoIsActive @get_transaction_info = 1,
                                @get_outer_command = 1,
                                @get_plans = 1,
                                @return_schema = 1,
                                @get_locks = 1,
                                @find_block_leaders = 1,
                                @schema = @schema OUTPUT;
        --create table (with lock db and schema exists)
		BEGIN TRY
			SET @SQL = REPLACE(@schema, '<table_name>', @destination_table);
			EXEC ( @SQL );
			PRINT N'Table created'
		END TRY
		BEGIN CATCH
			PRINT 'Failed to create table ' + @destination_table;
			RETURN
		END CATCH
		--create index on collection_time
		SET @SQL = 'CREATE CLUSTERED INDEX cx_collection_time ON ' + @destination_table + '(collection_time ASC)';
        EXEC ( @SQL );
		PRINT N'Index created';
    END;

WHILE (getdate() <= @stop_datetime)
BEGIN
    --collect activity into logging table
    PRINT CAST(getdate() AS NVARCHAR(32)) + N' - Executing sp_WhoiIsActiveToTable'
	EXEC dbo.sp_WhoIsActive @get_transaction_info = 1,
                            @get_outer_command = 1,
                            @get_plans = 1,
                            @get_locks = 1,
                            @find_block_leaders = 1,
                            @destination_table = @destination_table;
    WAITFOR DELAY @delay_time;
END

--purge older data
SET @SQL
    = 'DELETE FROM ' + @destination_table + ' WHERE collection_time < DATEADD(day, -' + CAST(@retention_days AS VARCHAR(10))
        + ', GETDATE());';
EXEC ( @SQL );
PRINT N'Old data deleted'
GO

/* Test the stored procedure *****************************

USE master;
GO

DECLARE @stop_datetime DATETIME;
SET @stop_datetime = DATEADD(MINUTE, 2, GETDATE());

EXEC dba_database..sp_WhoiIsActiveToTable
        @destination_database   = 'dba_database',
        @destination_schema     = 'dbo',
        @destination_table      = 'WhoIsActive',
        @stop_datetime          = @stop_datetime,		
        @delay_time             = '00:00:20',
        @retention_days         = 15;
GO

-- Check results
SELECT * FROM dba_database..WhoIsActive;
GO

***************************************************/
