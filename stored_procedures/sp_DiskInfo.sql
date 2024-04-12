SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

IF OBJECT_ID('dbo.sp_DiskInfo') IS  NULL
    EXEC ('CREATE PROCEDURE dbo.sp_DiskInfo AS RETURN 138;');
GO

ALTER PROCEDURE dbo.sp_DiskInfo
	/* Parameters */
   @type			TINYINT		= 99,						-- 1 Database Autogrowth Report
															-- 2 Database Data/Log file Size
   @databaseName	SYSNAME		= NULL,
   @daysback		TINYINT		= 7,
   @help			TINYINT		= 0
--WITH ENCRYPTION
AS
BEGIN
	
	IF @type NOT IN (1,2)
		SET @help = 1
	
	IF (
		SELECT convert(INT, value_in_use)
		FROM sys.configurations
		WHERE name = 'default trace enabled'
		) = 0
		BEGIN
			SELECT '!!! Default Trace NOT ENABLED !!!' as [Error];
			SET @help = 1;
		END

	IF @help = 1
	BEGIN
		PRINT '
=====================================================================================================
	Author:        	Marco Assis
	Create date:   	30/03/22
	Description:   	Disk Useful Information
	Tested on:		SQL 2012, 2016, 2019
---------------------------------------------------------------------------------------------------
	Parameters:
	@type			= 1 Database Autogrowth Report, 
					= 2 Database Data/Log file Size, 
	@databaseName	= NULL (default) for ALL Databases
	@DaysBack		= # of days to look back (default 7 days)
---------------------------------------------------------------------------------------------------
	Change History
	Date   		Version		Author       	Description
	30/03/22	v1.0 		Marco Assis		Initial Build
---------------------------------------------------------------------------------------------------
	You may alter this code for your own *non-commercial* purposes. You may
	republish altered code as long as you include this copyright and give due
	credit, but you must obtain prior permission before blogging this code.
		   
	THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
	ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
	TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
	PARTICULAR PURPOSE.
=====================================================================================================
';
	RETURN;
	END

	DECLARE @sql NVARCHAR(2000) = N'';

	IF @type = 1  /* Option 1 - Database Autogrowth Report */
		BEGIN 
			SELECT 'Type 1: Database Autogrowth Report' as [Type], @daysback as [Last_X_Days];
			DECLARE @curr_tracefilename VARCHAR(500);
			DECLARE @base_tracefilename VARCHAR(500);
			DECLARE @indx INT;

			SELECT @curr_tracefilename = path
			FROM sys.traces
			WHERE is_default = 1;

			SET @curr_tracefilename = reverse(@curr_tracefilename);

			SELECT @indx = patindex('%\%', @curr_tracefilename);

			SET @curr_tracefilename = reverse(@curr_tracefilename);
			SET @base_tracefilename = left(@curr_tracefilename, len(@curr_tracefilename) - @indx) + '\log.trc';
			SELECT DatabaseName
				,Filename
				,convert(INT, EventClass) AS EventClass
				,(Duration / 1000) AS Duration
				,StartTime
				,EndTime
				,cast((IntegerData * 8.0 / 1024) AS INT) AS ChangeInSize
			FROM::fn_trace_gettable(@base_tracefilename, DEFAULT)
			LEFT OUTER JOIN sys.databases AS d ON (d.name = DB_NAME())
			WHERE EventClass >= 92
				AND EventClass <= 95
				AND ServerName = @@servername
				AND StartTime >= DATEADD(day, @daysback * -1, getdate())
				AND DatabaseName LIKE ISNULL(@DatabaseName,N'%')
			ORDER BY DatabaseName, StartTime DESC
		END
		
	IF @type = 2
		SELECT 'Type 2: ** Work in Progres **' as [Type];

END
GO