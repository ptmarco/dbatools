IF OBJECT_ID('dbo.sp_RebuildHeaps') IS  NULL
    EXEC ('CREATE PROCEDURE dbo.sp_RebuildHeaps AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_RebuildHeaps
/* User Parameters */
	@DatabaseName SYSNAME = NULL,
	@fragmentation_threshold FLOAT = 30,
	@forwarded_record_count_threshold BIGINT = 0,
	@ignore_forwarded_record_count_threshold BIT = 0, -- Rebuild even if there are no fowarded_records
	@max_heap_size_mb INT = 1000,	-- Threshold to HEAP Rebuild, default 1GB
	@logToCommandLog BIT = 1,
	@Rebuild BIT = 0 -- 1 Will REBUILD all HEAPS with fragmentation > @fragmentation_threshold
AS
BEGIN
/*
====================================================================================================================
Author:			Marco Assis
Create date:	xx/2024
Description:	Rebuild Heaps as a stored procedure
Tested On:		SQL Server 2012, 2016, 2019
Notes:	

Parameters:
@DatabaseName								Database to execute
@fragmentation_threshold					Fragmentation Threshold, default = 30
@forwarded_record_count_threshold 			Threshold for forwarded_record_count, default = 0
@ignore_forwarded_record_count_threshold 	To ignore the forwarded_record_count and REBUILD event if its 0

@logToCommandLog 							Log REBUILDs to dba_database.dbo.CommandLog, default = 1
@Rebuild 									1 Will Execute REBUILD, 0 Only shows HEAPS statistics

Examples:
	
	-- Only 1 database
	EXEC dba_database.dbo.sp_RebuildHeaps 
		@DatabaseName = '<database name> | NULL for current database'
		,@fragmentation_threshold = 30
		,@forwarded_record_count_threshold = 0
		,@max_heap_size_mb INT = 1000
		,@logToCommandLog = 1
		,@ignore_forwarded_record_count_threshold = 1
		,@Rebuild = 0';
	
	
	-- Running on ALL databases
	DECLARE @sql NVARCHAR(MAX) = 
	N'USE [?]; IF DB_ID() > 4
	EXEC dba_database.dbo.sp_RebuildHeaps 
		@DatabaseName = [?]
		,@fragmentation_threshold = 30
		,@forwarded_record_count_threshold = 0
		,@max_heap_size_mb INT = 1000
		,@logToCommandLog = 1
		,@ignore_forwarded_record_count_threshold = 1
		,@Rebuild = 0';

	EXEC sp_MSforeachdb @sql
	GO

====================================================================================================================
Change History
Date   			Author       	Description	
xx/12/23		Marco Assis		Initial Build
05/02/24		Marco Assis		Fix missing schema
05/02/24		Marco Assis		Add thrshold table size to prevent rebuilding very large tables
====================================================================================================================
*/IF @DatabaseName IS NULL SELECT @DatabaseName = DB_NAME();
DECLARE @tsql NVARCHAR(MAX) = N'USE ' + QUOTENAME(@DatabaseName) + ';' + CHAR(13) + 
N'SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;

' + 
N'/* User Parameters */
DECLARE @DatabaseName SYSNAME = ''' + @DatabaseName + N''',
		@fragmentation_threshold FLOAT = ' + CAST(@fragmentation_threshold AS NVARCHAR(3)) + N',
		@forwarded_record_count_threshold BIGINT = ' + CAST(@forwarded_record_count_threshold AS NVARCHAR(16)) + N',
		@ignore_forwarded_record_count_threshold BIT = ' + CAST(@ignore_forwarded_record_count_threshold AS NCHAR(1)) + N', 
		@max_heap_size_mb INT = ' + CAST(@max_heap_size_mb AS NCHAR(12)) + N',
		@logToCommandLog BIT = ' + CAST(@logToCommandLog AS NCHAR(1)) + N',
		@Rebuild BIT = ' + CAST(@Rebuild AS CHAR(1)) + N';'

/* Local Variables */
DECLARE @sql NVARCHAR(MAX),
		@SchemaName SYSNAME,
		@ObjectName SYSNAME,
		@StartTime DATETIME2,
		@EndTime DATETIME2;

-- Create Results temp tables
IF OBJECT_ID(N'tempdb.dbo.##heaps_temp','U') IS NOT NULL
 DROP table ##heaps_temp;
IF OBJECT_ID(N'tempdb.dbo.#heaps','U') IS NOT NULL
 DROP table #heaps;

SELECT 
 SCH.name [schema], 
 TBL.name AS [table],
 TBL.object_id [object_id]
INTO ##heaps_temp
FROM sys.tables AS TBL
 INNER JOIN sys.schemas AS SCH 
  ON TBL.schema_id = SCH.schema_id 
INNER JOIN sys.indexes AS IDX 
  ON TBL.object_id = IDX.object_id
  AND IDX.type = 0 
  WHERE 1 = 0;

INSERT INTO ##heaps_temp ([schema],[table],[object_id])
SELECT 
 SCH.name,
 TBL.name,
 TBL.object_id
FROM sys.tables AS TBL
 INNER JOIN sys.schemas AS SCH 
  ON TBL.schema_id = SCH.schema_id
 INNER JOIN sys.indexes AS IDX 
  ON TBL.object_id = IDX.object_id
  AND IDX.type = 0;

-- Get Target HEAPS
SELECT ##heaps_temp.*,
  IPS.forwarded_record_count, 
  IPS.avg_fragmentation_in_percent,
  IPS.page_count,
  IPS.page_count/128.0 [size_mb],
  CASE WHEN 
	avg_fragmentation_in_percent > @fragmentation_threshold THEN 
		CASE WHEN 
		(IPS.page_count/128.0) <= @max_heap_size_mb THEN 
			'ALTER TABLE ' + quotename(##heaps_temp.[schema]) + '.' + quotename(##heaps_temp.[table]) + ' REBUILD;'
		ELSE
			''-- ALTER TABLE ' + quotename(##heaps_temp.[schema]) + '.' + quotename(##heaps_temp.[table]) + ' REBUILD;'
		END
  END [Rebuild]
INTO #heaps
FROM ##heaps_temp
CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(),##heaps_temp.object_id,0,null,NULL) AS IPS
ORDER BY forwarded_record_count DESC;

IF @Rebuild = 1
 BEGIN
  DECLARE [execute] CURSOR FAST_FORWARD FOR
   SELECT [schema],[table],[Rebuild]
   FROM #heaps
   WHERE avg_fragmentation_in_percent > @fragmentation_threshold
    AND ISNULL(forwarded_record_count,@ignore_forwarded_record_count_threshold) > @forwarded_record_count_threshold;
  
  OPEN [execute]
  FETCH NEXT FROM [execute] INTO @SchemaName, @ObjectName,@sql;
  
  WHILE @@FETCH_STATUS = 0
   BEGIN
	IF @logToCommandLog = 1
		BEGIN
			SELECT @StartTime = getdate();
			EXEC(@sql)
			SELECT @EndTime = getdate();
			INSERT INTO dba_database..CommandLog (
					DatabaseName
					,SchemaName
					,ObjectName
					,ObjectType
					,Command
					,CommandType
					,StartTime,
					EndTime,
					ErrorNumber
					)
			VALUES (
					@DatabaseName
					,@SchemaName
					,@ObjectName
					,'H'
					,@sql
					,'REBUILD HEAPS'
					,@StartTime
					,@EndTime
					,0
					)
		END
		ELSE EXEC(@sql);
	FETCH NEXT FROM [execute] INTO @SchemaName, @ObjectName,@sql;
   END
   CLOSE [execute]
   DEALLOCATE [execute]
 END
 ELSE 
	SELECT * FROM #heaps WHERE avg_fragmentation_in_percent > @fragmentation_threshold ORDER BY size_mb DESC;

-- Cleanup
DROP TABLE ##heaps_temp;
DROP TABLE #heaps
;

--PRINT @tsql;
EXEC(@tsql);
END