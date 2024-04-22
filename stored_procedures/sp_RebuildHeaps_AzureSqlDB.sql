IF OBJECT_ID('dbo.sp_RebuildHeaps_AzureSqlDB') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_RebuildHeaps_AzureSqlDB AS RETURN 0;');
GO

ALTER PROCEDURE [dbo].[sp_RebuildHeaps_AzureSqlDB]
/* User Parameters */
	@fragmentation_threshold FLOAT = 30,
	@forwarded_record_count_threshold BIGINT = 0,
	@ignore_forwarded_record_count_threshold BIT = 0, -- Rebuild even if there are no fowarded_records
	@max_heap_size_mb INT = 1000,	-- Threshold to HEAP Rebuild, default 1GB
	@log BIT = 1,
	@Rebuild BIT = 0 -- 1 Will REBUILD all HEAPS with fragmentation > @fragmentation_threshold
AS
BEGIN
/*
====================================================================================================================
Author:			Marco Assis
Create date:	01/2024
Description:	Rebuild HEAPS with high	forwarded records for Azure SQL Database
====================================================================================================================
Change History
Date   			Author       	Description	
27/02/24		Marco Assis		Initial Adaptation from original sp
====================================================================================================================
Example:
	-- Rebuild heaps with default values
	EXEC dbo.sp_RebuildHeaps_AzureSqlDB
		@Rebuild = 1;
*/

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;

/* User Parameters */

/* Local Variables */
DECLARE @SchemaName SYSNAME,
		@ObjectName SYSNAME,
		@StartTime DATETIME2,
		@EndTime DATETIME2,
		@sql NVARCHAR(MAX);

-- Create temp objects
IF OBJECT_ID(N'tempdb.dbo.##heaps_temp','U') IS NOT NULL
 DROP table ##heaps_temp;
IF OBJECT_ID(N'tempdb.dbo.#heaps','U') IS NOT NULL
 DROP table #heaps;
IF OBJECT_ID(N'tempdb.dbo.#log','U') IS NOT NULL
 DROP table #log;

IF @log = 1
	CREATE TABLE #log (
		DatabaseName SYSNAME NOT NULL
		,SchemaName SYSNAME NOT NULL
		,ObjectName SYSNAME NOT NULL
		,ObjectType NCHAR(1)
		,CommandType NVARCHAR(32) NOT NULL
		,Command NVARCHAR(MAX)
		,StartTime DATETIME2 NOT NULL
		,EndTime DATETIME2 NOT NULL
		,ErrorCode INT
		,StartDate DATETIME2
		,EndDate DATETIME2
);

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
			'-- ALTER TABLE ' + quotename(##heaps_temp.[schema]) + '.' + quotename(##heaps_temp.[table]) + ' REBUILD;' 
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
	IF @log = 1
		BEGIN
			SELECT @StartTime = getdate();
			PRINT @sql
			--EXEC(@sql)
			SELECT @EndTime = getdate();
			INSERT INTO #log (
					DatabaseName
					,SchemaName
					,ObjectName
					,ObjectType
					,CommandType
					,Command
					,StartTime,
					EndTime,
					ErrorCode
					)
			VALUES (
					DB_NAME()
					,@SchemaName
					,@ObjectName
					,N'N'
					,N'REBUILD HEAPS'
					,@sql
					,@StartTime
					,@EndTime
					,0
					)
		END
	--EXEC(@sql);
	PRINT @sql;
	FETCH NEXT FROM [execute] INTO @SchemaName, @ObjectName,@sql;
   END
   CLOSE [execute]
   DEALLOCATE [execute]
 END
 ELSE -- Only show current Heap State
	BEGIN
		SELECT 'Heaps > threshold'  [Description]
		SELECT * FROM #heaps WHERE avg_fragmentation_in_percent > @fragmentation_threshold ORDER BY size_mb DESC;
	END

-- Output Log
IF @log = 1 SELECT * FROM #log ORDER BY EndTime DESC;

-- Cleanup
DROP TABLE ##heaps_temp;
DROP TABLE #heaps
DROP TABLE #log

END