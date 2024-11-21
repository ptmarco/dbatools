SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_RebuildHeaps]') AND type in (N'P', N'PC'))
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[sp_RebuildHeaps] AS RETURN 1;' 
GO

ALTER PROCEDURE [dbo].[sp_RebuildHeaps]
/* User Parameters */
    @DatabaseName                               SYSNAME			= NULL,
    @fragmentation_threshold                    FLOAT			= 30,
    @forwarded_record_count_threshold           BIGINT			= 0,
    @ignore_forwarded_record_count_threshold    BIT				= 0,	-- Rebuild even if there are no fowarded_records
    @max_heap_size_mb                           INT				= 1000, -- Threshold to HEAP Rebuild, default 1GB
    @CommandLogTable                            NVARCHAR(64)    = '',	-- 3 part name. Object MUST already exist!
    @Rebuild                                    BIT				= 0		-- 1 Will REBUILD all HEAPS with fragmentation > @fragmentation_threshold
AS
--WITH ENCRYPTION
BEGIN
/*
====================================================================================================================
Author:         Marco Assis
Create date:    01/2024
Description:    Rebuild HEAPS with high    forwarded records
====================================================================================================================
Change History
Date            Author              Description    
xx/12/23        Marco Assis         Initial Build
05/02/24        Marco Assis         Fix missing schema
05/02/24        Marco Assis         Add thrshold table size to prevent rebuilding very large tables
21/11/24        Marco Assis         Add Azure SQL Database Compatibility
====================================================================================================================
*/
DECLARE @tsql NVARCHAR(MAX)
        ,@use NVARCHAR(128) = N'';

IF @DatabaseName IS NULL 
    SELECT @DatabaseName = DB_NAME();

IF SERVERPROPERTY('EngineEdition') IN (1,2,3,4,8) -- Exclude Azure SQL Database, Synapse and Edge
    SELECT @use = N'USE ' + QUOTENAME(@DatabaseName) + N'; ' --+ CHAR(13)

SELECT @tsql = @use + 
N'SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;

/* User Parameters */
DECLARE @DatabaseName SYSNAME = ''' + @DatabaseName + N''',
        @fragmentation_threshold FLOAT = ' + CAST(@fragmentation_threshold AS NVARCHAR(3)) + N',
        @forwarded_record_count_threshold BIGINT = ' + CAST(@forwarded_record_count_threshold AS NVARCHAR(16)) + N',
        @ignore_forwarded_record_count_threshold BIT = ' + CAST(@ignore_forwarded_record_count_threshold AS NCHAR(1)) + N', 
        @max_heap_size_mb INT = ' + CAST(@max_heap_size_mb AS NCHAR(12)) + N',
        @CommandLogTable NVARCHAR(128) = ''' --+ @CommandLogTable 
		+ N''',
        @Rebuild BIT = ' + CAST(@Rebuild AS CHAR(1)) + N';

/* Local Variables */
DECLARE @sql NVARCHAR(MAX),
        @SchemaName SYSNAME,
        @ObjectName SYSNAME,
        @StartTime DATETIME2,
        @EndTime DATETIME2

-- Create Results temp tables
IF OBJECT_ID(N''tempdb.dbo.#heaps_in_scope'',''U'') IS NOT NULL
 DROP table #heaps_in_scope;
IF OBJECT_ID(N''tempdb.dbo.#heaps'',''U'') IS NOT NULL
 DROP table #heaps;

SELECT 
 SCH.name [schema], 
 TBL.name AS [table],
 TBL.object_id [object_id]
INTO #heaps_in_scope
FROM sys.tables AS TBL
 INNER JOIN sys.schemas AS SCH 
  ON TBL.schema_id = SCH.schema_id 
INNER JOIN sys.indexes AS IDX 
  ON TBL.object_id = IDX.object_id
  AND IDX.type = 0 
  WHERE 1 = 0;

INSERT INTO #heaps_in_scope ([schema],[table],[object_id])
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
SELECT #heaps_in_scope.*,
  IPS.forwarded_record_count, 
  IPS.avg_fragmentation_in_percent,
  IPS.page_count,
  IPS.page_count/128.0 [size_mb],
  CASE WHEN 
    avg_fragmentation_in_percent > @fragmentation_threshold THEN 
        CASE WHEN 
        (IPS.page_count/128.0) <= @max_heap_size_mb THEN
            ''ALTER TABLE '' + quotename(#heaps_in_scope.[schema]) + ''.'' + quotename(#heaps_in_scope.[table]) + '' REBUILD;'' 
        ELSE
            ''-- ALTER TABLE '' + quotename(#heaps_in_scope.[schema]) + ''.'' + quotename(#heaps_in_scope.[table]) + '' REBUILD;'' 
        END
  END [Rebuild]
INTO #heaps
FROM #heaps_in_scope
CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(),#heaps_in_scope.object_id,0,null,NULL) AS IPS
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
    SELECT @StartTime = getdate();
    EXEC(@sql);
    SELECT @EndTime = getdate();
    PRINT quotename(DB_NAME()) + N'' '' + @sql + CASE LEFT(@sql,1) WHEN ''-'' THEN N'' Skipped due to thresholds'' ELSE '' Executed'' END;
        IF OBJECT_ID(''' + @CommandLogTable + N''') IS NOT NULL
			INSERT INTO ' + @CommandLogTable + N' (DatabaseName, SchemaName, ObjectName ,ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber)
			VALUES (DB_NAME(), @SchemaName, @ObjectName, ''H'', @sql, ''REBUILD_HEAPS'', @StartTime, @EndTime, 0)   
	FETCH NEXT FROM [execute] INTO @SchemaName, @ObjectName,@sql;
   END
   CLOSE [execute]
   DEALLOCATE [execute]
 END
 ELSE 
    SELECT @@SERVERNAME [Server], DB_NAME() [DatabaseName], * FROM #heaps WHERE avg_fragmentation_in_percent > @fragmentation_threshold ORDER BY size_mb DESC;

-- Cleanup
DROP TABLE #heaps_in_scope;
DROP TABLE #heaps
';

--PRINT @tsql;
EXEC(@tsql);
END
GO