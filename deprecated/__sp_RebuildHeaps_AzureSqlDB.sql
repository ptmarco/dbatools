SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_RebuildHeaps]') AND type in (N'P', N'PC'))
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[sp_RebuildHeaps] AS' 
GO

ALTER PROCEDURE [dbo].[sp_RebuildHeaps]
/* User Parameters */
    @DatabaseName                               SYSNAME = NULL,
    @fragmentation_threshold                    FLOAT   = 30,
    @forwarded_record_count_threshold           BIGINT  = 0,
    @ignore_forwarded_record_count_threshold    BIT     = 0, -- Rebuild even if there are no fowarded_records
    @max_heap_size_mb                           INT     = 1000,    -- Threshold to HEAP Rebuild, default 1GB
    @logToCommandLog                            BIT     = 1,
    @Rebuild                                    BIT     = 0 -- 1 Will REBUILD all HEAPS with fragmentation > @fragmentation_threshold
--WITH ENCRYPTION
AS
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
20/11/24        Marco Assis         Add Azure SQL Database Compatibility
                                    Temporarely Remove Logging capabilities
====================================================================================================================
*/
DECLARE @tsql NVARCHAR(MAX)
IF @DatabaseName IS NULL SELECT @DatabaseName = DB_NAME();

IF SERVERPROPERTY('EngineEdition') IN (2,3,4,8) -- Exclude Azure SQL Database, Synapse and Edge
    SELECT @tsql = N'USE ' + QUOTENAME(@DatabaseName) + ';' + CHAR(13)

SELECT @tsql = N'SET ANSI_NULLS ON;
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
        @Rebuild BIT = ' + CAST(@Rebuild AS CHAR(1)) + N'; 

/* Local Variables */
DECLARE @sql NVARCHAR(MAX),
        @SchemaName SYSNAME,
        @ObjectName SYSNAME,
        @StartTime DATETIME2,
        @EndTime DATETIME2,
		@ProcessStart DATETIME = GETDATE();

-- Create Results temp tables
IF OBJECT_ID(N''tempdb.dbo.##heaps_temp'',''U'') IS NOT NULL
 DROP table ##heaps_temp;
IF OBJECT_ID(N''tempdb.dbo.#heaps'',''U'') IS NOT NULL
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
            ''ALTER TABLE '' + quotename(##heaps_temp.[schema]) + ''.'' + quotename(##heaps_temp.[table]) + '' REBUILD;'' 
        ELSE
            ''-- ALTER TABLE '' + quotename(##heaps_temp.[schema]) + ''.'' + quotename(##heaps_temp.[table]) + '' REBUILD;'' 
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
    EXEC(@sql);
    FETCH NEXT FROM [execute] INTO @SchemaName, @ObjectName,@sql;
   END
   CLOSE [execute]
   DEALLOCATE [execute]
 END
 ELSE 
    SELECT @@SERVERNAME [Server], DB_NAME() [DatabaseName], * FROM #heaps WHERE avg_fragmentation_in_percent > @fragmentation_threshold ORDER BY size_mb DESC;

-- Cleanup
DROP TABLE ##heaps_temp;
DROP TABLE #heaps
';

--PRINT @tsql;
EXEC(@tsql);
END
GO

/*
IF @logToCommandLog = 1
        BEGIN
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N''#CommandLog'') AND type in (N''U''))
			BEGIN
				CREATE TABLE #CommandLog](
				  [ID] [int] IDENTITY(1,1) NOT NULL,
				  [DatabaseName] [sysname] NULL,
				  [SchemaName] [sysname] NULL,
				  [ObjectName] [sysname] NULL,
				  [ObjectType] [char](2) NULL,
				  [IndexName] [sysname] NULL,
				  [IndexType] [tinyint] NULL,
				  [StatisticsName] [sysname] NULL,
				  [PartitionNumber] [int] NULL,
				  [ExtendedInfo] [xml] NULL,
				  [Command] [nvarchar](max) NOT NULL,
				  [CommandType] [nvarchar](60) NOT NULL,
				  [StartTime] [datetime2](7) NOT NULL,
				  [EndTime] [datetime2](7) NULL,
				  [ErrorNumber] [int] NULL,
				  [ErrorMessage] [nvarchar](max) NULL,
				 CONSTRAINT [PK_CommandLog] PRIMARY KEY CLUSTERED ([ID] ASC)
				 WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
				)
			END
			SELECT @StartTime = getdate();
            EXEC(@sql)
            SELECT @EndTime = getdate();
            INSERT INTO #CommandLog (
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
                    ,''H''
                    ,@sql
                    ,''REBUILD_HEAPS''
                    ,@StartTime
                    ,@EndTime
                    ,0
                    )
        END
        ELSE 

-- Output Log
SELECT	*
FROM	#CommandLog
WHERE	CommandType = N''CommandType''
	AND	StartTime >= @ProcessStart
*/