CREATE OR ALTER PROCEDURE dbo.CaptureTheMetrics (
    @DatabaseName sysname,
    @TargetTable nvarchar(300) = NULL  -- three-part name database.schema.table; default dba_database.perf.Metrics
)
AS
BEGIN
  /*
    Gather performance metrics from various DMVs and insert them into the target table.
    Credits to Aaron Bertrand https://www.sqlshack.com/monitoring-sql-server-performance-metrics-using-dmvs/

    Marco Assis @ Kyndrl:

    2025-10-16: Refactored: removed hardcoded 'dba_database'. The target database is now passed
                via @DatabaseName. Cross-database DMV references are handled with dynamic SQL
                and QUOTENAME to avoid injection.

    2025-10-16: Added @TargetTable (three-part name). If NULL defaults to dba_database.perf.Metrics.
                The procedure safely quotes each part (database, schema, table) using PARSENAME + QUOTENAME
                and builds the INSERT target dynamically. Example override:
                  EXEC dbo.CaptureTheMetrics @DatabaseName = N'dba_database', @TargetTable = N'Control.dbo.Metrics';
  */
  DECLARE @db sysname = @DatabaseName;
  DECLARE @dbThreePartPrefix nvarchar(300) = QUOTENAME(@db) + N'.sys.';

  /* Resolve target table */
  IF @TargetTable IS NULL OR LTRIM(RTRIM(@TargetTable)) = N''
      SET @TargetTable = N'dba_database.perf.Metrics';

  DECLARE @TargetDB sysname = PARSENAME(@TargetTable,3);
  DECLARE @TargetSchema sysname = PARSENAME(@TargetTable,2);
  DECLARE @TargetObject sysname = PARSENAME(@TargetTable,1);

  IF @TargetDB IS NULL OR @TargetSchema IS NULL OR @TargetObject IS NULL
  BEGIN
      RAISERROR('Target table must be a three-part name database.schema.table',16,1);
      RETURN;
  END

  DECLARE @TargetTableQuoted nvarchar(400) = QUOTENAME(@TargetDB) + N'.' + QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@TargetObject);

  WHILE 1 = 1
  BEGIN
    IF EXISTS(SELECT 1 FROM sys.databases WHERE name = @db AND state = 0)
    BEGIN
      DECLARE @sql nvarchar(max) = N'
;WITH perf_src AS
(
    SELECT instance_name, counter_name, cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name LIKE N''%total server memory%''
       OR ( [object_name] LIKE N''%:Resource Pool Stats%''
            AND counter_name IN (N''CPU usage %'', N''CPU usage % base'')
            AND instance_name = N''default'')
       OR ( counter_name IN (N''Log File(s) Size (KB)'', N''Log File(s) Used Size (KB)'')
            AND instance_name = @db)
),
cpu AS
(
    SELECT cpu = COALESCE(100*(CONVERT(float,val.cntr_value) / NULLIF(base.cntr_value,0)),0)
    FROM       perf_src AS val
    INNER JOIN perf_src AS base
       ON val.counter_name  = N''CPU usage %''
      AND base.counter_name = N''CPU usage % base''
),
mem AS
(
    SELECT mem_usage = cntr_value/1024.0
    FROM perf_src
    WHERE counter_name LIKE N''%total server memory%''
),
dbuse AS
(
    SELECT db_size = SUM(base.size/128.0),
           used_size = SUM(base.size/128.0) - SUM(val.unallocated_extent_page_count/128.0)
    FROM ' + QUOTENAME(@db) + N'.sys.dm_db_file_space_usage AS val
    INNER JOIN ' + QUOTENAME(@db) + N'.sys.database_files AS base
       ON val.[file_id] = base.[file_id]
),
vstore AS
(
    SELECT size = CONVERT(bigint,persistent_version_store_size_kb)/1024.0
    FROM sys.dm_tran_persistent_version_store_stats
    WHERE database_id = DB_ID(@db)
),
rowgroup AS
(
    SELECT size = SUM(size_in_bytes)/1024.0/1024.0
    FROM ' + QUOTENAME(@db) + N'.sys.dm_db_column_store_row_group_physical_stats
),
loguse AS
(
    SELECT log_size = base.cntr_value/1024.0,
           used_size = val.cntr_value/1024.0
    FROM       perf_src AS val
    INNER JOIN perf_src AS base
       ON val.counter_name  = N''Log File(s) Used Size (KB)''
      AND base.counter_name = N''Log File(s) Size (KB)''
)
INSERT ' + @TargetTableQuoted + N'
(
  cpu,mem,db_size,db_used,db_perc,log_size,log_used,log_perc,vstore,rowgroup
)
SELECT cpu      = CONVERT(decimal(6,3),cpu.cpu),
       mem      = CONVERT(decimal(18,3),mem.mem_usage),
       db_size  = CONVERT(decimal(18,3),dbuse.db_size),
       db_used  = CONVERT(decimal(18,3),dbuse.used_size),
       db_perc  = CONVERT(decimal(5,2),COALESCE(100*(CONVERT(float,dbuse.used_size) / NULLIF(dbuse.db_size,0)),0)),
       log_size = CONVERT(decimal(18,3),loguse.log_size),
       log_used = CONVERT(decimal(18,3),loguse.used_size),
       log_perc = CONVERT(decimal(5,2),COALESCE(100*(CONVERT(float,loguse.used_size) / NULLIF(loguse.log_size,0)),0)),
       vstore   = CONVERT(decimal(18,3),vstore.size),
       rowgroup = CONVERT(decimal(18,3),COALESCE(rowgroup.size,0))
FROM cpu
INNER JOIN      mem      ON 1=1
INNER JOIN      dbuse    ON 1=1
INNER JOIN      loguse   ON 1=1
LEFT  JOIN      vstore   ON 1=1
LEFT  JOIN      rowgroup ON 1=1;
';
      EXEC sys.sp_executesql @sql, N'@db sysname', @db=@db;
    END
    -- wait three seconds, then try again
    WAITFOR DELAY '00:00:03';
  END
END
GO

-- Exemplo de execução:
-- EXEC dbo.CaptureTheMetrics @DatabaseName = N'dba_database', @TargetTable = N'dba_database.perf.Metrics';
