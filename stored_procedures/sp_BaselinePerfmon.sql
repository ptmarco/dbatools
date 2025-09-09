/*** Userfull Querys ***********************************************************************************

-- CPU Usage (point-in-time)
;WITH base AS 
(
	SELECT	sysdate, object_name, counter_name, cntr_value
	FROM	Baseline.perfmon
	WHERE	1=1
		AND counter_name	like N'CPU usage \% base' ESCAPE '\'
		--AND counter_name	like N'%base'
		AND object_name		like N'%Resource Pool Stats'
		AND instance_name	like N'default'
)
SELECT	usage.sysdate
		,usage.object_name
		,usage.counter_name
		,usage.instance_name
		--sage.cntr_type
		,usage.cntr_value
		,base.cntr_value "base"
		,CAST( ( ( (usage.cntr_value*1.0) / (base.cntr_value*1.0) ) * 100.0) AS NUMERIC(36,2)) [cpu_%]
FROM	Baseline.perfmon AS usage
JOIN	base
			ON base.sysdate = usage.sysdate
			AND base.object_name = usage.object_name
WHERE	1=1
		AND usage.counter_name	   like		N'CPU usage %' ESCAPE '\'
		AND usage.counter_name	   not like N'% base' 
		AND usage.counter_name	   not like N'CPU usage target %%' ESCAPE '\'
		AND usage.object_name	   like		N'%Resource Pool Stats'
		AND usage.instance_name	   =		N'default'
		AND usage.sysdate > dateadd(hour,-24*7,getdate())
ORDER BY
		usage.sysdate desc, cntr_type
		--cntr_value desc


-- Last 24h per Database (point-in-time)
SELECT	perf.instance_name
		, perf.object_name
		, perf.counter_name
		, perf.sysdate
		, perf.cntr_type
		, perf.cntr_value
		, perf.previous
		, perf.delta
		, perf.elapsed
		, t.meaning
FROM	Baseline.perfmon perf
		INNER JOIN Baseline.cntr_type t
			ON t.cntr_type = perf.cntr_type
WHERE	1=1
	AND sysdate > dateadd(minute,24*-60,getdate())
	AND object_name		LIKE        N'%Database%'
	AND object_name		NOT LIKE    N'% Replica'
	--AND instance_name	LIKE N'IAF'
	AND instance_name	LIKE	    N'_Total'
    AND counter_name	IN          (N'Active Transactions','Percent Log Used','Transactions/sec','')
	AND perf.cntr_value !=          0
ORDER BY
		instance_name, counter_name, sysdate desc


-- Average/Hour (excludes cumulative perfmons)
SELECT	perf.instance_name
		, perf.object_name
		, perf.counter_name
		, perf.cntr_type
		, t.meaning [type]
		, FORMAT(datepart(month,perf.sysdate), '00') [Month]
		, FORMAT(datepart(day,perf.sysdate), '00')	 [Day]
		, FORMAT(datepart(hour,perf.sysdate), '00')  [Hour]
		, avg(perf.cntr_value)*1.0					 [cntr_value_avg_hour]
		, avg(perf.delta)*1.0						 [delta_avg_hour]
FROM	Baseline.perfmon perf
		INNER JOIN Baseline.cntr_type t
			ON t.cntr_type = perf.cntr_type
WHERE	1=1
	AND sysdate > dateadd(day,-7,getdate())
	AND perf.cntr_type  IN			(65792,537003264,1073874176,1073939712) -- not cumulative
	-- optional filters
	--AND object_name		LIKE		N'%Database%'
	--AND object_name		NOT LIKE	N'% Replica'
	--AND instance_name	LIKE		N'IAF'
	AND instance_name	LIKE		N'_Total' --or instance_name = N'')
	--AND counter_name	IN			(N'Active Transactions','Percent Log Used','Transactions/sec','')
GROUP BY
	perf.instance_name
	, perf.object_name
	, perf.counter_name
	, perf.cntr_type
	, t.meaning
	, datepart(month,perf.sysdate)
	, datepart(day,perf.sysdate)
	, datepart(hour,perf.sysdate)
ORDER BY
		instance_name, Month, Day, Hour, object_name, counter_name

*/

IF OBJECT_ID('sp_BaselinePerfmon','P') IS NULL
	EXEC(N'CREATE PROCEDURE dbo.sp_BaselinePerfmon AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_BaselinePerfmon
    @duration   	INT         -- Execution Duration in minutes
    ,@delay     	INT = 120   -- Delay in seconds between perfmon recordings
    ,@retention 	INT = 30    -- Days of history to keep
	,@RowsPerBatch 	INT	= 10000
--WITH ENCRYPTION
AS

/*
====================================================================================================================
Author:         Marco Assis
Description:    Records perfmon counters into Baseline.Perfmon
Tested On:      SQL Server 2012+
Notes:          

Parameters:
    @duration   INT    Execution Duration in minutes
    @delay      INT    Delay in seconds between perfmon recordings
    @retention  INT    Days of history to keep

Examples:
    EXEC sp_BaselinePerfmon @duration = 60*24, @delay = 60, @retention = 90
====================================================================================================================
Change History
Date        Author          Description	
05/24       Marco Assis     Initial Build
06/24       Marco Assis     fix comulative filter: Add 2 more types
====================================================================================================================
*/

SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON

/* Local Variables */
DECLARE @previous   	BIGINT
        ,@until     	DATETIME 	= dateadd(MINUTE,@duration,getdate())
        ,@now       	DATETIME2

/* Main Code *****************************************************************************************************/

-- Check / Create required schemas & tables
IF OBJECT_ID(N'Baseline.perfmon',N'U') IS NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Baseline')
        EXEC('CREATE SCHEMA [Baseline];');
    
    CREATE TABLE [Baseline].[perfmon](
        [sysdate]     [datetime]      NOT NULL,
        [object_name]   [nvarchar](128) NULL,
        [counter_name]  [nvarchar](128) NULL,
        [instance_name] [nvarchar](128) NULL,
        [cntr_type]     [int]           NOT NULL,
        [cntr_value]    [bigint]        NOT NULL,
        [previous]      [bigint]        NULL,
        [delta]         [bigint]        NULL,
        [elapsed]       [int]           NULL
    )
    ALTER AUTHORIZATION ON [Baseline].[perfmon] TO  SCHEMA OWNER
    -- Add Compression
    ALTER TABLE Baseline.perfmon REBUILD PARTITION = ALL
        WITH (DATA_COMPRESSION = PAGE);
    -- Create Indexes
    CREATE CLUSTERED INDEX  PK_sysdate                  ON Baseline.perfmon (sysdate);
    CREATE NONCLUSTERED INDEX IX_sysdate_object
    ON [Baseline].[perfmon] ([sysdate],[object_name])
        INCLUDE ([counter_name],[instance_name],[cntr_type],[cntr_value])
END

IF OBJECT_ID(N'Baseline.cntr_type',N'U') IS NULL
BEGIN
    CREATE TABLE Baseline.cntr_type (
        cntr_type   INT
        ,meaning    NVARCHAR(128)
    )
    INSERT INTO Baseline.cntr_type (cntr_type, meaning)
    VALUES  (65792,      N'Point-in-time')
            ,(272696320 ,N'Cumulative')
            ,(272696576 ,N'Cumulative')
            ,(537003264 ,N'Ratio value/base(1073939712)')
            ,(1073874176,N'Average measure for an operation over time. value/base(1073939712)' )
            ,(1073939712,N'Base value for 537003264/537003264')
END

-- Register sys.dm_os_performance_counters
WHILE (getdate() < @until)
BEGIN
    SELECT @now = CURRENT_TIMESTAMP

    ;WITH previous AS (
        SELECT  sysdate, 
                object_name, 
                counter_name, 
                instance_name, 
                cntr_type, 
                cntr_value
        FROM    Baseline.perfmon
        WHERE   1 = 1
            --AND cntr_type in (272696320,272696576)
            AND sysdate = (SELECT max(sysdate) FROM Baseline.perfmon)
    )
    INSERT INTO Baseline.perfmon (sysdate, object_name, counter_name, instance_name, cntr_type, cntr_value, previous, delta, elapsed)
        SELECT  @now
                ,rtrim(pc.object_name)
                ,rtrim(pc.counter_name)
                ,rtrim(pc.instance_name)
                ,pc.cntr_type
                ,pc.cntr_value
                ,previous.cntr_value
                ,case pc.cntr_type
                    when 272696320 then (pc.cntr_value - previous.cntr_value)
                    when 272696576 then (pc.cntr_value - previous.cntr_value)
                    else 0
                 end -- only makes sense for cumulative counters
                ,datediff(second,previous.sysdate,@now)
        FROM    sys.dm_os_performance_counters pc
        LEFT OUTER JOIN previous
                ON  previous.object_name    = pc.object_name
                AND previous.counter_name   = pc.counter_name
                AND previous.instance_name  = pc.instance_name

    WAITFOR DELAY @delay;
END

-- Purge old records
WHILE (@@ROWCOUNT > 0)
BEGIN
	DELETE TOP (@RowsPerBatch) FROM Baseline.perfmon
		WHERE sysdate <= dateadd(day,(@retention*-1),getdate());
END
GO

