SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF OBJECT_ID('sp_ServerDiagnostics','P') IS NULL -- U User Tables, FN Inline Functions, Inline Table Functions, P Stored Procedures
	EXEC(N'CREATE PROCEDURE dbo.sp_ServerDiagnostics AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_ServerDiagnostics
	@runfor_m	INT = 1
	,@delay_s	INT = 10
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Description:    sp_ServerDiagnostics
                Capture sp_server_diagnostics to ##sp_server_diagnostics andd isplays organized results
Author:         Marco Assis
Tested On:      SQL Server 2012+

Parameters:
	@runfor_m	INT # minutes to run diagnostics
	@delay_s	INT # seconds intervale between collections
Returns:
    Collected Data Organized by topic

Examples:
    EXEC sp_ServerDiagnostics 
        @runfor_m = 10, 
        @delay_s = 10
====================================================================================================================
Change History
v1
21.06.24    Marco Assis     Initial Build
====================================================================================================================
License:
    GNU General Public License v3.0
    https://github.com/ptmarco/dbatools/blob/master/LICENSE

Github:
    https://github.com/ptmarco/dbatools/

You can contact me by e-mail at marcoassis@gmail.com
====================================================================================================================
*/

/* Environment */
SET NOCOUNT ON;
SET STATISTICS XML OFF
SET STATISTICS IO OFF
SET STATISTICS TIME OFF
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET NOEXEC OFF

/* Local Variables */
DECLARE @delay		DATETIME
		,@stop		DATETIME

/* Main Code */

-- Create Temp Table
IF OBJECT_ID('tempdb..##sp_server_diagnostics','U') IS NOT NULL
	DROP TABLE ##sp_server_diagnostics;

CREATE TABLE ##sp_server_diagnostics (
    create_time DATETIME,
    component_type SYSNAME,
    component_name SYSNAME,
    [state] INT,
    state_desc SYSNAME,
    [data] XML
);

-- Start Colleting Diagnostics
SELECT @delay	= DATEADD(second,@delay_s,convert(DATETIME, 0))
SELECT @stop	= DATEADD(mi, @runfor_m, getdate())

WHILE getdate() <= @stop
BEGIN
	INSERT INTO ##sp_server_diagnostics
		EXEC sp_server_diagnostics;
	WAITFOR DELAY @delay
END

-- System
SELECT 'System' [Scope]
SELECT 
	create_time,
	data.value('(/system/@systemCpuUtilization)[1]', 'bigint') AS 'System_CPU',
    data.value('(/system/@sqlCpuUtilization)[1]', 'bigint') AS 'SQL_CPU',
    data.value('(/system/@nonYieldingTasksReported)[1]', 'bigint') AS 'NonYielding_Tasks',
    data.value('(/system/@pageFaults)[1]', 'bigint') AS 'Page_Faults',
    data.value('(/system/@latchWarnings)[1]', 'bigint') AS 'Latch_Warnings',
    data.value('(/system/@BadPagesDetected)[1]', 'bigint') AS 'BadPages_Detected',
    data.value('(/system/@BadPagesFixed)[1]', 'bigint') AS 'BadPages_Fixed'
FROM ##sp_server_diagnostics
WHERE component_name LIKE 'system'
ORDER BY create_time DESC

-- Resource Monitor
SELECT 'Resource Monitor' [Scope]
SELECT 
	create_time,
	data.value('(./Record/ResourceMonitor/Notification)[1]', 'VARCHAR(max)') AS [Notification],
    data.value('(/resource/memoryReport/entry[@description=''Working Set'']/@value)[1]', 'bigint') / 1024 AS [SQL_Mem_in_use_MB],
    data.value('(/resource/memoryReport/entry[@description=''Available Paging File'']/@value)[1]', 'bigint') / 1024 AS [Avail_Pagefile_MB],
    data.value('(/resource/memoryReport/entry[@description=''Available Physical Memory'']/@value)[1]', 'bigint') / 1024 AS [Avail_Physical_Mem_MB],
    data.value('(/resource/memoryReport/entry[@description=''Available Virtual Memory'']/@value)[1]', 'bigint') / 1024 AS [Avail_VAS_MB],
    data.value('(/resource/@lastNotification)[1]', 'varchar(100)') AS 'LastNotification',
    data.value('(/resource/@outOfMemoryExceptions)[1]', 'bigint') AS 'OOM_Exceptions'
FROM ##sp_server_diagnostics
WHERE component_name LIKE 'resource'
ORDER BY create_time DESC

-- Nonpreemptive waits
SELECT 'Nonpreemptive waits' [Scope]
SELECT 
	waits.evt.value('(@waitType)', 'varchar(100)') AS 'Wait_Type',
	create_time,
    waits.evt.value('(@waits)', 'bigint') AS 'Waits',
    waits.evt.value('(@averageWaitTime)', 'bigint') AS 'Avg_Wait_Time',
    waits.evt.value('(@maxWaitTime)', 'bigint') AS 'Max_Wait_Time'
FROM ##sp_server_diagnostics
CROSS APPLY data.nodes('/queryProcessing/topWaits/nonPreemptive/byDuration/wait') AS waits(evt)
WHERE component_name LIKE 'query_processing'
ORDER BY 1, create_time DESC

-- Preemptive waits
SELECT 'Preemptive waits' [Scope]
SELECT 
	waits.evt.value('(@waitType)', 'varchar(100)') AS 'Wait_Type',
	create_time,
    waits.evt.value('(@waits)', 'bigint') AS 'Waits',
    waits.evt.value('(@averageWaitTime)', 'bigint') AS 'Avg_Wait_Time',
    waits.evt.value('(@maxWaitTime)', 'bigint') AS 'Max_Wait_Time'
FROM ##sp_server_diagnostics
CROSS APPLY data.nodes('/queryProcessing/topWaits/preemptive/byDuration/wait') AS waits(evt)
WHERE component_name LIKE 'query_processing'
ORDER BY 1, create_time DESC

-- CPU intensive requests
SELECT 'CPU intensive requests' [Scope]
SELECT 
	create_time,
	cpureq.evt.value('(@sessionId)', 'bigint') AS 'SessionID',
    cpureq.evt.value('(@command)', 'varchar(100)') AS 'Command',
    cpureq.evt.value('(@cpuUtilization)', 'bigint') AS 'CPU_Utilization',
    cpureq.evt.value('(@cpuTimeMs)', 'bigint') AS 'CPU_Time_ms'
FROM ##sp_server_diagnostics
CROSS APPLY data.nodes('/queryProcessing/cpuIntensiveRequests/request') AS cpureq(evt)
WHERE component_name LIKE 'query_processing'
ORDER BY create_time DESC

-- Blocked process report
SELECT 'Blocked process report' [Scope]
SELECT 
	create_time,
	blk.evt.query('.') AS 'Blocked_Process_Report_XML'
FROM ##sp_server_diagnostics
CROSS APPLY data.nodes('/queryProcessing/blockingTasks/blocked-process-report') AS blk(evt)
WHERE component_name LIKE 'query_processing'
ORDER BY create_time DESC

-- Input/output
SELECT 'IO' [Scope]
SELECT 
	create_time,
	data.value('(/ioSubsystem/@ioLatchTimeouts)[1]', 'bigint') AS 'Latch_Timeouts',
    data.value('(/ioSubsystem/@totalLongIos)[1]', 'bigint') AS 'Total_Long_IOs'
FROM ##sp_server_diagnostics
WHERE component_name LIKE 'io_subsystem'
ORDER BY create_time DESC

-- Event information
SELECT 'Events' [Scope]
SELECT 
	xevts.evt.value('(@name)', 'varchar(100)') AS 'xEvent_Name',
	create_time,
    xevts.evt.value('(@package)', 'varchar(100)') AS 'Package',
    xevts.evt.value('(@timestamp)', 'datetime') AS 'xEvent_Time',
    xevts.evt.query('.') AS 'Event Data'
FROM ##sp_server_diagnostics
CROSS APPLY data.nodes('/events/session/RingBufferTarget/event') AS xevts(evt)
WHERE component_name LIKE 'events'
ORDER BY 1, create_time DESC

SELECT N'Feel free to query ##sp_server_diagnostics while this session exists' [Next]
GO

