/*
====================================================================================================================
Author:             Marco Assis
Description:        Active sessions detailed information
Tested On:          SQL Server 2012+
Notes:

Parameters:

Examples:
    EXEC sp_WhoIsActive @get_outer_command = 0, @show_system_spids = 0, @show_sleeping_spids = 1
    EXEC sp_WhatIsActive @include_system = 0

To Do:
    --plan
    --sql
    --sentence

====================================================================================================================
Change History
Date            Author              Description    
07/05/24        Marco Assis         Initial Build
====================================================================================================================
*/
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
SET TRANSACTION ISOLATION  LEVEL READ COMmITtED;
GO

IF OBJECT_ID('sp_WhatIsActive','P') IS NULL
    EXEC('CREATE PROCEDURE dbo.sp_WhatIsActive AS RETURN 1;');

GO
ALTER PROCEDURE dbo.sp_WhatIsActive
/*WITH ENCRYPTION -- Dont use, hides tsql from query_plan*/
@include_system BIT = 0
AS
SELECT --DISTINCT 
    r.session_id
    --,CONVERT(varchar, DATEADD(ms, DATEDIFF_BIG(SECOND,r.start_time,CURRENT_TIMESTAMP) * 1000, 0), 8) [running_for]
    ,r.start_time
    ,r.STATUS [status]
    ,c.TEXT [sql_text]
    ,qp.query_plan
    ,db_name(r.database_id) [Database]
    --,USER_NAME(r.user_id) [user]
    ,r.command
    ,s.login_name
    ,r.wait_type [wait_now]
    ,r.last_wait_type [wait_before]
    ,r.wait_time
    ,r.blocking_session_id [blocked_by]
    ,r.wait_resource
    ,r.percent_complete
    ,s.open_transaction_count [session_open_transaction_count]
    --,t.open_transactions
    --,CAST(DATEADD(ms,DATEDIFF(SECOND,t.oldest_open_transaction,CURRENT_TIMESTAMP) * 1000, 0) AS BIGINT) [transaction_running_ms]
    ,t.oldest_open_transaction [oldest_transaction]
    ,r.cpu_time [request_cpu_time]
    ,r.logical_reads [request_logical_reads]
    ,r.reads [request_physical_reads]
    ,r.writes [request_writes]
    ,r.row_count [request_row_count]
    ,s.cpu_time [session_cpu_time]
    ,s.logical_reads [session_logical_reads]
    ,s.reads [session_physical_reads]
    ,s.writes [session_writes]
    ,s.row_count [session_row_count]
    ,s.host_name
    ,s.program_name
    ,r.granted_query_memory
    ,r.dop
    --,l.resource_type [lock_request]
    --,l.request_owner_type [lock_type]
    --,l.request_status [lock_status]
    --,l.locks_total
    ,c.num_reads [connection_reads]
    ,c.num_writes [connection_writes]
    ,c.net_packet_size
    ,c.connect_time
    ,c.client_net_address
    ,c.client_tcp_port
    ,c.objectid
FROM sys.dm_exec_requests r
/*LEFT JOIN (
    SELECT DISTINCT request_session_id
        ,resource_database_id
        ,resource_type
        ,request_type
        ,request_status
        ,request_owner_type
        ,count(1) locks_total
    FROM sys.dm_tran_locks
    GROUP BY request_session_id
        ,resource_database_id
        ,resource_type
        ,request_type
        ,request_status
        ,request_owner_type
    ) l
    ON l.request_session_id = r.session_id*/
LEFT JOIN sys.dm_exec_sessions s
    ON s.session_id = r.session_id
LEFT OUTER JOIN (
    SELECT session_id
        ,num_reads
        ,num_writes
        ,net_packet_size
        ,connect_time
        ,client_net_address
        ,client_tcp_port
        ,sql_text.TEXT
        ,sql_text.objectid
    FROM sys.dm_exec_connections
    CROSS APPLY sys.dm_exec_sql_text(most_recent_sql_handle) sql_text
    ) c
    ON c.session_id = s.session_id
LEFT OUTER JOIN ( SELECT st.session_id, count(at.transaction_id) [open_transactions], min(at.transaction_begin_time) [oldest_open_transaction]
            FROM sys.dm_tran_session_transactions st
            LEFT JOIN sys.dm_tran_active_transactions at
                ON at.transaction_id = st.transaction_id
            GROUP BY st.session_id, st.open_transaction_count) t
    ON t.session_id = r.session_id
OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) qp
WHERE 1=1
    AND r.STATUS NOT IN ('background') -- background rollback running runnable sleeping suspended
    --AND r.STATUS = 'sleeping'
    AND r.session_id != @@SPID
    --AND r.blocking_session_id IS NOT NULL
    --AND r.blocking_session_id > 0;		
    AND r.session_id > (CASE @include_system WHEN 0 THEN 50 ELSE 0 END)
ORDER BY r.start_time ASC
GO