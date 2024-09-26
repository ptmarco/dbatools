SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF OBJECT_ID('dbo.sp_kill_stale_DTCXact','P') IS NULL
	EXEC(N'CREATE PROCEDURE dbo.sp_kill_stale_DTCXact AS return 1;')
GO

ALTER PROCEDURE dbo.sp_kill_stale_DTCXact
	@hours_old SMALLINT = 1,
	@dont_kill BIT = 0
--WITH ENCRYPTION
AS
BEGIN
/*
====================================================================================================================
Description:    sp_kill_stale_DTCXact
                Kill long running stale DTCXact requests that might be causing locks
                Only for request_session_id = -2
Author:         Marco Assis
Tested On:      SQL Server 2012+
Notes:
				Code created for Filenet @ Unicre
Parameters:
                @hours_old  How old, in hours, to consider the connection "old" to be killed (default -1)
				@dont_kill  1 Dont kill transaction, only generate log, 0 Kill Transaction (default)
Returns:
                INSERT INTO Log.sp_kill_stale_DTCXact with 
                dm_tran_active_transactions and sys.syslock details for killed transactions for killed transactions
Examples:
                -- Only log, dont kill
				EXEC sp_kill_stale_DTCXact @hours_old = 1 , @dont_kill = 1
====================================================================================================================
Change History
v1
xx.05.24    Marco Assis     Initial Build
====================================================================================================================
License:
    GNU General Public License v3.0
    https://github.com/ptmarco/dbatools/blob/master/LICENSE

Github:
    https://github.com/ptmarco/dbatools/

You can contact me by e-mail at marcoassis@gmail.com
====================================================================================================================
*/

/*** Environment ***/
SET NOCOUNT ON;
SET STATISTICS XML OFF
SET STATISTICS IO OFF
SET STATISTICS TIME OFF
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET NOEXEC OFF

/*** Local Variables ***/
DECLARE @req_transactionUOW NCHAR(36),
		@kill NVARCHAR(MAX),
        @mail_profile SYSNAME,
        @recipients NVARCHAR(128),
        @body NVARCHAR(128)

SELECT TOP 1 @mail_profile = [name] FROM msdb.dbo.sysmail_profile WHERE [name] LIKE '%KYNDRYL%';
SELECT TOP 1 @recipients = email_address FROM msdb.dbo.sysoperators WHERE [name] LIKE '%KYNDRYL%' AND enabled = 1;

DECLARE c CURSOR FAST_FORWARD FOR
	SELECT	DISTINCT CAST(l.req_transactionUOW AS NVARCHAR(128))
	FROM	master..syslockinfo l
	LEFT OUTER JOIN 
			sys.dm_tran_active_transactions at
				ON at.transaction_id = l.req_transactionID
	WHERE	1=1
		AND req_spid = -2
		AND	req_transactionUOW != '00000000-0000-0000-0000-000000000000'
		AND at.transaction_begin_time <= dateadd(hour, (@hours_old * -1), getdate());

/*** Main Code ***/
OPEN c
FETCH NEXT FROM c INTO @req_transactionUOW

-- Create Log table if not exist
IF OBJECT_ID('Log.sp_kill_stale_DTCXact','U') IS NULL
	SELECT	getdate() [timestamp], l.*, at.*
	INTO	log.sp_kill_stale_DTCXact
	FROM	master..syslockinfo l
	LEFT OUTER JOIN 
			sys.dm_tran_active_transactions at
				ON at.transaction_id = l.req_transactionID
	WHERE	1=0;

WHILE (@@FETCH_STATUS = 0)
BEGIN
	-- Log Action
	INSERT INTO log.sp_kill_stale_DTCXact
	SELECT	getdate(), l.*, at.*
	FROM	master..syslockinfo l
	LEFT OUTER JOIN 
			sys.dm_tran_active_transactions at
				ON at.transaction_id = l.req_transactionID
	WHERE	1=1
		AND req_spid = -2
		AND	req_transactionUOW != '00000000-0000-0000-0000-000000000000'
		AND at.transaction_begin_time <= dateadd(hour, (@hours_old * -1), getdate())
        AND l.req_transactionUOW = @req_transactionUOW;
	
	-- Kill Transaction
	SELECT @kill = CONCAT(N'Kill ', quotename(@req_transactionUOW,''''))
	--PRINT @kill
	IF @dont_kill = 0
		EXEC(@kill);
        
    -- Notify DBA
    SELECT @body = CONCAT(N'Transaction ', @req_transactionUOW, N' Killed')
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = @mail_profile,
        @recipients = @recipients,
        @body = @body,
        @subject = N'Filenet DTCXact Stale Transaction Killed';
	
	FETCH NEXT FROM c INTO @req_transactionUOW
END

CLOSE c
DEALLOCATE c
END