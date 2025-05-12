SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

-- fn_BackupDestination
IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'fn_BackupDestination')
	DROP FUNCTION dbo.fn_BackupDestination;
GO

CREATE FUNCTION dbo.fn_BackupDestination (@media_set_id INT)
RETURNS NVARCHAR(1000)
--WITH ENCRYPTION
AS
BEGIN
	-- =======================================================
	-- Author:		Marco Assis
	-- Create date: 21/03/2022
	-- Description:	Returns 1st row for Backup Destination
	-- Parameters:	media_set_id
	-- =======================================================
	DECLARE @location NVARCHAR(1000)
	
	SELECT 	@location = physical_device_name
	FROM 	msdb.dbo.backupmediafamily
	WHERE	family_sequence_number = 1
	AND		media_set_id = @media_set_id
	
	RETURN @location

END
GO

-- sp_BackupInfo
IF OBJECT_ID('dbo.sp_BackupInfo') IS  NULL
    EXEC ('CREATE PROCEDURE dbo.sp_BackupInfo AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_BackupInfo
	/* Parameters */
   @type			TINYINT			= 1,
   @help			TINYINT			= 0,		
   @backuptype		NVARCHAR(4)		= NULL,
   @database		NVARCHAR(50)	= NULL,		
   @days			TINYINT			= 7,			
   @excludecopyonly	BIT 			= 0,
   @excludeoffline	BIT				= 0,
   @excludelogback	BIT				= 0
--WITH ENCRYPTION
AS
BEGIN

	IF @Help = 1
	BEGIN
		PRINT '
	/*=====================================================================================================
		Author:        	Marco Assis
		Create date:   	24/01/22
		Description:   	Lists Backup Information
		Tested on:		SQL 2012, 2014, 2016, 2017, 2019
	---------------------------------------------------------------------------------------------------
		Parameters:
		@type				= 1 Backup History for @daysback, 
							= 2 Current Backup/Restore Progress
							= 3 Last Backup per Database & Backup Type
		@database			= For specific database, NULL for all
		@Days				= # of days to look back
		@backuptype			= null (default) for ALL, LOG/DIFF/FULL (for @type = 1)
		@excludecopyonly	= 0 no, 1 yes -- Only for @type = 3
		@excludeoffline		= 0 no, 1 yes -- Only for @type = 3
		@excludelogback		= 0 no, 1 yes -- Only for @type = 3
	---------------------------------------------------------------------------------------------------
	---------------------------------------------------------------------------------------------------
		Change History
		Date   		Version		Author       	Description
					v1.0 		Marco Assis		Initial Build
		07/02/22	v1.1		Marco Assis		Add Progress Report
		18/03/22	v1.2		Marco Assis		Add @type = 3
		21/03/22	v1.2.1		Marco Assis		Add @excludelogback
		29/06/2022	v.1.2.2		Marco Assis		Add elapsed + mb_per_sec + Minor esthetic improvments
	---------------------------------------------------------------------------------------------------
		You may alter this code for your own *non-commercial* purposes. You may
		republish altered code as long as you include this copyright and give due
		credit, but you must obtain prior permission before blogging this code.
		   
		THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
		ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
		TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
		PARTICULAR PURPOSE.
	=====================================================================================================*/
';
	RETURN;
	END

	--DECLARE @sql NVARCHAR(2000) = N'';
	DECLARE @bcktype NCHAR(1);

	IF @type = 1
	BEGIN
		SELECT @bcktype = 
			CASE UPPER(@backuptype)
				WHEN 'LOG' THEN N'L'
				WHEN 'FULL' THEN N'D'
				WHEN 'DIFF' THEN N'I'
				ELSE NULL
			END
		SELECT 	'Type 1: Backup History for @daysback =' as [Type], 
				@days as [Days], 
				ISNULL(@database, N'-') as [Database], 
				ISNULL(UPPER(@backuptype),N'-') as [Backup Type];
		SELECT 
		CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, 
		msdb.dbo.backupset.database_name, 
		msdb.dbo.backupset.backup_start_date, 
		msdb.dbo.backupset.backup_finish_date, 
		DATEDIFF(mi,msdb.dbo.backupset.backup_start_date,msdb.dbo.backupset.backup_finish_date) as elapsed_min,
		msdb.dbo.backupset.expiration_date, 
		CASE msdb..backupset.type 
			WHEN 'D' THEN 'FULL' 
			WHEN 'L' THEN 'LOG'
			WHEN 'I' THEN 'DIFF'
			WHEN 'F' THEN 'FILE/FILEGROUP'
			WHEN 'G' THEN 'DIFF FILE'
			WHEN 'P' THEN 'PARTIAL'
			WHEN 'Q' THEN 'DIFF PARTIAL'
			ELSE msdb..backupset.type 
		END AS backup_type,
		msdb.dbo.backupset.is_copy_only,
		CONVERT(DECIMAL(20,1),msdb.dbo.backupset.backup_size/1048576.0) as size_mb, 
		CASE 
			WHEN DATEDIFF(ss,msdb.dbo.backupset.backup_start_date,msdb.dbo.backupset.backup_finish_date) <= 0 THEN 0
			ELSE CONVERT(DECIMAL(20,1),
						(msdb.dbo.backupset.backup_size/1048576.0) /
						CONVERT(BIGINT,DATEDIFF(ss,msdb.dbo.backupset.backup_start_date,msdb.dbo.backupset.backup_finish_date))
						) 
		END as mb_per_sec,
		msdb.dbo.backupmediafamily.logical_device_name, 
		msdb.dbo.backupmediafamily.physical_device_name, 
		msdb.dbo.backupset.name AS backupset_name, 
		msdb.dbo.backupset.description,
		msdb.dbo.backupset.USER_NAME
		FROM msdb.dbo.backupmediafamily 
		INNER JOIN msdb.dbo.backupset 
		ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id 
		WHERE 	(CONVERT(datetime, msdb.dbo.backupset.backup_start_date, 102) >= GETDATE() - @days)
				AND msdb.dbo.backupset.database_name LIKE ISNULL(@database,'%')
				AND msdb..backupset.type LIKE ISNULL(@bcktype,'%')
		ORDER BY 
		msdb.dbo.backupset.database_name, 
		msdb.dbo.backupset.backup_start_date DESC,
		msdb..backupset.type
	END
	
	IF @type = 2
	BEGIN
		SELECT 'Type 2: Current Backup/Restore Progress' as [Type]
		SELECT	r.session_id
				,r.command
				,s.text
				,r.start_time
				,r.percent_complete
				,CAST(((DATEDIFF(s,start_time,GetDate()))/3600) AS varchar) + ' hour(s), '
						+ CAST((DATEDIFF(s,start_time,GetDate())%3600)/60 AS varchar) + ' min, '
						+ CAST((DATEDIFF(s,start_time,GetDate())%60) AS varchar) + ' sec' 
					AS elapsed
				,CAST((estimated_completion_time/3600000) AS varchar) + ' hour(s), '
						+ CAST((estimated_completion_time %3600000)/60000 AS varchar) + ' min, '
						+ CAST((estimated_completion_time %60000)/1000 AS varchar) + ' sec' 
					AS estimated_remaining
				,DATEADD(ss,estimated_completion_time/1000, GETDATE()) 
					AS estimated_completion
		FROM	sys.dm_exec_requests r
		CROSS	APPLY sys.dm_exec_sql_text(r.sql_handle) s
		WHERE	r.command in ('RESTORE DATABASE', 'BACKUP DATABASE', 'RESTORE LOG', 'BACKUP LOG')
	END

	IF @type = 3 AND @excludelogback = 0
	BEGIN
		SELECT 'Type 3: Last Backup per Database & Backup Type' as [Type], @excludecopyonly as [Exclude COPY_ONY]
				,@excludelogback as [Exclude Log Backups], @excludeoffline as [Exclude OFFLINE Databases];
		;WITH lastID AS (
			SELECT 
				d.name,
				d.state_desc,
				d.recovery_model_desc,
				MAX(CASE WHEN bs.type = 'D' THEN bs.media_set_id ELSE NULL END) AS LFID,
				MAX(CASE WHEN bs.type = 'I' THEN bs.media_set_id ELSE NULL END) AS LDID,
				MAX(CASE WHEN bs.type = 'L' THEN bs.media_set_id ELSE NULL END) AS LLID
			FROM		sys.databases d
			LEFT JOIN	msdb.dbo.backupset bs
						ON d.name = bs.database_name
			WHERE		d.name NOT LIKE N'tempdb'
					AND d.name LIKE ISNULL(@database,'%')	
					AND ISNULL(bs.is_copy_only,0) <= CASE WHEN @excludecopyonly = 1 THEN 0 ELSE 1 END
			GROUP BY d.name, d.state_desc, d.recovery_model_desc, bs.is_copy_only
			)
			SELECT	LastID.name											as [Database]
					,LastID.state_desc									as [Status]
					,LastID.recovery_model_desc							as [Recovery_Model]
					,DATEDIFF(dd,bsF.backup_finish_date,getdate())		as [Full_Days_Old]
					,bsF.backup_finish_date								as [Last_Full _Finish_Date]
					,dba_database.dbo.fn_BackupDestination(lastID.LFID)	as [Last_Full_Destination]
					,bsF.is_copy_only									as [Last_Full_Copy_Only]
					,DATEDIFF(dd,bsD.backup_finish_date,getdate())		as [Diff_Days_Old]
					,bsD.backup_finish_date								as [Last_Diff_Finish_Date]
					,dba_database.dbo.fn_BackupDestination(lastID.LDID)	as [Last_Diff_Destination]
					,bsD.is_copy_only									as [Last_Diff_Copy_Only]
					,DATEDIFF(mi,bsL.backup_finish_date,getdate())		as [Log_Min_Old]
					,bsL.backup_finish_date								as [Last_Log_Finish_Date]
					,dba_database.dbo.fn_BackupDestination(lastID.LLID)	as [Last_Log_Destination]
			FROM	lastID 
			LEFT JOIN	msdb.dbo.backupset bsF
					ON bsF.media_set_id = lastID.LFID
			LEFT JOIN	msdb.dbo.backupset bsD
					ON bsD.media_set_id = lastID.LDID
			LEFT JOIN	msdb.dbo.backupset bsL
					ON bsL.media_set_id = lastID.LLID
			WHERE 	LastID.state_desc LIKE 
						CASE WHEN @excludeoffline = 0 THEN '%'
						ELSE N'ONLINE'
						END
			ORDER BY	lastID.name
	END

	IF @type = 3 AND @excludelogback = 1
	BEGIN
		SELECT 'Type 3: Last Backup per Database & Backup Type' as [Type], @excludecopyonly as [Exclude COPY_ONY]
		,@excludelogback as [Exclude Log Backups], @excludeoffline as [Exclude OFFLINE Databases];
		;WITH lastID AS (
			SELECT 
				d.name,
				d.state_desc,
				d.recovery_model_desc,
				MAX(CASE WHEN bs.type = 'D' THEN bs.media_set_id ELSE NULL END) AS LFID,
				MAX(CASE WHEN bs.type = 'I' THEN bs.media_set_id ELSE NULL END) AS LDID
			FROM		sys.databases d
			LEFT JOIN	msdb.dbo.backupset bs
						ON d.name = bs.database_name
			WHERE		d.name NOT LIKE N'tempdb'
					AND d.name LIKE ISNULL(@database,'%')	
					AND ISNULL(bs.is_copy_only,0) <= CASE WHEN @excludecopyonly = 1 THEN 0 ELSE 1 END
			GROUP BY d.name, d.state_desc, d.recovery_model_desc, bs.is_copy_only
			)
			SELECT	DISTINCT
					LastID.name											as [Database]
					,LastID.state_desc									as [Status]
					,LastID.recovery_model_desc 						as [Recovery_Model]
					,DATEDIFF(dd,bsF.backup_finish_date,getdate())		as [Full_Days_Old]
					,bsF.backup_finish_date								as [Last_Full _Finish_Date]
					,dba_database.dbo.fn_BackupDestination(lastID.LFID)	as [Last_Full_Destination]
					,bsF.is_copy_only									as [Last_Full_Copy_Only]
					,DATEDIFF(dd,bsD.backup_finish_date,getdate()) 		as [Diff_Days_Old]
					,bsD.backup_finish_date								as [Last_Diff_Finish_Date]
					,dba_database.dbo.fn_BackupDestination(lastID.LDID)	as [Last_Diff_Destination]
					,bsD.is_copy_only									as [Last_Diff_Copy_Only]
			FROM	lastID 
			LEFT JOIN	msdb.dbo.backupset bsF
					ON bsF.media_set_id = lastID.LFID
			LEFT JOIN	msdb.dbo.backupset bsD
					ON bsD.media_set_id = lastID.LDID
			WHERE LastID.state_desc LIKE 
					CASE WHEN @excludeoffline = 0 THEN '%'
					ELSE N'ONLINE'
					END
			ORDER BY	lastID.name
	END

END
GO