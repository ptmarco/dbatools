SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF OBJECT_ID('sp_CheckForBackup','P') IS NULL
    EXEC(N'CREATE PROCEDURE dbo.sp_CheckForBackup AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_CheckForBackup
    @Full                       BIT             = 0,
    @Diff                       BIT             = 0,
    @Log                        BIT             = 0,
    @DatabaseNameFilter         NVARCHAR(64)    = N'%',
    @DatabaseNameExcludeFilter  NVARCHAR(64)    = N'',
    @ThresholdMinutes           INT         	= 10081,		-- 8 dias + 1 min
    @ExcludeCopyOnlyBackups     BIT             = 1,
    @DatabaseAgeMinimumHours    INT             = 24,
    @ExcludeSystemDatabases     BIT             = 0,
    @RaiseError                 BIT             = 1,
    @SendMailTo                 NVARCHAR(64)    = NULL,			-- N'marco.assis@kyndryl.com',
    @SubjectTag					NVARCHAR(64)	= N'[WARNING]',	-- Identify Customer on email Subject &/or Incident Severity
    @ErrorLevel                 SMALLINT        = 16,
    @ErrorState                 SMALLINT        = 1
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Description:    Find Databases with backups older than specified threshold
Author:         Marco Assis
Tested On:      SQL Server 2012 - 2022
Notes:
    > !!!! Currently does not support AGs with backup @ secondary node !!!!
    > Notify by email not done yet
Parameters:
	Mandatory
	n/a
	Optional
    @Full                       BIT             Scope Backup Type               Default = 0 - ONLY 1 BACKUP TYPE ALLOWED
    @Diff                       BIT             Scope Backup Type               Default = 0 - ONLY 1 BACKUP TYPE ALLOWED
    @Log                        BIT             Scope Backup Type               Default = 0 - ONLY 1 BACKUP TYPE ALLOWED
    @DatabaseNameFilter         NVARCHAR(32)    Include String filter,          Default = %
    @DatabaseNameExcludeFilter  NVARCHAR(32)    Exclude String filter,          Default = %
    @ThresholdMinutes           INT             Backup Age Threshold            Default = 192 (8 days)
    @DatabaseAgeMinimumHours    INT             Minimum hours for               Default = 25
												database create_date to exclude 
                                                recent databases that might not 
                                                have backups yet.
    @ExcludeCopyOnlyBackups     BIT             Exclude copy_only backups       Default = true
    @ExcludeSystemDatabases     BIT             Exclude system databases        Default = false
    @RaiseError                 BIT             RAISERROR for each db in fault  Default = true
    @SendMailTo                 NVARCHAR(64)    email address(s) to notify IF   Default = NULL
                                                there is a workingt default mail
                                                profile
	@SubjectTag					NVARCHAR(64)	email Subject Start TAG			Default = '[WARNING]'
    @ErrorLevel                 TINYINT         Error Level                     Default = 16
    @ErrorState                 SMALLINT        Error S
                                                                                    
Returns:
	1. List of databases in scope
	2. Last backup information IF last backup is older than threshold
    3. RAISERROR no Erro Log for each Database with Last Backup older than threshold
Examples:
    EXEC dbo.sp_CheckBackup 
        @Log                        = true,
        @DatabaseNameFilter         = N'%IPS%',
        @DatabaseNameExcludeFilter  = N'%dba_%',
        @ThresholdMinutes           = 192,
        @DatabaseAgeMinimumHours    = 25,
        @ExcludeCopyOnlyBackups     = 1,
        @RaiseError                 = true
====================================================================================================================
Change History
v1
xx.10.24    Marco Assis     Initial Build
xx.xx.24    MArco Assis     Add notification by mail    
====================================================================================================================
License:
    GNU General Public License v3.0
    https://github.com/ptmarco/dbatools/blob/master/LICENSE

Github:
    https://github.com/ptmarco/dbatools/

You can contact me by e-mail at marcoassis@gmail.com
====================================================================================================================
*/

/**** Environment ****/
SET NOCOUNT ON;
SET STATISTICS XML OFF
SET STATISTICS IO OFF
SET STATISTICS TIME OFF
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET NOEXEC OFF

/**** Local User Parameters ****/

/**** Local Variables ****/
DECLARE @BackupType         CHAR(1),
        @BackupTypeDesc     NVARCHAR(12),
        @TypeCheck          SMALLINT;

DECLARE @Targets TABLE (
    database_id INT,
    name        SYSNAME
);

/**** Main Code ****/

/* Determine Backup Type */
SELECT @TypeCheck = ( CAST(@Full AS INT) + CAST(@Diff AS INT) + CAST(@Log AS INT) );
IF  @TypeCheck != 1 
    BEGIN
        IF @TypeCheck > 1
            PRINT N'Error: Only 1 backup type allowed.'         + CHAR(13) + N'Example: EXEC dbo.sp_CheckForBackup @Diff = true'
        ELSE
            PRINT N'Error: Only 1 Backup type can be specified' + CHAR(13) + N'Example: EXEC dbo.sp_CheckForBackup @Diff = true';
        RETURN 1;
    END
ELSE
    SELECT @BackupType = CASE 
                            WHEN @Full  = 1  THEN 'D'
                            WHEN @Diff  = 1  THEN 'I'
                            WHEN @Log   = 1  THEN 'L'
                        END

/* Determine Eligible Databases */
INSERT INTO @Targets
    SELECT  database_id, name
    FROM    sys.databases
    WHERE   1=1
        AND name                NOT IN      ('tempdb')                                              -- tempdb doesnt allow backups
        AND state               NOT IN      (6,10)                                                  -- Exclude Offline Databases
        AND create_date         <           DATEADD(HOUR, @DatabaseAgeMinimumHours * -1 ,GETDATE()) -- Exclude recent databases
        AND name                LIKE        @DatabaseNameFilter                                     -- Database Name Filter
        AND name                NOT LIKE    @DatabaseNameExcludeFilter
        -- Mandatory Full Recovery
        AND recovery_model      <           CASE @Log
                                                WHEN 1 THEN 3
                                                ELSE        999
                                            END
        AND database_id         >           CASE @ExcludeSystemDatabases
                                                WHEN 1 THEN 4
                                                ELSE        0
                                            END;
-- Display for informationl purpose
SELECT  *
FROM    @Targets
UNION ALL
SELECT -1 [database_id], N'**** Databases Checked ****' [name]
ORDER BY database_id ASC;

/* Get Latest Backup, of specified type if exits, for each Scope Database */
BEGIN TRY
    ;WITH LastBackup AS (
			SELECT  d.name, 
					bs.type,
					MAX(bs.backup_set_id) [max_backup_set_id]
					FROM @Targets d
			INNER JOIN
				msdb.dbo.backupset bs
					ON d.name = bs.database_name
			WHERE   1=1
				AND  bs.type = @BackupType
				AND	(
					bs.is_copy_only = 0
					OR	bs.is_copy_only = 
							CASE @ExcludeCopyOnlyBackups
								WHEN 1 THEN 0
								ELSE 1
							END
					)
			GROUP BY 
				d.name, bs.type
	)
    SELECT  t.name, 
            CASE bs.type
                WHEN 'D' THEN 'FULL'
                WHEN 'I' THEN 'DIFF'
                WHEN 'L' THEN 'LOG'
                ELSE 'OTHER'
            END AS [Backup Type],
            CAST(DATEDIFF(DAY, bs.backup_finish_date, GETDATE()) AS DECIMAL (10,1)) [backup_age_d],
            CAST(@ThresholdMinutes/60.0/24.0 AS DECIMAL (6,2)) [Threshold_d],
            CAST(DATEDIFF(HOUR, bs.backup_finish_date, GETDATE()) AS DECIMAL (10,1)) [backup_age_h],
            CAST(@ThresholdMinutes/60.0 AS DECIMAL (6,2)) [Threshold_h],
            CAST(DATEDIFF(MINUTE, bs.backup_finish_date, GETDATE()) AS DECIMAL (10,1)) [backup_age_m],
            @ThresholdMinutes [Threshold_m],
            d.recovery_model_desc,
            d.state_desc,
            bs.backup_set_id,
            bs.type,
            bs.is_copy_only, 
            bs.backup_finish_date,
            CAST(bs.compressed_backup_size/128 AS DECIMAL(16,2)) [compressed_backup_size_mb],
            bs.description,
            bs.user_name,
            bs.name as software_name,
            bms.MTF_major_version,
            bms.is_compressed,
            --bms.is_encrypted,
            bms.is_password_protected
    INTO    #missing_backup
    FROM    @Targets t
    LEFT OUTER JOIN 
            LastBackup lb
                ON lb.name = t.name
    LEFT OUTER JOIN    
            msdb.dbo.backupset bs
                ON bs.backup_set_id = lb.max_backup_set_id
    LEFT OUTER JOIN
            msdb.dbo.backupmediaset bms
                ON bms.media_set_id = bs.media_set_id
    INNER JOIN
            sys.databases d
                ON d.name = t.name
    WHERE   1=1
        AND bs.backup_finish_date < DATEADD(MINUTE, @ThresholdMinutes * -1 , GETDATE()) 
        OR	bs.backup_finish_date IS NULL

    SELECT  *
    FROM    #missing_backup
    ORDER BY backup_finish_date DESC;

    /* RAISERROR & SEND MAIL */
    IF @RaiseError = 1 OR @SendMailTo IS NOT NULL
    BEGIN
		-- Local Variables
        DECLARE @dbName     SYSNAME
                ,@errormsg  SYSNAME
                ,@profile	NVARCHAR(128) = NULL
                ,@subject	NVARCHAR(128) = NULL
                ,@body		NVARCHAR(999) = NULL
                ;
        
        SELECT @BackupTypeDesc = CASE 
                                    WHEN @Full  = 1  THEN 'FULL'
                                    WHEN @Diff  = 1  THEN 'DIFFERENTIAL'
                                    WHEN @Log   = 1  THEN 'LOG'
                                END;
		
		-- Get Mail Profile if exists
		IF @SendMailTo IS NOT NULL
			IF EXISTS (SELECT 1 FROM msdb.dbo.sysmail_principalprofile WHERE is_default = 1)
			BEGIN
				SELECT @profile = p.name 
				FROM	msdb.dbo.sysmail_principalprofile pp
				INNER JOIN
						msdb.dbo.sysmail_profile p
							ON p.profile_id = pp.profile_id
				WHERE	1=1
					AND pp.is_default = 1;
			END
			ELSE
				SELECT 'Database Mail not available' [Error];
        
        -- Cursor for offending databases
        DECLARE c CURSOR FAST_FORWARD FOR 
            SELECT  name
            FROM    #missing_backup;
        
        OPEN c
        FETCH NEXT FROM c INTO @dbName;
        
        WHILE (@@FETCH_STATUS = 0)
        BEGIN
            SELECT @dbName = UPPER(@dbName); -- For better readability ;-)
            -- RAISE ERROR
            IF @RaiseError = 1
				RAISERROR(
							'%s BACKUP for database [%s] is either MISSING or too OLD'
							,@ErrorLevel
							,@ErrorState
							,@BackupTypeDesc
							,@dbName
						 ) WITH LOG
            
            -- SEND MAIL
            IF @profile IS NOT NULL
				BEGIN
					SELECT	@subject = @SubjectTag + N' ' + @@SERVERNAME + N' ' + @BackupTypeDesc + N' BACKUP for database [' + @dbname + 'N] is either MISSING or too OLD'
							--,@query  = N'SELECT * FROM #missing_backup WHERE name = ''' + @dbName + N''' ORDER BY backup_finish_date DESC;'
							,@body = N'Last successful' + @BackupType + N' backup for database ' + name + N' was  on ' + CAST(backup_finish_date AS NVARCHAR(64))
					FROM	#missing_backup
					WHERE	name = @dbName;
					
					EXEC msdb.dbo.sp_send_dbmail
						@profile_name = @profile,
						@recipients = @SendMailTo,
                        --@query = @query,
						@subject=@subject, 
						@body=@body;
				END
            
            FETCH NEXT FROM c INTO @dbName
        END
    CLOSE c
    DEALLOCATE c
    END

END TRY
BEGIN CATCH
    SELECT  ERROR_NUMBER() AS ErrorNumber,
            ERROR_MESSAGE() AS ErrorMessage;
    RETURN 0;
END CATCH
    RETURN 1;
GO

RETURN

/*** Testing  ***/
EXEC dbo.sp_CheckForBackup
    @ThresholdMinutes           = 100 -- 1h 60 | 24h 1440 | 7d 10080 | 30d 43200
    ,@Full                      = true
    --,@Log                       = true
    --,@Diff                      = true
    ,@ExcludeSystemDatabases    = false
    ,@ExcludeCopyOnlyBackups    = false
    ,@RaiseError                = false
    ,@SendMailTo				= N'marco.assis@kyndryl.com'
    ,@SubjectTag				= N'CIN - [WARNING]'
    ,@DatabaseNameFilter        = 'Cin%'
    ,@DatabaseNameExcludeFilter = 'dba_%'

EXEC sp_readerrorlog 0,1,'backup'
GO