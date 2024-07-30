/*
use dba_database
select @@servername
select * from dba_database..commandlog where endtime >= dateadd(hour,-8,getdate())
*/

SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF OBJECT_ID('sp_IndexOptimize','P') IS NULL -- U User Tables, FN Inline Functions, Inline Table Functions, P Stored Procedures
	EXEC(N'CREATE PROCEDURE dbo.sp_IndexOptimize AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_IndexOptimize
	@Databases      nvarchar(256)   =   N'ALL_DATABASES,-dba%',
	@Indexes        nvarchar(1024)  =   N'ALL_INDEXES',
    @SortInTempDB   nchar(1)        =   N'N',
    @TerminateTime  nvarchar(5)     =   NULL,
	@Duration		int				=   NULL,
    @FragLevel1		int				=   30,
	@FragLevel2		int				=   70,
	@PrintOnly		bit				=   0
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Description:    sp_IndexOptimize
                Launch Marco Assis personalized Ola Hallengren sp_IndexOptimize
Author:         Marco Assis
Tested On:      SQL Server 2012+
Notes:
    
Parameters:
    @Database       nvarchar(512)   Target Database(s)      , Default = 'ALL_DATABASES,-dba%'
    @Index          nvarchar(256)   Target Index(s) Name    , Default = 'ALL_INDEXES'
    @SortInTempDB   bit             Sort in tempdb          , Default = 'N'
	@Duration		int				Max hours to run		, Default = NULL (No limit)
    @Terminate time mnvarchar(5)    Process Time Window     , Default = NULL (No limit) if not null will overwrite @Durante
    @FragmentationLevel1            Thershold for Level 1   , Default= 30
    @FragmentationLevel2            Thershold for Level 1   , Default= 70
	@PrintOnly						Print (1), Execute (0)	, Default = 0

Returns:
    
Examples:
    EXEC sp_OptimizeIndex
        @Database       = 'IAF', 
        @Index          = 'pk_tabela_coluna', 
        @SortInTempDB   = 'N',
        @Terminate      = '23:00',
        @FragLevel1      = 25,
        @FragLevel2      = 65
====================================================================================================================
Change History
25.06.24    Marco Assis     Initial Build
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
DECLARE @sql    nvarchar(max),
        @cores  smallint,
        @limit  varchar(16) -- # seconds until optimization ends

/* Main Code */

-- Time limit
IF @TerminateTime IS NOT NULL
    SELECT @limit = CAST( DATEDIFF(mi,getdate(),CAST(CAST(sysutcdatetime() as date) as datetime) + CAST(@TerminateTime AS datetime)) * 60 AS NVARCHAR(12) )
ELSE 
	IF @Duration IS NOT NULL
		SELECT @limit = CAST( DATEDIFF(second,getdate(), dateadd(mi,@Duration,getdate())) * 60 AS NVARCHAR(12))
	ELSE
		SELECT @limit = 172800; -- 48 hours;

-- MAXDOP
SELECT	@cores = count(scheduler_id)
FROM	sys.dm_os_schedulers 
WHERE	status = 'VISIBLE ONLINE'

SET @cores =    CASE 
                    WHEN @cores		=	1	THEN	1
                    WHEN @cores		<=	4	THEN	@cores - 1
                    WHEN @cores		>	4	THEN	@cores - FLOOR(@cores / 4)
                END

-- Generate tsql
SET @sql = N'EXEC dbo.IndexOptimize
    @Databases							= ''ALL_DATABASES,-dba%'',
	@Indexes							= ''ALL_INDEXES'',
	@FragmentationLow					= NULL,
	@FragmentationMedium				= ''INDEX_REORGANIZE,INDEX_REBUILD_ONLINE'',
	@FragmentationHigh					= ''INDEX_REBUILD_ONLINE,INDEX_REORGANIZE'',	
	@FragmentationLevel1				= ' +  CAST(@FragLevel1 AS CHAR(3)) + N',
	@FragmentationLevel2				= ' +  CAST(@FragLevel2 AS CHAR(3)) + N',
	@UpdateStatistics					= NULL, -- ALL / INDEX / COLUMNS
	@OnlyModifiedStatistics				= ''Y'',
	@StatisticsSample					= NULL, -- Sample size %
	@StatisticsResample					= ''N'', 
	@FillFactor							= 100,
	@LOBCompaction						= ''Y'',
	@MAXDOP								= ' + CAST(@cores as NVARCHAR(2)) + ',
	@PartitionLevel						= ''Y'',
	@WaitAtLowPriorityMaxDuration		= 120, -- wait for index lock for x secs 
	@WaitAtLowPriorityAbortAfterWait	= ''SELF'', -- who looses dispute for lock after max duration expires
	@TimeLimit							= ' + @limit + ', -- seconds until stop optimizing
	@LogToTable							= ''Y'', -- register operations @commandLog
	@Execute							= ''Y''
'

IF @SortInTempDB = N'Y'
    SELECT @sql += N',
    @SortInTempdb						= ''Y'''
ELSE
    BEGIN
        -- Resumable only for >= SQL 2016
        IF SERVERPROPERTY('ProductMajorVersion') >= 14 OR SERVERPROPERTY('EngineEdition') IN (5, 8)
            SELECT @sql += N',
        @Resumable							= ''Y'''
        -- WaitAtLowPriority* only for >= SQL 2016
	    SELECT @sql = REPLACE(@sql, N'@WaitAtLowPriority', N'--@WaitAtLowPriority')
    END

-- Use tempdb during week days (don't risk blowing up db space to spare standby)
--IF DATEPART(WEEKDAY,getdate()) BETWEEN 2 AND 6
--	SELECT @SQL = REPLACE(@sql,N'@SortInTempdb						= ''N'',' , N'@SortInTempdb						= ''Y'',')

-- Print / Execute
IF @PrintOnly = 1
	PRINT @sql
ELSE 
	EXEC sp_executesql @sql

GO