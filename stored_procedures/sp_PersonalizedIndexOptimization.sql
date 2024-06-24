SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF OBJECT_ID('sp_PersonalizedIndexOptimization.sql','P') IS NULL 
	EXEC(N'CREATE PROCEDURE dbo.sp_PersonalizedIndexOptimization AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_PersonalizedIndexOptimization
	@object_id INT,
	@index_id INT
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Description:    sp_PersonalizedIndexOptimization
                Executes Ola Hallengrens sp_IndexOptimize with my custom parameters
Author:         Marco Assis
Tested On:      SQL Server 2012+
Notes:
    <none>
Parameters:
    @TargetDatabases    NVARCHAR(256)          default: 'ALL_DATABASES,-dba_%'
    ,@Indexes           NVARCHAR(1024)         default: 'ALL_INDEXES'
    ,@PrintOnly         BIT                    default: 0 (false)
Returns:
    Nothing
Examples:
    EXEC sp_PersonalizedIndexOptimization
        @TargetDatabases = 'ALL_DATABASES,-dba_%'
        ,@Indexes = 'ALL_INDEXES''
        ,@PrintOnly = 0
====================================================================================================================
Change History
v1
06.24    Marco Assis     Initial Build
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

/* Main Code */

DECLARE @sql					NVARCHAR(max)
		,@cores					SMALLINT
		,@TargetFinishHour		SMALLINT		= NULL -- formato 24h, exemplo para 20 para 20:00
		,@SecondsUntilFinish	INT

SELECT	@cores = count(scheduler_id)
		,@SecondsUntilFinish = CASE @TargetFinishHour
								WHEN NULL THEN NULL
								ELSE CAST( DATEDIFF(mi,getdate(),DATEADD(hour, @TargetFinishHour, DATEDIFF(dd, 0, GETDATE())))  * 60 AS nvarchar(12) )
							   END
FROM	sys.dm_os_schedulers 
WHERE	status = 'VISIBLE ONLINE'

SET @cores = CASE 
				WHEN @cores		=	1	THEN	1
				WHEN @cores		<=	4	THEN	@cores - 1
				WHEN @cores		>	4	THEN	@cores - FLOOR(@cores / 4)
				END

SET @sql = N'EXEC [dba_database].[dbo].[IndexOptimize]
	@Databases=''ALL_DATABASES,-IAF,-dba_database'',
	@FragmentationLow = NULL,
	@FragmentationMedium = ''INDEX_REORGANIZE,INDEX_REBUILD_ONLINE'',
	@FragmentationHigh = ''INDEX_REBUILD_ONLINE,INDEX_REORGANIZE'',
	@FragmentationLevel1 = 30,
	@FragmentationLevel2 = 70,
	@UpdateStatistics = ''ALL'',
	@OnlyModifiedStatistics = ''Y'',
	@StatisticsResample = ''N'',
	@SortInTempdb = ''Y'',
	@MAXDOP = ' + CAST(@cores as NVARCHAR(2)) + ',
	@PartitionLevel = ''Y'',
	@Indexes = ''-IAF.dbo.ufx_h2h_interchange_adddata.uadt_id_fich_reg_type_elim_idx,-IAF.dbo.ufx_h2h_interchange_adddata.uadt_pk,-imagem_diaria_mmov5_auxiliar_fechos.idmf_pk'',
	@LogToTable = ''Y'''

-- Add time windows
SELECT @sql = @sql 
			+ CASE 
				WHEN @SecondsUntilFinish IS NULL THEN N''
				ELSE N',' + CHAR(13) + '	@TimeLimit = ' +CAST(@SecondsUntilFinish AS nvarchar(12)) + N';'
			  END

PRINT @sql;
--EXEC sp_executesql @sql
