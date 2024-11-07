SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
SET	NOEXEC OFF
GO

IF OBJECT_ID('sp_SqueezeDB','P') IS NULL 
	EXEC(N'CREATE PROCEDURE dbo.sp_SqueezeDB AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_SqueezeDB
	@DatabaseName           SYSNAME		=	NULL
    ,@TargetType            NVARCHAR(4)	=	N'LOG'
    ,@LeaveFreeSpace_pct    SMALLINT	=	10
    ,@MinimumGainMB         BIGINT		=	256
    ,@WhatIf				BIT			=	0
    
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Description:    sp_SqueezeDB
                Executes DBCC SHRINK FILE to all databases with freespace > MinimumGainMB 
                and leaves DB with @LeaveFreeSpace_pct % free space
Author:         Marco Assis
Tested On:      SQL Server 2012+
Notes:
    For Data Files executes
        1)  DBCC SHRINKFILE NOTRUNCATE
        2)  DBCC SHRINKFILE TRUNCATE -  Yes, Fragmentations will go awire. This is the cost of bad storage mangement policys, deal with it ...
        3)  DBCC SHRINKFILE to Current_Free_Space + @LeaveFreeSpace_pct %
    For Log Files (NOTRUNCATE is not an option)
        1)  DBCC SHRINKFILE to Current_Free_Space + @LeaveFreeSpace_pct %
Parameters:
     @DatabaseName          SYSNAME     = DB_NAME()
    ,@TargetType            NVARCHAR(4) = N'LOG'
    ,@LeaveFreeSpace_pct    INT         = 10
    ,@MinimumGainMB         LARGEINT    = 100    
    [ @WhatIf				BIT			= 1 ] optional
Returns:
    
Examples:
    EXEC sp_SqueezeDB 
        @databaseName           = N'DBCliente'
        ,@type                  = N'DATA'
        ,@LeaveFreeSpace_pct    = 10
        ,@MinimumGainMB         = 250
====================================================================================================================
Change History
v1
02.10.24    Marco Assis     Initial Build
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

/**** Local Variables ****/
DECLARE  @sql		NVARCHAR(MAX)

/**** Set Defaults for null parameters & Manipulate Parameters ****/
SELECT	@DatabaseName	=	ISNULL(@DatabaseName, DB_NAME())

/**** Main Code ****/

-- Check if Database exists and is suitable for operation
IF NOT EXISTS (
	SELECT	1 
	FROM	sys.databases 
	WHERE	1=1
		AND	name			= @databaseName 
		AND state			= 0 -- ONLINE
		AND is_read_only	= 0
		)
BEGIN
	RAISERROR(N'Specified database does not exist or is offline / read only', 20, 1) WITH LOG;
	RETURN;
END
ELSE
	PRINT	N'/*' + CHAR(13)
			+ N'Shrinking Database:' + CHAR(9)
			+ UPPER(@DatabaseName) + CHAR(13)
			+ N'File type:' + CHAR(9) + CHAR(9) + CHAR(9)
			+ UPPER(@TargetType)
			+ CHAR(13)
			+ N'Free Space Target:' + CHAR(9)
			+ CAST(@LeaveFreeSpace_pct AS NVARCHAR(3)) + N'%'
			+ CHAR(13)
			+ N'Minimum gain' + CHAR(9) + CHAR(9) + CAST(@MinimumGainMB AS NVARCHAR(16)) + ' MB'
			+ CHAR(13) + N'*/'

DECLARE c CURSOR FAST_FORWARD FOR
	SELECT	
		N'USE ' + QUOTENAME(DB_NAME(database_id))
		+ CHAR(13)
        + CASE
            WHEN @TargetType = 'ROWS' THEN 
		+       N'DBCC SHRINKFILE(''' + name + ''',' + CAST(ROUND(size/128*(100-@LeaveFreeSpace_pct)/100,0) AS NVARCHAR(32)) + N',NOTRUNCATE);' + CHAR(13)
            ELSE
                N''
            END
        + N'DBCC SHRINKFILE(''' + name + ''',TRUNCATEONLY);'
	FROM	master.sys.master_files
	WHERE	1=1
		AND	type_desc LIKE ISNULL(@TargetType,N'%')
		AND	DB_NAME(database_id)= @databaseName
		AND DB_NAME(database_id) NOT IN ('master','model','msd','tempdb','dba_database')
		AND ROUND(size/128*(100-@LeaveFreeSpace_pct)/100,0) >= @MinimumGainMB
OPEN c

FETCH NEXT FROM c INTO @sql
WHILE (@@FETCH_STATUS = 0)
BEGIN
	IF @WhatIf = 0 
		EXEC(@sql)
	ELSE
		PRINT @sql;
	FETCH NEXT FROM c INTO @sql
END

CLOSE c
DEALLOCATE c
GO


EXEC dbo.sp_SqueezeDB
    @DatabaseName           = N'PAY',
    @TargetType            = N'ROWS',
    @LeaveFreeSpace_pct    = 10,
    @MinimumGainMB         = 250
GO