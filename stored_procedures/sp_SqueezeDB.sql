SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF OBJECT_ID('sp_SqueezeDB','P') IS NULL 
	EXEC(N'CREATE PROCEDURE dbo.sp_SqueezeDB AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_SqueezeDB
	@databaseName           SYSNAME     = DB_NAME()
    ,@type                  NVARCHAR(4) = N'LOG'
    ,@LeaveFreeSpace_pct    INT         = 10
    ,@MinimumGainMB         LARGEINT    = 100    
    
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
        2)  DBCC SHRINKFILE TRUNCATE
        3)  DBCC SHRINKFILE to Current_Free_Space + @LeaveFreeSpace_pct %
    For Log Files
        1)  DBCC SHRINKFILE to Current_Free_Space + @LeaveFreeSpace_pct %
Parameters:
    @databaseName           SYSNAME     = DB_NAME()
    ,@type                  NVARCHAR(4) = N'LOG'
    ,@LeaveFreeSpace_pct    INT         = 10
    ,@MinimumGainMB         LARGEINT    = 100    
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

/**** User Parameters ****/

/**** Local Variables ****/

/**** Exit ****/
RAISERROR(N'Oops! No, don''t just hit F5', 20, 1) WITH LOG;
SET NOEXEC ON

/**** Main Code ****/




/*
SUSER_NAME()
*/



log so normal
data 
    2 truncateonly 
    1 notruncate

-- Wasted Space from Offline Databases
select mf.name, mf.physical_name, size from sys.master_files mf
join sys.databases d
	on d.database_id = mf.database_id
WHERE d.state_desc = 'offline'
USE master
CREATE TABLE #FileSize
(dbName NVARCHAR(128), 
    FileName NVARCHAR(128), 
    type_desc NVARCHAR(128),
    CurrentSizeMB DECIMAL(10,2), 
    FreeSpaceMB DECIMAL(10,2)
);

-- Generate Shrink for All Databases
INSERT INTO #FileSize(dbName, FileName, type_desc, CurrentSizeMB, FreeSpaceMB)
exec sp_msforeachdb 
'use [?]; 
 SELECT DB_NAME() AS DbName, 
        name AS FileName, 
        type_desc,
        size/128.0 AS CurrentSizeMB,  
        size/128.0 - CAST(FILEPROPERTY(name, ''SpaceUsed'') AS INT)/128.0 AS FreeSpaceMB
FROM sys.database_files
WHERE type IN (0,1);';
    
SELECT * 
,'use ' + QUOTENAME(dbname) + ' DBCC SHRINKFILE (N''' + filename + ''' , ' + CAST(round((CurrentSizeMB*1.1),0) AS NVARCHAR(50)) + ', NOTRUNCATE)' "Shrink"
FROM #FileSize
WHERE dbName NOT IN ('distribution', 'master', 'model', 'msdb','tempdb')
AND type_desc = 'LOG'
ORDER BY FreeSpaceMB DESC
    
DROP TABLE #FileSize;
