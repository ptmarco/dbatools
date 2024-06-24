IF OBJECT_ID('dbo.sp_ScriptDropIndex') IS NULL
    EXEC ('CREATE PROCEDURE dbo.sp_ScriptDropIndex AS RETURN 1;');
GO
ALTER PROCEDURE dbo.sp_ScriptDropIndex 
	@target_object_id INT
    ,@target_index_id SYSNAME
--WITH ENCRYPTION
AS

/*
====================================================================================================================
Author:         Marco Assis
Create date:    05/2024
Description:    Return NVARCHAR with ALL columns for a Index
Tested On:      SQL Server 2012+
Notes:          

Parameters:
    ject_id INT,
    @index_id INT,
    @is_include_column INT
Examples:
    
====================================================================================================================
Change History
Date        Author          Description	

====================================================================================================================
*/

SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

DECLARE @FilterSchemaName VARCHAR(255) = '%' -- '%' = all, 'dbo'
,@FilterTableName VARCHAR(255) = '%' -- '%' = all
,@FilterIndexName VARCHAR(255) = '%' -- '%' = all, '%PK%'
,@FilterIndexType VARCHAR(50) = '%' -- '%' = all, 'CLUSTERED', 'CLUSTERED COLUMNSTORE', 'NONCLUSTERED', 'NONCLUSTERED HASH', 'SPATIAL', 'XML'

------------------------------------------------------------------------------------------------------

DECLARE
-- Variables for CursorIndex
@SchemaName VARCHAR(256)
,@TableName VARCHAR(256)
,@IndexName VARCHAR(256)
,@IndexTypeDesc VARCHAR(100)
,@IsPrimaryKey BIT
,@IsUniqueConstraint BIT

-- Other Variables
,@TabSpaces VARCHAR(4)
,@TsqlScriptDropIndex VARCHAR(MAX)


SELECT @TabSpaces = ' ' -- used to simulate a tab to tidy up the output code

------------------------------------------------------------------------------------------------------

DECLARE CursorIndex CURSOR FOR

SELECT
SCHEMA_NAME(t.schema_id) AS SchemaName
,t.name AS TableName
,i.name AS IndexName
,i.type_desc AS IndexTypeDesc
,i.is_primary_key AS IsPrimaryKey
,i.is_unique_constraint AS IsUniqueConstraint -- not actually used

FROM sys.indexes i

INNER JOIN sys.tables t
ON t.object_id = i.object_id
AND t.is_ms_shipped <> 1 -- ignore system tables

WHERE t.is_ms_shipped = 0 -- ignore system tables
AND t.name <> 'sysdiagrams'
AND i.name IS NOT NULL
AND i.type > 0 -- to ignore HEAPs
--AND i.is_primary_key = 0 -- to exclude PRIMARY KEY indexes
--AND i.is_unique_constraint = 0
AND SCHEMA_NAME(t.schema_id) LIKE @FilterSchemaName
AND t.name LIKE @FilterTableName
AND i.name LIKE @FilterIndexName
AND i.type_desc LIKE @FilterIndexType
AND i.index_id = @target_index_id
AND t.object_id = @target_object_id

ORDER BY
SCHEMA_NAME(t.schema_id)
,t.name
,i.is_primary_key -- sort primary keys lower
,i.name

OPEN CursorIndex
FETCH NEXT FROM CursorIndex INTO @SchemaName, @TableName, @IndexName, @IndexTypeDesc, @IsPrimaryKey, @IsUniqueConstraint
WHILE (@@fetch_status = 0)
BEGIN

-- Build the TSQL Script
SET @TsqlScriptDropIndex = 'IF EXISTS (SELECT * FROM SYS.INDEXES WHERE OBJECT_ID = OBJECT_ID(''' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ''') AND NAME = ''' + @IndexName + ''')'
+ CHAR(13)
+ 'BEGIN'
+ CHAR(13) + @TabSpaces
+ 'PRINT CONVERT(VARCHAR, GETDATE(), 120) + '': dropping '
+ CASE
WHEN @IsPrimaryKey = 1
THEN 'PRIMARY KEY constraint/index: '
ELSE + @IndexTypeDesc + ' index: '
END
+ QUOTENAME(@IndexName) + ''''
+ CHAR(13) + @TabSpaces
+ CASE
WHEN @IsPrimaryKey = 1
THEN 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' DROP CONSTRAINT ' + QUOTENAME(@IndexName)
ELSE 'DROP INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)
END
+ CHAR(13)
+ 'END'
+ CHAR(13) + CHAR(13)

-- Output the TSQL Script to the Messsages Window
PRINT @TsqlScriptDropIndex

FETCH NEXT FROM CursorIndex INTO @SchemaName, @TableName, @IndexName, @IndexTypeDesc, @IsPrimaryKey, @IsUniqueConstraint

END
CLOSE CursorIndex
DEALLOCATE CursorIndex 