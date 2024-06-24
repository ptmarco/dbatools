IF OBJECT_ID('sp_GetIndexColumns','P') IS NULL
	EXEC(N'CREATE PROCEDURE dbo.sp_GetIndexColumns AS RETURN 1;')
GO

ALTER PROCEDURE dbo.sp_GetIndexColumns
	@object_id          INT,
	@index_id           INT,
	@is_include_column  INT,
	@db                 SYSNAME
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Author:         Marco Assis
Create date:    05/2024
Description:    Returns String with ALL the columns + order for a specified database / object / Index
Tested On:      SQL Server 2012+
Notes:          

Parameters:
    @object_id INT,
    @index_id INT,
    @is_include_column INT
	@db SYSNAME
Examples:
    EXEC sp_GetIndexColumns @object_id = 12314324, @index_id = 1, @is_include_column = 0, @db = 'dba_database'
====================================================================================================================
Change History
Date        Author			Description	
13.05.24	Marco Assis		Initial Build
====================================================================================================================
*/

SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

DECLARE @sql NVARCHAR(MAX) = N'
DECLARE 
	@object_id INT			= ' + CAST(@object_id AS NVARCHAR(64)) +'
	,@index_id INT			= ' + CAST(@index_id AS NVARCHAR(64)) +'
	,@is_include_column INT	= ' + CAST(@is_include_column AS NVARCHAR(64)) +'
	,@db SYSNAME				= ''' + @db + ''';
IF EXISTS	(
			SELECT 1 
			FROM ' + @db + N'.sys.index_columns 
			WHERE object_id = @object_id 
				AND index_id = @index_id 
				AND is_included_column = @is_include_column
			)
BEGIN
	DECLARE @columns NVARCHAR(MAX),
			@cname SYSNAME,
			@order VARCHAR(4);

	DECLARE c CURSOR FAST_FORWARD FOR
		SELECT	c.[name]
				,CASE ic.is_descending_key
					WHEN 1 THEN ''DESC''
					ELSE ''ASC''
				END
		FROM ' + @db + N'.sys.index_columns ic
		JOIN ' + @db + '.sys.columns c
			ON ic.column_id = c.column_id AND ic.object_id = c.object_id
		WHERE ic.object_id = @object_id
			AND ic.index_id = @index_id
			AND ic.is_included_column = @is_include_column
		ORDER BY ic.index_column_id ASC;	
			
	OPEN c
	FETCH NEXT FROM c INTO @cname, @order
	SELECT @columns = N''(''

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		SELECT @columns = @columns + @cname + '' '' + @order + N'', ''
		FETCH NEXT FROM c INTO @cname, @order
	END
	CLOSE c
	DEALLOCATE c
END
SELECT @columns = @columns + N'')''
SELECT REPLACE(@columns,'', )'','')'') "columns"';

EXEC(@sql);
--PRINT @sql;
GO