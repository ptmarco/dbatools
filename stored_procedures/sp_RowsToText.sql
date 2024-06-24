SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID('sp_RowsToText','P') IS NULL
    EXEC('CREATE PROCEDURE dbo.sp_RowsToText AS SELECT 1;')
GO

ALTER PROCEDURE dbo.sp_RowsToText
--WITH ENCRYPTION
AS
BEGIN
/*
====================================================================================================================
Author:         Marco Assis
Create date:    05/2024
Description:    Returns String with ALL the columns + order for a specified database / object / Index
Tested On:      SQL Server 2012+
Notes:          

Parameters:
    Must create temp table ##code and insert text
Examples:
    EXEC sp_RowsToText 
====================================================================================================================
Change History
Date        Author			Description	
27.05.24	Marco Assis		Initial Build
====================================================================================================================
*/
DECLARE c CURSOR FAST_FORWARD FOR
    SELECT code FROM ##code ORDER BY row ASC;

DECLARE @sql NVARCHAR(MAX) = N'',
        @row NVARCHAR(MAX) = N''

OPEN c
FETCH NEXT FROM c INTO @row
WHILE (@@FETCH_STATUS = 0)
BEGIN
    SET @sql = @sql + @row
    FETCH NEXT FROM c INTO @row
END
CLOSE c
DEALLOCATE c
SELECT @sql;

END
GO

/* testes
set nocount on;
create or alter procedure dbo.sp_teste
as 
    select db_name() [Database]
go
if object_id('tempdb..##code','U') IS NOT NULL drop table ##code
create table ##code (row int identity(1,1), code nvarchar(255));
insert into ##code
    EXEC sp_helptext 'dbo.sp_teste'
exec dbo.sp_RowsToText;
declare @sql nvarchar(max)
if object_id('tempdb..##text','U') IS NOT NULL drop table ##text
create table ##text (text nvarchar(max));
INSERT INTO ##text 
	EXEC dbo.sp_RowsToText
SELECT @sql = replace(text,'dbo.','#') FROM ##text
--select object_id('tempdb..sp_teste','P') "id"
PRINT @sql;
EXEC(@sql);
GO
*/