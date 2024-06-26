IF OBJECT_ID('dbo.sp_IndexInfo') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_IndexInfo AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_IndexInfo
	@TableName SYSNAME		=	'%'
	,@SchemaName SYSNAME	= '%'
	,@StatsLevel SYSNAME	= 'SAMPLED'
--WITH ENCRYPTION
AS
BEGIN
/*
====================================================================================================================
Author:			Marco Assis
Create date:	03/2024
Description:	Returns Detaild Index Information
====================================================================================================================
Change History
Date   			Author       	Description	

====================================================================================================================
Example:
*/
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
SET NOCOUNT ON;

SELECT 
	t.object_id, 
	s.name + '.' + t.name [object_name],
	i.index_id,
	i.name [index_name], 
	i.type,
	i.type_desc,
	i.is_primary_key, 
	is_unique, 
	is_unique_constraint, 
	is_ms_shipped
	,is_disabled
	,stats.partition_number
	,stats.page_count
	,stats.record_count
	,stats.ghost_record_count
	,stats.alloc_unit_type_desc
	,stats.avg_fragmentation_in_percent
	,stats.avg_page_space_used_in_percent
	,stats.avg_fragment_size_in_pages
	,stats.fragment_count
	,stats.forwarded_record_count
FROM sys.tables t
INNER JOIN sys.schemas s 
	ON s.schema_id = t.schema_id
INNER JOIN sys.indexes i 
	ON i.object_id = t.object_id
CROSS APPLY sys.dm_db_index_physical_stats(db_id(),i.object_id, i.index_id, NULL, @StatsLevel) stats -- DETAILED | LIMITED | SAMPLED
WHERE 1=1
	AND t.name LIKE ISNULL(@TableName,'%')
	AND s.name LIKE ISNULL(@SchemaName,'%')
	--AND i.type > 0
	AND t.is_ms_shipped = 0
	AND t.name<>'sysdiagrams'
	--AND (is_primary_key=0 and is_unique_constraint=0)
ORDER BY t.name, i.index_id

END
GO