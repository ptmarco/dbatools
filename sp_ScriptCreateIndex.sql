IF OBJECT_ID('dbo.sp_ScriptCreateIndex') IS NULL
    EXEC ('CREATE PROCEDURE dbo.sp_ScriptCreateIndex AS RETURN 1;');
GO

ALTER PROCEDURE dbo.sp_ScriptCreateIndex 
	@target_object_id INT
    ,@target_index_id SYSNAME
-- WITH ENCRYPTION
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
DECLARE -- Variables for CursorIndex
    @SchemaName VARCHAR(100)
    ,@TableName VARCHAR(256)
    ,@IndexName VARCHAR(256)
    ,@IsUnique VARCHAR(100)
    ,@IndexTypeDesc VARCHAR(100)
    ,@IndexOptions VARCHAR(MAX)
    ,@IsDisabled VARCHAR(100)
    ,@FileGroupName VARCHAR(100)
    ,@DataCompressionType VARCHAR(100)
    ,@IsFiltered BIT
    ,@FilterDefinition VARCHAR(MAX)
    ,@IsPrimaryKey BIT
    ,@IsUniqueConstraint BIT
    ,@CompressionDelay INT
    -- Variables for CursorIndexColumn
    ,@ColumnName VARCHAR(100)
    ,@IsDescendingKey INT
    ,@IsIncludedColumn INT
    -- Other Variables
    ,@TabSpaces VARCHAR(4)
    ,@TsqlScriptBeforeIndex VARCHAR(MAX)
    ,@TsqlScriptCreateIndex VARCHAR(MAX)
    ,@TsqlScriptDisableIndex VARCHAR(MAX)
    ,@TsqlScriptAfterIndex VARCHAR(MAX)
    ,@IndexColumns VARCHAR(MAX)
    ,@IncludedColumns VARCHAR(MAX)
SELECT @TabSpaces = '    ' -- used to simulate a tab to tidy up the output code
------------------------------------------------------------------------------------------------------
DECLARE CursorIndex CURSOR
FOR
-- CTE to collect partitioned index information
WITH PartitionedIndexes
AS (
    SELECT t.object_id AS ObjectID
        ,t.name AS TableName
        ,ic.column_id AS PartitioningColumnID
        ,c.name AS PartitioningColumnName
        ,s.name AS PartitionScheme
        ,ix.name AS IndexName
        ,ix.index_id
    FROM sys.tables t
    INNER JOIN sys.indexes i
        ON i.object_id = t.object_id
    INNER JOIN sys.index_columns ic
        ON ic.index_id = i.index_id
            AND ic.object_id = t.object_id
    INNER JOIN sys.columns c
        ON c.object_id = ic.object_id
            AND c.column_id = ic.column_id
    INNER JOIN sys.partition_schemes s
        ON s.data_space_id = i.data_space_id
    INNER JOIN sys.indexes ix
        ON ix.object_id = t.object_id
            AND ix.index_id = i.index_id
    WHERE ic.partition_ordinal = 1 -- only want 1 record per index at this stage
        AND t.is_ms_shipped = 0 -- ignore system tables
        AND t.object_id = @target_object_id
        AND i.index_id = @target_index_id
    )
SELECT SCHEMA_NAME(t.schema_id) AS SchemaName
    ,t.name AS TableName
    ,ix.name AS IndexName
    ,CASE 
        WHEN ix.is_unique = 1
            THEN 'UNIQUE '
        ELSE ''
        END AS IsUnique
    ,CASE 
        WHEN t.object_id IN (
                SELECT object_id
                FROM sys.tables
                WHERE is_memory_optimized = 1
                )
            AND ix.type_desc <> 'NONCLUSTERED HASH'
            THEN 'MEMORY_OPTIMIZED'
        ELSE ix.type_desc
        END AS TypeDesc -- SQL Server 2014 & later
    --,ix.type_desc AS TypeDesc -- SQL Server 2012 & earlier (is_memory_optimized doesn't exist)
    ,CASE 
        WHEN ix.is_padded = 1
            THEN 'PAD_INDEX = ON, '
        ELSE 'PAD_INDEX = OFF, '
        END + CASE 
        WHEN INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1
            THEN 'STATISTICS_NORECOMPUTE = ON, '
        ELSE 'STATISTICS_NORECOMPUTE = OFF, '
        END + CASE 
        WHEN ix.is_primary_key = 1
            OR ix.is_unique_constraint = 1
            THEN ''
        ELSE 'SORT_IN_TEMPDB = OFF, '
        END + CASE 
        WHEN ix.ignore_dup_key = 1
            THEN 'IGNORE_DUP_KEY = ON, '
        ELSE 'IGNORE_DUP_KEY = OFF, '
        END + CASE 
        WHEN ix.type_desc NOT LIKE '%COLUMNSTORE%'
            AND @@VERSION LIKE '%ENTERPRISE%'
            AND ix.is_primary_key = 0
            AND ix.is_unique_constraint = 0
            AND (
                (
                    LOBTable.CanBeBuiltOnline IS NULL
                    AND ix.type_desc = 'CLUSTERED'
                    )
                OR (
                    LOBIndexes.CanBeBuiltOnline IS NULL
                    AND ix.type_desc <> 'CLUSTERED'
                    )
                )
            THEN 'ONLINE = ON, '
        ELSE ''
        END + CASE 
        WHEN ix.allow_row_locks = 1
            THEN 'ALLOW_ROW_LOCKS = ON, '
        ELSE 'ALLOW_ROW_LOCKS = OFF, '
        END + CASE 
        WHEN ix.allow_page_locks = 1
            THEN 'ALLOW_PAGE_LOCKS = ON, '
        ELSE 'ALLOW_PAGE_LOCKS = OFF, '
        END + CASE 
        WHEN CAST(ix.fill_factor AS VARCHAR(3)) = 0
            THEN ''
        ELSE 'FILLFACTOR =' + CAST(ix.fill_factor AS VARCHAR(3)) + ', '
        END + CASE 
        WHEN p.data_compression_desc IS NULL
            THEN 'DATA_COMPRESSION = NONE'
        ELSE 'DATA_COMPRESSION = ' + p.data_compression_desc
        END AS IndexOptions
    ,ix.is_disabled AS IsDisabled
    ,CASE 
        WHEN ic.IsColumnPartitioned = 1
            THEN '[' + PIdx.PartitionScheme + ']' + '(' + '[' + PIdx.PartitioningColumnName + ']' + ')'
        WHEN ic.IsColumnPartitioned = 0
            THEN '[' + FILEGROUP_NAME(ix.data_space_id) + ']'
        END AS FileGroupName
    ,ix.has_filter AS HasFilter
    ,ix.filter_definition AS FilterDefinition
    ,ix.is_primary_key AS IsPrimaryKey
    ,ix.is_unique_constraint AS IsUniqueConstraint
    ,ix.compression_delay AS CompressionDelay -- SQL Server 2014 and later
    --,NULL AS CompressionDelay -- SQL Server 2012 and earlier (compression_delay doesn't exist)
FROM sys.tables t
INNER JOIN sys.indexes ix
    ON ix.object_id = t.object_id
INNER JOIN (
    SELECT DISTINCT object_id
        ,index_id
        ,MAX(partition_ordinal) AS IsColumnPartitioned
    FROM sys.index_columns
    GROUP BY object_id
        ,index_id
    ) ic
    ON ic.index_id = ix.index_id
        AND ic.object_id = t.object_id
LEFT OUTER JOIN (
    SELECT DISTINCT object_id
        ,index_id
        ,data_compression_desc
    FROM sys.partitions
    ) p
    ON p.object_id = ix.object_id
        AND p.index_id = ix.index_id
LEFT OUTER JOIN PartitionedIndexes PIdx
    ON PIdx.ObjectID = t.object_id
        AND PIdx.index_id = ix.index_id
LEFT OUTER JOIN (
    SELECT DISTINCT c.object_id
        ,0 AS CanBeBuiltOnline
    FROM sys.columns c
    INNER JOIN sys.types t
        ON t.user_type_id = c.user_type_id
    WHERE t.Name IN ('image', 'ntext', 'text', 'XML')
        OR (
            t.Name IN ('VARCHAR', 'nVARCHAR', 'varbinary')
            AND c.max_length = - 1
            )
        OR c.is_filestream = 1
    ) LOBTable
    ON LOBTable.object_id = t.object_id
LEFT OUTER JOIN (
    SELECT DISTINCT c.object_id
        ,i.index_id
        ,0 AS CanBeBuiltOnline
    FROM sys.columns c
    INNER JOIN sys.types t
        ON t.user_type_id = c.user_type_id
    LEFT OUTER JOIN sys.index_columns ic
        ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
    INNER JOIN sys.indexes i
        ON i.object_id = ic.object_id
            AND i.index_id = ic.index_id
    WHERE t.Name IN ('image', 'ntext', 'text', 'XML')
        OR (
            t.Name IN ('VARCHAR', 'nVARCHAR', 'varbinary')
            AND c.max_length = - 1
            )
        OR c.is_filestream = 1
    ) LOBIndexes
    ON LOBIndexes.object_id = t.object_id
        AND LOBIndexes.index_id = ix.index_id
WHERE t.is_ms_shipped = 0 -- ignore system tables
    AND t.name <> 'sysdiagrams'
    AND ix.name IS NOT NULL
    AND ix.type > 0 -- to ignore HEAPs
    --AND ix.is_primary_key = 0 -- to ignore PRIMARY KEY indexes
    --AND ix.is_unique_constraint = 0
    AND SCHEMA_NAME(t.schema_id) LIKE @FilterSchemaName
    AND t.name LIKE @FilterTableName
    AND ix.name LIKE @FilterIndexName
    AND ix.type_desc LIKE @FilterIndexType
    AND t.object_id = @target_object_id
    AND ix.index_id = @target_index_id
ORDER BY SCHEMA_NAME(t.schema_id)
    ,t.name
    ,CASE 
        WHEN ix.type_desc NOT LIKE '%COLUMNSTORE%'
            AND @@VERSION LIKE '%ENTERPRISE%'
            AND ix.is_primary_key = 0
            AND ix.is_unique_constraint = 0
            AND (
                (
                    LOBTable.CanBeBuiltOnline IS NULL
                    AND ix.type_desc = 'CLUSTERED'
                    )
                OR (
                    LOBIndexes.CanBeBuiltOnline IS NULL
                    AND ix.type_desc <> 'CLUSTERED'
                    )
                )
            THEN 0
        ELSE 1
        END -- sort 'ONLINE = ON' indexes higher
    ,CASE 
        WHEN ix.is_primary_key = 1
            THEN 0
        ELSE 1
        END -- sort primary keys higher
    ,CASE 
        WHEN ix.type_desc LIKE '%COLUMNSTORE%'
            THEN 1
        ELSE 0
        END -- sort columnstores lower
    ,ix.name
OPEN CursorIndex
FETCH NEXT
FROM CursorIndex
INTO @SchemaName
    ,@TableName
    ,@IndexName
    ,@IsUnique
    ,@IndexTypeDesc
    ,@IndexOptions
    ,@IsDisabled
    ,@FileGroupName
    ,@IsFiltered
    ,@FilterDefinition
    ,@IsPrimaryKey
    ,@IsUniqueConstraint
    ,@CompressionDelay
WHILE (@@fetch_status = 0)
BEGIN
    SELECT @IndexColumns = ''
        ,@IncludedColumns = ''
        ,@DataCompressionType = ''
    ---------------------------------------------------
    DECLARE CursorIndexColumn CURSOR
    FOR
    SELECT col.name AS ColumnName
        ,ixc.is_descending_key AS IsDescendingKey
        ,ixc.is_included_column AS IsIncludedColumn
    FROM sys.tables tb
    INNER JOIN sys.indexes ix
        ON tb.object_id = ix.object_id
    INNER JOIN sys.index_columns ixc
        ON ix.object_id = ixc.object_id
            AND ix.index_id = ixc.index_id
    INNER JOIN sys.columns col
        ON ixc.object_id = col.object_id
            AND ixc.column_id = col.column_id
    WHERE SCHEMA_NAME(tb.schema_id) = @SchemaName
        AND tb.name = @TableName
        AND ix.name = @IndexName
    --AND ( ix.type > 0 AND ix.is_primary_key = 0 AND ix.is_unique_constraint = 0 ) -- to ignore PK indexes
    --AND ix.type_desc <> 'CLUSTERED' -- to ignore CLUSTERED indexes
    --AND ix.type_desc NOT LIKE '%COLUMNSTORE%' -- to ignore COLUMNSTORE indexes
    ORDER BY ixc.key_ordinal -- this is actually the genuine index column order, not ixc.index_column_id
    OPEN CursorIndexColumn
    FETCH NEXT
    FROM CursorIndexColumn
    INTO @ColumnName
        ,@IsDescendingKey
        ,@IsIncludedColumn
    WHILE (@@fetch_status = 0)
    BEGIN
        IF (
                @IsIncludedColumn = 0
                OR @IndexTypeDesc LIKE '%COLUMNSTORE%'
                OR @IndexTypeDesc IN ('XML', 'spatial', 'NONCLUSTERED HASH')
                )
            SET @IndexColumns = CASE 
                    WHEN @IndexTypeDesc LIKE '%COLUMNSTORE%'
                        OR @IndexTypeDesc IN ('XML', 'spatial', 'NONCLUSTERED HASH')
                        THEN @IndexColumns + QUOTENAME(@ColumnName) + ', '
                    ELSE @IndexColumns + QUOTENAME(@ColumnName) + CASE 
                            WHEN @IsDescendingKey = 1
                                THEN ' DESC, '
                            ELSE ' ASC, '
                            END
                    END
        ELSE
            SET @IncludedColumns = @IncludedColumns + QUOTENAME(@ColumnName) + ', '
        FETCH NEXT
        FROM CursorIndexColumn
        INTO @ColumnName
            ,@IsDescendingKey
            ,@IsIncludedColumn
    END
    CLOSE CursorIndexColumn
    DEALLOCATE CursorIndexColumn
    ---------------------------------------------------
    -- Build the TSQL Script
    SELECT @IndexColumns = SUBSTRING(@IndexColumns, 1, CASE 
                WHEN LEN(@IndexColumns) = 0
                    THEN 0
                ELSE LEN(@IndexColumns) - 1
                END)
        ,@IncludedColumns = CASE 
            WHEN LEN(@IncludedColumns) > 0
                THEN SUBSTRING(@IncludedColumns, 1, LEN(@IncludedColumns) - 1)
            ELSE ''
            END
        ,@DataCompressionType = SUBSTRING(@IndexOptions, CHARINDEX('DATA_COMPRESSION', @IndexOptions) + 18, LEN(@IndexOptions))
        ,@TsqlScriptBeforeIndex = 'IF NOT EXISTS (SELECT * FROM SYS.INDEXES WHERE OBJECT_ID = OBJECT_ID(''' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ''') AND NAME = ''' + @IndexName + ''')' + CHAR(13) + @TabSpaces + 'AND EXISTS (SELECT * FROM SYS.OBJECTS WHERE OBJECT_ID = OBJECT_ID(''' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + '''))' + CHAR(13) + 'BEGIN ' + CHAR(13) + @TabSpaces + 'PRINT CONVERT(VARCHAR, GETDATE(), 120) + '': creating ' + CASE 
            WHEN @IsPrimaryKey = 1
                THEN '(PRIMARY KEY) '
            ELSE ''
            END + @IndexTypeDesc + ' index: ' + QUOTENAME(@IndexName) + ''''
        ,@TsqlScriptAfterIndex = 'END ' + CHAR(13)
        ,@TsqlScriptDisableIndex = ''
    SET @TsqlScriptCreateIndex = CASE 
            WHEN @IndexTypeDesc LIKE '%COLUMNSTORE%'
                THEN CASE 
                        WHEN @IndexTypeDesc LIKE 'CLUSTERED%'
                            THEN @TabSpaces + 'CREATE ' + @IndexTypeDesc + ' INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + 'WITH (COMPRESSION_DELAY = ' + CONVERT(VARCHAR, @CompressionDelay) + ', DATA_COMPRESSION = ' + @DataCompressionType + ') ON ' + @FileGroupName + ';'
                        WHEN @IsFiltered = 1
                            THEN @TabSpaces + 'CREATE ' + @IndexTypeDesc + ' INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + + CHAR(13) + @TabSpaces + 'WHERE ' + @FilterDefinition + CHAR(13) + @TabSpaces + 'WITH (COMPRESSION_DELAY = ' + CONVERT(VARCHAR, @CompressionDelay) + ', DATA_COMPRESSION = ' + @DataCompressionType + ') ON ' + @FileGroupName + ';'
                        ELSE @TabSpaces + 'CREATE ' + @IndexTypeDesc + ' INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + CHAR(13) + @TabSpaces + 'WITH (COMPRESSION_DELAY = ' + CONVERT(VARCHAR, @CompressionDelay) + ', DATA_COMPRESSION = ' + @DataCompressionType + ') ON ' + @FileGroupName + ';'
                        END
            WHEN @IndexTypeDesc = 'MEMORY_OPTIMIZED'
                THEN CASE 
                        WHEN @IsPrimaryKey = 1
                            THEN @TabSpaces + 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ADD CONSTRAINT ' + QUOTENAME(@IndexName) + ' ' + ' PRIMARY KEY NONCLUSTERED' + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ');'
                        WHEN @IsPrimaryKey = 0
                            THEN @TabSpaces + 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ADD INDEX ' + QUOTENAME(@IndexName) + ' ' + @IsUnique + ' NONCLUSTERED' + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ');'
                        END
                    -- SQL Server 2014 and later (sys.hash_indexes doesn't exist prior to this)
            WHEN @IndexTypeDesc = 'NONCLUSTERED HASH'
                THEN CASE 
                        WHEN @IsPrimaryKey = 1
                            THEN (
                                    SELECT @TabSpaces + 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ADD CONSTRAINT ' + QUOTENAME(@IndexName) + ' PRIMARY KEY NONCLUSTERED HASH' + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + 'WITH ( BUCKET_COUNT = ' + CONVERT(VARCHAR, bucket_count) + ');'
                                    FROM sys.hash_indexes
                                    WHERE object_id = OBJECT_ID(@SchemaName + '.' + @TableName)
                                        AND name = @IndexName
                                    )
                        WHEN @IsPrimaryKey = 0
                            THEN (
                                    SELECT @TabSpaces + 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ADD INDEX ' + QUOTENAME(@IndexName) + ' ' + @IndexTypeDesc + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + 'WITH ( BUCKET_COUNT = ' + CONVERT(VARCHAR, bucket_count) + ');'
                                    FROM sys.hash_indexes
                                    WHERE object_id = OBJECT_ID(@SchemaName + '.' + @TableName)
                                        AND name = @IndexName
                                    )
                        END
                    -- SQL Server 2014 and later (xml_index_type_description doesn't exist prior to this)
            WHEN @IndexTypeDesc = 'XML'
                THEN CASE 
                        WHEN EXISTS (
                                SELECT name
                                FROM sys.xml_indexes
                                WHERE xml_index_type_description = 'PRIMARY_XML'
                                    AND name = @IndexName
                                    AND object_id = OBJECT_ID(@SchemaName + '.' + @TableName)
                                )
                            THEN @TabSpaces + 'CREATE PRIMARY XML INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + CHAR(13) + @TabSpaces + 'WITH (' + @IndexOptions + ');'
                        WHEN EXISTS (
                                SELECT object_id
                                    ,index_id
                                    ,name
                                FROM sys.xml_indexes
                                WHERE xml_index_type_description <> 'PRIMARY_XML'
                                    AND name = @IndexName
                                    AND object_id = OBJECT_ID(@SchemaName + '.' + @TableName)
                                )
                            THEN (
                                    SELECT @TabSpaces + 'CREATE XML INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ')' + CHAR(13) + @TabSpaces + 'USING XML INDEX ' + QUOTENAME(P.name) + ' FOR ' + I.secondary_type_desc COLLATE LATIN1_GENERAL_CS_AS + ' WITH (' + @IndexOptions + ');'
                                    FROM sys.xml_indexes I
                                    INNER JOIN (
                                        SELECT object_id
                                            ,index_id
                                            ,name
                                        FROM sys.xml_indexes
                                        WHERE xml_index_type_description = 'PRIMARY_XML'
                                        ) P
                                        ON P.object_id = I.object_id
                                            AND P.index_id = I.using_xml_index_id
                                            AND I.name = @IndexName
                                    )
                        END
            WHEN @IndexTypeDesc = 'spatial'
                THEN (
                        SELECT @TabSpaces + 'CREATE SPATIAL INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ')' + 'USING ' + tessellation_scheme + CHAR(13) + @TabSpaces + 'WITH (BOUNDING_BOX =(' + CONVERT(VARCHAR, bounding_box_xmin) + ', ' + CONVERT(VARCHAR, bounding_box_ymin) + ', ' + CONVERT(VARCHAR, bounding_box_xmax) + ', ' + CONVERT(VARCHAR, bounding_box_ymax) + '), ' + 'GRIDS =(LEVEL1 = ' + level_1_grid_desc + ',LEVEL_2 = ' + level_2_grid_desc + ',LEVEL3 = ' + level_3_grid_desc + ',LEVEL4 = ' + level_4_grid_desc + '), ' + 'CELLS_PER_OBJECT = ' + CONVERT(VARCHAR, cells_per_object) + ', ' + @IndexOptions + ') ON ' + @FileGroupName + ';'
                        FROM sys.spatial_index_tessellations
                        WHERE object_id = OBJECT_ID(@SchemaName + '.' + @TableName)
                            AND index_id = (
                                SELECT index_id
                                FROM sys.indexes
                                WHERE object_id = OBJECT_ID(@SchemaName + '.' + @TableName)
                                    AND name = @IndexName
                                )
                        )
            WHEN @IsFiltered = 1
                THEN @TabSpaces + 'CREATE ' + @IsUnique + @IndexTypeDesc + ' INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + CASE 
                        WHEN LEN(@IncludedColumns) > 0
                            THEN CHAR(13) + @TabSpaces + 'INCLUDE (' + @IncludedColumns + ')'
                        ELSE ''
                        END + CHAR(13) + @TabSpaces + 'WHERE ' + @FilterDefinition + CHAR(13) + @TabSpaces + 'WITH (' + @IndexOptions + ') ON ' + @FileGroupName + ';'
            WHEN @IsPrimaryKey = 1
                THEN @TabSpaces + 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ADD CONSTRAINT ' + QUOTENAME(@IndexName) + ' PRIMARY KEY ' + @IndexTypeDesc + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + CHAR(13) + @TabSpaces + 'WITH (' + @IndexOptions + ') ON ' + @FileGroupName + ';'
            WHEN @IsUniqueConstraint = 1
                THEN @TabSpaces + 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ADD CONSTRAINT ' + QUOTENAME(@IndexName) + ' ' + @IsUnique + @IndexTypeDesc + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + CHAR(13) + @TabSpaces + 'WITH (' + @IndexOptions + ');'
            ELSE @TabSpaces + 'CREATE ' + @IsUnique + @IndexTypeDesc + ' INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) + @TabSpaces + '(' + CHAR(13) + @TabSpaces + @TabSpaces + @IndexColumns + CHAR(13) + @TabSpaces + ') ' + CASE 
                    WHEN LEN(@IncludedColumns) > 0
                        THEN CHAR(13) + @TabSpaces + 'INCLUDE (' + @IncludedColumns + ')'
                    ELSE ''
                    END + CHAR(13) + @TabSpaces + 'WITH (' + @IndexOptions + ') ON ' + @FileGroupName + ';'
            END
    IF (@IsDisabled = 1)
        SET @TsqlScriptDisableIndex = @TabSpaces + 'PRINT CONVERT(VARCHAR, GETDATE(), 120) + '': disabling index: ' + QUOTENAME(@IndexName) + '''' + CHAR(13) + @TabSpaces + 'ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' DISABLE;'
    ---------------------------------------------------
    -- Output the TSQL Script to the Messages Window
    -- Comment
    IF (@TsqlScriptCreateIndex LIKE '%ONLINE = ON%')
        PRINT '-- NOTE: If a COLUMNSTORE index already exists, ONLINE = ON will cause an error. You will need to drop the COLUMNSTORE, add the ONLINE = ON, then add the COLUMNSTORE.'
    -- Start IF block
    PRINT @TsqlScriptBeforeIndex
    -- Create/Alter Index/Constraint
    PRINT @TsqlScriptCreateIndex
    --PRINT @TabSpaces + 'GO;' -- can't call GO inside an IF block
    -- Disable Index
    IF (@TsqlScriptDisableIndex <> '')
        --PRINT @TsqlScriptDisableIndex + @TabSpaces + 'GO;' -- can't call GO inside an IF block
        PRINT @TsqlScriptDisableIndex
    -- End IF block
    PRINT @TsqlScriptAfterIndex + CHAR(10) + CHAR(10)
    ---------------------------------------------------
    FETCH NEXT
    FROM CursorIndex
    INTO @SchemaName
        ,@TableName
        ,@IndexName
        ,@IsUnique
        ,@IndexTypeDesc
        ,@IndexOptions
        ,@IsDisabled
        ,@FileGroupName
        ,@IsFiltered
        ,@FilterDefinition
        ,@IsPrimaryKey
        ,@IsUniqueConstraint
        ,@CompressionDelay
END
CLOSE CursorIndex
DEALLOCATE CursorIndex
