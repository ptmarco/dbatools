USE [dba_database]
GO

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID('dbo.sp_ReadAllErrorLogs') IS  NULL
    EXEC ('CREATE PROCEDURE dbo.sp_ReadAllErrorLogs AS RETURN 138;');
GO

ALTER PROCEDURE sp_ReadAllErrorLogs
   @From DATETIME = NULL,
   @To DATETIME = NULL,
   @Help TINYINT = 0,
   @Search1 NVARCHAR(255) = 'error',
   @Search2 NVARCHAR(255) = NULL,
   @DaysBack TINYINT = 0,
   @ExtraRows INT = 1  --Shows this many log records before and after the line matching "SearchString"
                       --If there are multiple rows with the exact LogDate, "adjacent" becomes inaccurate ..still useful
--WITH ENCRYPTION
AS
BEGIN

 /*=====================================================================================================
	Author:        	Marco Assis
	Create date:   	17/12/21
	Description:   	sp_ReadAllErrorLogs
					Search Multiple Log Files at one
	Tested on:		SQL 2012, 2016, 2019
---------------------------------------------------------------------------------------------------
	Change History
	Date   		Version		Author       		Description
	19/4/2018	v1.0		Jana Sattainathan	[Twitter: @SQLJana] [Blog: sqljana.wordpress.com]
	https://sqljana.wordpress.com/2018/04/20/sql-server-a-more-flexible-xp_readerrorlog-that-reads-all-error-logs-including-archives/
	18/01/22	v1.1		Marco Assis			Add @help, soften parameters names, add @daysback
---------------------------------------------------------------------------------------------------
	Example(s)
        -- Search "error" on he tlatest 10 log files
		EXEC sp_ReadAllErrorLogs
            @DaysBack = 10,
            @Search1  = 'error';
        
        -- Search "error" AND "18732" on all log files between dates 1/10-31/10 and get extra 2 previous and after rows
		EXEC sp_ReadAllErrorLogs
            @From      = '2021-10-01',
            @To        = '2021-10-31',
            @Search1   = 'error',
            @Search2   = '18732',
            @ExtraRows = 2;
---------------------------------------------------------------------------------------------------
	To Do:
		1. Add @source to allow for SQL Agent Log file search
---------------------------------------------------------------------------------------------------
	You may alter this code for your own *non-commercial* purposes. You may
	republish altered code as long as you include this copyright and give due
	credit, but you must obtain prior permission before blogging this code.
	   
	THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
	ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
	TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
	PARTICULAR PURPOSE.
=====================================================================================================*/

    DECLARE @ArchiveNumber INT;
 
	IF @Help = 1
	BEGIN
		PRINT 'Parameters:
@DaysBack = # of days to look back <opcional>
@From = ''20200301'' <opcional>
@To = ''20200301'' <opcional>
@Search1 = ''1st text to search''
@Search2 = ''2nd text to search'' <opcional>
@ExtraRows = 0 # of rows before & after to also show <opcional>
';
	RETURN;
	END

	IF @DaysBack > 0
		BEGIN
			SET @To = getdate()
			SET @From = DATEADD(dd,@DaysBack * -1, @to)
		END

    IF object_id('TEMPDB.DBO.##TempLogList1') IS NOT NULL
        DROP TABLE ##TempLogList1;
    IF object_id('TEMPDB.DBO.#TempLogList2') IS NOT NULL
        DROP TABLE #TempLogList2;
    IF object_id('TEMPDB.DBO.#TempLog1') IS NOT NULL
        DROP TABLE #TempLog1;
    IF object_id('TEMPDB.DBO.#TempLog1') IS NOT NULL
        DROP TABLE #TempLog2;
 
    CREATE TABLE ##TempLogList1(
        ArchiveNumber INT NOT NULL,
        --LogFromDate DATE NOT NULL,
        LogToDate DATE NOT NULL,
        LogSizeBytes BIGINT NOT NULL);
 
    CREATE TABLE #TempLog1(
        LogDate     DATETIME,
        ProcessInfo VARCHAR(64),
        LogText     VARCHAR(MAX));
 
    CREATE TABLE #TempLog2(
        LogDate     DATETIME,
        ProcessInfo VARCHAR(64),
        LogText     VARCHAR(MAX));
 
    --Get the list of all logs available (current and archived)
    INSERT INTO ##TempLogList1
    EXEC sys.sp_enumerrorlogs;
 
    --LogFromDate is populated here
    SELECT
        ArchiveNumber,
        COALESCE((LEAD(LogToDate) OVER (ORDER BY ArchiveNumber)), '20000101') LogFromDate,
        LogToDate,
        LogSizeBytes
    INTO
        #TempLogList2
    FROM
        ##TempLogList1
    ORDER BY
        LogFromDate, LogToDate;
 
    --Remove archive logs whose date criteria does not fit the parameters
    --....and No, it is not a mistake that the comparison has the two dates interchanged! Just think for a few minutes
    DELETE FROM #TempLogList2
    WHERE LogToDate < COALESCE(@From, '20000101') OR LogFromDate > COALESCE(@To, '99991231');
 
    --Loop through and get the list
    WHILE 1=1
    BEGIN
        SELECT @ArchiveNumber = MIN(ArchiveNumber)
        FROM #TempLogList2;
 
        IF @ArchiveNumber  IS NULL
          BREAK;
 
        --Insert the error log data into our temp table
        --Read the errorlog data
        /*
        --https://www.mssqltips.com/sqlservertip/1476/reading-the-sql-server-log-files-using-tsql/
        This procedure takes four parameters:
 
        Value of error log file you want to read: 0 = current, 1 = Archive #1, 2 = Archive #2, etc...
        Log file type: 1 or NULL = error log, 2 = SQL Agent log
        Search string 1: String one you want to search for
        Search string 2: String two you want to search for to further refine the results
        */
        INSERT INTO #TempLog1
        EXEC xp_readerrorlog @ArchiveNumber, 1, @Search1, @Search2, @From, @To, 'ASC'
 
        IF (@ExtraRows > 0)
            --This is purely to get the adjacent records
            INSERT INTO #TempLog2
            EXEC xp_readerrorlog @ArchiveNumber, 1, NULL, NULL, @From, @To, 'ASC';
 
        --Remove just processed archive number from the list
        DELETE FROM #TempLogList2
        WHERE ArchiveNumber = @ArchiveNumber;
    END;
 
    IF (@ExtraRows <= 0)
        SELECT * FROM #TempLog1
        ORDER BY LogDate ASC;
    ELSE
    BEGIN
 
        --To give the search text some context, we include the log records adjacent to the ones
        --  that matched the search criteria. For example search string "error" would match
        --  "Error: 1101, Severity: 17, State: 12.". However, to get the context, we need
        --  the adjacent rows that show the specific error which is on another adjacent row:
        --      Could not allocate a new page for database 'MyDb' because of insufficient disk space in filegroup 'PRIMARY'.
        --      Create the necessary space by dropping objects in the filegroup, adding additional files to the filegroup, or setting autogrowth on for existing files in the filegroup.
        WITH t1
        AS
        (
            SELECT *
            FROM #TempLog1
        ),
        t2
        AS
        (
            --Select the previous and next x'TH dates as part of the current row
            SELECT *,
                LAG(LogDate, @ExtraRows) OVER (ORDER BY LogDate) AS LagLogDate,
                LEAD(LogDate, @ExtraRows) OVER (ORDER BY LogDate) AS LeadLogDate
            FROM #TempLog2
        )
        SELECT DISTINCT t2.LogDate, t2.ProcessInfo, t2.LogText
        FROM t2
        INNER JOIN t1
            ON t1.LogDate BETWEEN t2.LagLogDate
                AND t2.LeadLogDate
        ORDER BY
            t2.LogDate ASC;
    END;
 
    DROP TABLE #TempLog1;
    DROP TABLE #TempLog2;
    DROP TABLE ##TempLogList1;
    DROP TABLE #TempLogList2;
END;

PRINT 'sp_ReadAllErrorLogs Created'
GO

RETURN
-- Testing
EXEC sp_ReadAllErrorLogs
    @search1 = 'severity',
    @extrarows = 2,
    @from = '2024-10-01'
    ;