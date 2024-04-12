IF OBJECT_ID('dbo.sp_UpdateSqlServerBuilds','P') IS NULL
    EXEC('CREATE PROCEDURE dbo.sp_UpdateSqlServerBuilds AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_UpdateSqlServerBuilds AS
BEGIN
/*
====================================================================================================================
Author:			Marco Assis
Create date:	04/2024
Description:	Build SQL Releases & Builds tables from updated info @ https://sqlserverbuilds.blogspot.com
Notes:			

Parameters:

Examples:

====================================================================================================================
Change History
Date   		Author       	Description	

====================================================================================================================
*/

-- Enable "Ole Automation Procedures" configuration enabled on SQL Server.
DECLARE @undo BIT = 0;
IF (SELECT value FROM sys.configurations WHERE name = 'Ole Automation Procedures') != 1
BEGIN
    EXEC sp_configure N'show advanced options', 1;
    RECONFIGURE;
    EXEC sp_configure N'Ole Automation Procedures', 1;
    RECONFIGURE;
    SELECT @undo = 1;
END

-- Clean Up
IF OBJECT_ID('dbo.SqlServerBuild','U') IS NULL
    DROP TABLE SqlServerBuilds;
IF OBJECT_ID('dbo.SqlServerRelease','U') IS NULL
    DROP TABLE SqlServerBuilds;

-- SqlServerBuilds
SET NOCOUNT ON;
DECLARE @Url NVARCHAR(2048) = N'https://docs.google.com/spreadsheets/d/16Ymdz80xlCzb6CwRFVokwo0onkofVYFoSkc7mYe6pgw/gviz/tq?gid=1648964847&tqx=out:json';
DECLARE @obj INT, @hr INT, @status INT;
DECLARE @Response TABLE(ResponseText NVARCHAR(MAX) NULL);

EXEC @hr = sp_OACreate 'MSXML2.XMLHTTP', @obj OUT;
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OACreate error', 0;
IF @obj IS NULL              THROW 50000, 'sp_OACreate error', 1;

EXEC @hr = sp_OAMethod @obj, 'open', NULL, 'GET', @Url, False;
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OAMethod open() error', 0;

EXEC @hr = sp_OAMethod @obj, 'send';
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OAMethod send() error', 0;

EXEC @hr = sp_OAGetProperty @obj, 'status', @status OUT;
IF ISNULL(@hr, 0)     <>   0 THROW 50000, 'sp_OAGetProperty status error', 0;
IF ISNULL(@status, 0) <> 200 THROW 50000, 'sp_OAGetProperty status error', 1;

INSERT INTO @Response(ResponseText)
EXEC @hr = sp_OAGetProperty @obj, 'responseText';
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OAGetProperty responseText error', 0;

EXEC @hr = sp_OADestroy @obj;
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OADestroy error', 0;

DECLARE @JsonP NVARCHAR(MAX) = (SELECT ResponseText FROM @Response);
IF ISNULL(@JsonP, N'') = N'' THROW 50000, 'Empty response', 0;

-- Now we have JSON-P and we need to clear the text at the beginning '/*O_o*/<NewLine>google.visualization.Query.setResponse(' and end ');'.
DECLARE @Json NVARCHAR(MAX) = SUBSTRING(@JsonP, 48, DATALENGTH(@JsonP) - 48 - 1);

-- Transform JSON into a table-like dataset
SELECT *
INTO dbo.SqlServerRelease
FROM OPENJSON(@Json, '$.table.rows')
WITH (
  Release                    NVARCHAR(MAX) '$.c[0].v',
  FullName                   NVARCHAR(MAX) '$.c[1].v',
  Version                    NVARCHAR(MAX) '$.c[2].v',
  DatabaseCompatibilityLevel NVARCHAR(MAX) '$.c[3].f',
  InternalDatabaseVersion    NVARCHAR(MAX) '$.c[4].f',
  ReleaseDate                DATE          '$.c[5].f',
  MainstreamSupportEnds      DATE          '$.c[6].f',
  ExtendedSupportEnds        DATE          '$.c[7].f',
  IsLatest                   BIT           '$.c[8].v',
  IsObsolete                 BIT           '$.c[9].v',
  IsBeta                     BIT           '$.c[10].v'
);

SELECT *
FROM dbo.SqlServerRelease;

-- SqlServerBuilds
SET NOCOUNT ON;
SELECT @Url = N'https://docs.google.com/spreadsheets/d/16Ymdz80xlCzb6CwRFVokwo0onkofVYFoSkc7mYe6pgw/gviz/tq?tqx=out:json';
--DECLARE @Url NVARCHAR(2048) = N'https://docs.google.com/spreadsheets/d/16Ymdz80xlCzb6CwRFVokwo0onkofVYFoSkc7mYe6pgw/gviz/tq?tqx=out:json';
--DECLARE @obj INT, @hr INT, @status INT;
--DECLARE @Response TABLE(ResponseText NVARCHAR(MAX) NULL);

EXEC @hr = sp_OACreate 'MSXML2.XMLHTTP', @obj OUT;
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OACreate error', 0;
IF @obj IS NULL              THROW 50000, 'sp_OACreate error', 1;

EXEC @hr = sp_OAMethod @obj, 'open', NULL, 'GET', @Url, False;
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OAMethod open() error', 0;

EXEC @hr = sp_OAMethod @obj, 'send';
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OAMethod send() error', 0;

EXEC @hr = sp_OAGetProperty @obj, 'status', @status OUT;
IF ISNULL(@hr, 0)     <>   0 THROW 50000, 'sp_OAGetProperty status error', 0;
IF ISNULL(@status, 0) <> 200 THROW 50000, 'sp_OAGetProperty status error', 1;

INSERT INTO @Response(ResponseText)
EXEC @hr = sp_OAGetProperty @obj, 'responseText';
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OAGetProperty responseText error', 0;

EXEC @hr = sp_OADestroy @obj;
IF ISNULL(@hr, 0) <> 0       THROW 50000, 'sp_OADestroy error', 0;

--DECLARE @JsonP NVARCHAR(MAX) = (SELECT ResponseText FROM @Response);
SELECT @JsonP = (SELECT ResponseText FROM @Response);
IF ISNULL(@JsonP, N'') = N'' THROW 50000, 'Empty response', 0;

-- Now we have JSON-P and we need to clear the text at the beginning '/*O_o*/<NewLine>google.visualization.Query.setResponse(' and end ');'.
--DECLARE @Json NVARCHAR(MAX) = SUBSTRING(@JsonP, 48, DATALENGTH(@JsonP) - 48 - 1);
SELECT @Json = SUBSTRING(@JsonP, 48, DATALENGTH(@JsonP) - 48 - 1);

-- Transform JSON into a table-like dataset
SELECT *
INTO dbo.SqlServerBuilds
FROM OPENJSON(@Json, '$.table.rows')
WITH (
  SQLServer   NVARCHAR(MAX) '$.c[0].v',
  Version     NVARCHAR(MAX) '$.c[1].v',
  Build       NVARCHAR(MAX) '$.c[2].v',
  FileVersion NVARCHAR(MAX) '$.c[3].v',
  Description NVARCHAR(MAX) '$.c[4].v',
  Link        NVARCHAR(MAX) '$.c[5].v',
  ReleaseDate DATE          '$.c[6].f',
  SP          BIT           '$.c[7].v',
  CU          BIT           '$.c[8].v',
  HF          BIT           '$.c[9].v',
  RTM         BIT           '$.c[10].v',
  CTP         BIT           '$.c[11].v',
  New         BIT           '$.c[12].v',
  Withdrawn   BIT           '$.c[13].v'
);

SELECT *
FROM dbo.SqlServerBuilds;

-- Disable "Ole Automation Procedures" configuration enabled on SQL Server.
IF @undo = 1
BEGIN    
    EXEC sp_configure N'show advanced options', 1;
    RECONFIGURE;
    EXEC sp_configure N'Ole Automation Procedures', 1;
    RECONFIGURE;
END

END
GO