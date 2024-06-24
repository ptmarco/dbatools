SET NOEXEC OFF
GO

IF OBJECT_ID('sp_CloneUser_AzureSQLDB', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_CloneUser_AzureSQLDB;
GO

IF SERVERPROPERTY('EngineEdition') != 5
    SET NOEXEC ON;

CREATE PROCEDURE dbo.sp_CloneUser_AzureSQLDB 
    @ReferenceUser SYSNAME = NULL
    ,@User SYSNAME = NULL
    ,@PrintOnly BIT = 1
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Author:         Marco Assis
Create date:    05/2024
Description:    sp_CloneUser_AzureSQLDB
                Clone a Azure SQL DB User
Tested On:      Azure SQL Database

Notes:          
    What it does:
        1. Creates USer if not exist
        2. Adds database user to the same roles as reference user
        3. Only for EntraId Users

    What doesen't do (yet):
        1. Copy specific database object permissions, only roles
        2. Assumes that Database User, if already present, was created correctly

Parameters:
    @ReferenceUser SYSNAME                     = EXACT (as is in SQL Server) Mandatory Login to clone
    @User          SYSNAME                     = DOMAIN\Login (EntraId for Azure PaaS format login@domain) of User to assign permissiones
    @PrintOnly     BIT                         = Optional  1 Generate Scripts, 0 Executes Scripts. Default 0

Examples:
    EXEC dbo.sp_CloneUser 
        @ReferenceUser = 'PTI00133@unicredc.unicre.pt'
        ,@User         = 'PTI00459@unicredc.unicre.pt'
        ,@PrintOnly     = 1
====================================================================================================================
Change History
Date        Author          Description	
06/24       Marco Assis     Adaptation of sp_CloneUser for Azure SQL Database
====================================================================================================================
You may alter this code for your own *non-commercial* purposes. You may
republish altered code as long as you include this copyright and give due
credit, but you must obtain prior permission before blogging this code.
   
THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
PARTICULAR PURPOSE.
====================================================================================================================
*/
/* Environment */
SET QUOTED_IDENTIFIER ON
SET NOEXEC OFF

IF @PrintOnly = 1
    SET NOCOUNT ON
ELSE
    SET NOCOUNT OFF

/* Variables */
DECLARE @sql NVARCHAR(MAX)
    ,@db SYSNAME
    ,@from NVARCHAR(32) = N' FROM EXTERNAL PROVIDER'
    ,@role SYSNAME

IF OBJECT_ID('tempdb..##validation', 'U') IS NOT NULL
    DROP TABLE ##validation

-- Force All Caps
SELECT  @ReferenceUser = UPPER(@ReferenceUser)
        ,@User = UPPER(@User)

-- Validate reference User
IF NOT EXISTS (
        SELECT 1
        FROM sys.database_principals
        WHERE name = @ReferenceUser
        )
BEGIN
    PRINT N'Reference USer ' + @ReferenceUser + N' doesn''t exist';
    SET NOEXEC ON;
END

-- Database User
IF NOT EXISTS (
        SELECT 1
        FROM sys.database_principals
        WHERE name = @User
        )
BEGIN TRY
    SELECT @sql = N'-- Create New Database User' + CHAR(13) + N'CREATE USER ' + quotename(@User) + @from + N';' + CHAR(13)

    IF @PrintOnly = 0
        EXEC (@sql)
    ELSE
        PRINT @sql
END TRY

BEGIN CATCH
    PRINT N'Failed to create Database User';
    THROW;
    SET NOEXEC ON;
END CATCH
--PRINT N'Success';

-- Role(s)
SELECT  dp.name [role]
INTO    #roles
FROM    sys.database_role_members rm
JOIN    sys.database_principals dp
            ON dp.principal_id = rm.role_principal_id
WHERE   rm.member_principal_id IN (SELECT principal_id FROM sys.database_principals WHERE name = @ReferenceUser)

DECLARE roles CURSOR FAST_FORWARD
    FOR SELECT ROLE FROM #roles

OPEN roles
FETCH NEXT FROM roles INTO @role
WHILE (@@FETCH_STATUS = 0)
BEGIN
    SELECT @sql = N'-- Add user ' + quotename(@User) + N' to role: ' + @role + CHAR(13) 
                + N'ALTER ROLE ' + @role + N' ADD MEMBER ' + quotename(@User) + N';' + CHAR(13)
    BEGIN TRY
        IF @PrintOnly = 0
            EXEC (@sql)
        ELSE
            PRINT @sql
    END TRY
    BEGIN CATCH
        PRINT N'Failed to add user ' + quotename(@User) + N' to role' + @role + CHAR(13);
        THROW;
        SET NOEXEC ON;
    END CATCH
    FETCH NEXT FROM roles INTO @role
END

CLOSE roles
DEALLOCATE roles

-- Verify results
IF @PrintOnly = 0
BEGIN
    SELECT N'Permissions after execution' [ ]
    CREATE TABLE #validation (
    [Role] SYSNAME
    ,[User] SYSNAME
    ,[Type] NVARCHAR(14)
    )

    -- Get New Login Permissions
    INSERT INTO #validation
    SELECT 
        u1.name, 
        u2.name,
        'New User'
    FROM sys.database_role_members r
    JOIN sys.database_principals u1
        ON u1.principal_id = r.role_principal_id
    JOIN sys.database_principals u2
        ON u2.principal_id = r.member_principal_id
    CROSS APPLY sys.database_principals p
    WHERE   u2.name = @User
        AND p.name = @User
    
    -- Get Reference Login Permissions
    INSERT INTO #validation
    SELECT 
        u1.name, 
        u2.name,
        'Reference User'
    FROM sys.database_role_members r
    JOIN sys.database_principals u1
        ON u1.principal_id = r.role_principal_id
    JOIN sys.database_principals u2
        ON u2.principal_id = r.member_principal_id
    CROSS APPLY sys.database_principals p
    WHERE   u2.name = @ReferenceUser
            AND p.name = @ReferenceUser

    SELECT [Role]
            ,COALESCE([User], N'-') [User]
            ,Type
    FROM #validation
    ORDER BY 1,2,3

    DROP TABLE #validation
END
GO

SET NOEXEC OFF

/* Testes **********************************************************

if exists (select 1 from sys.database_principals where name like 'PTI00459@unicredc.unicre.pt')
    drop user [PTI00459@unicredc.unicre.pt]

EXEC dbo.sp_CloneUser_AzureSQLDB 
        @ReferenceUser = 'PTI00133@unicredc.unicre.pt'
        ,@User         = 'PTI00459@unicredc.unicre.pt'
        ,@PrintOnly    = 0

DROP PROCEDURE dbo.sp_CloneUser_AzureSQLDB

*/
