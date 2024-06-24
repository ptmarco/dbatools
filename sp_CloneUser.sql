SET NOEXEC OFF
GO

IF SERVERPROPERTY('EngineEdition') = 5
    SET NOEXEC ON;

IF OBJECT_ID('sp_CloneUser','P') IS NOT NULL
	DROP PROCEDURE dbo.sp_CloneUSer;
GO

CREATE PROCEDURE dbo.sp_CloneUser
    @ReferenceLogin     SYSNAME = NULL
    ,@Login             SYSNAME = NULL
    ,@Database          SYSNAME = NULL
    ,@PrintOnly         BIT     = 1
--WITH ENCRYPTION
AS

/* 
====================================================================================================================
Author:         Marco Assis
Create date:    05/2024
Description:    sp_CloneUser
                Clone a SQL Server Login for 1 or All Databases
Tested On:      SQL Server 2012+ (OnPrem & Azure IaaS)

Notes:          
    What it does:
        1. Creates Login if not exist
        2. Creates User in each database if not exist (includes msdb & master)
        3. Adds database user to the same roles as reference user
        4. Only for DOMAIN users

    What doesen't do (yet):
        1. Copy specific database object permissions, only roles
        2. Copy Login server roles
        3. Not compatible with Azure SQL Databases, only "traditional" SQL Server + Managed Instance
        4. Only applies on Databases with status Online & READ_WRITE/MULTI_USER
        5. Create Database Contained Users
        6. Assumes that Database User, if already present, was created correctly and not a orphan user

Parameters:
    @ReferenceLogin SYSNAME                     = EXACT (as is in SQL Server) Mandatory Login to clone
    @Login          SYSNAME                     = DOMAIN\Login of User to assign permissiones
    @Database       SYSNAME                     = Optional  if null = ALL except system Databases & dba_database
    @PrintOnly      BIT                         = Optional  1 Generate Scripts, 0 Executes Scripts. Default 0

Examples:
    EXEC dbo.sp_CloneUser 
        @ReferenceLogin = 'UNICREDC\PTI00133'
        ,@Login         = 'UNICREDC\PTI00459'
        ,@PrintOnly     = 1
        ,@Database     = 'msdb'
====================================================================================================================
Change History
Date        Author          Description	
05/24       Marco Assis     Initial Build
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
DECLARE @sql    NVARCHAR(MAX)
        ,@db    SYSNAME
        ,@from  NVARCHAR(32) = N' FROM WINDOWS'
        ,@role  SYSNAME

IF OBJECT_ID('tempdb..##validation','U') IS NOT NULL
    DROP TABLE ##validation
IF OBJECT_ID('tempdb..##targetDBs','U') IS NOT NULL
    DROP TABLE ##targetDBs
IF OBJECT_ID('tempdb..##roles','U') IS NOT NULL
    DROP TABLE ##roles

CREATE TABLE #roles (role SYSNAME)

-- Force All Caps
SELECT  @ReferenceLogin = UPPER(@ReferenceLogin)
        ,@Login         = UPPER(@Login)

-- Check for Azure SQL Database
IF SERVERPROPERTY('EngineEdition') = 5
BEGIN
    PRINT 'Azure SQL Database not supported at this point'
    SET NOEXEC ON
END

-- Validate reference login
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @ReferenceLogin)
    BEGIN
        PRINT N'Reference Login ' + @ReferenceLogin + N' Windows login not found';
        SET NOEXEC ON;
    END

-- Narrow Target Databases scope
CREATE TABLE ##targetDBs (id INT)

IF @Database IS NULL -- Null means no filter so its all databases
BEGIN
    SELECT @sql = N'USE [?]' + CHAR(13)
                  +N'INSERT INTO ##targetDBs (id)' + CHAR(13)
                  +N'SELECT db_id() FROM sys.database_principals WHERE name = ''' + @ReferenceLogin + N''''
    EXEC sp_MSforeachdb @sql
END
ELSE
    INSERT INTO ##targetDBs (id)
        SELECT DB_ID(@Database);

-- Server Login
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @Login)
    BEGIN TRY
        SELECT @sql = N'-- Create New Login' + CHAR(13) 
                    + N'USE master' + CHAR(13)
                    + N'CREATE LOGIN ' + quotename(@Login) + @from + N';' + CHAR(13)
        IF @PrintOnly = 0 
            EXEC(@sql)
        ELSE
            PRINT @sql
    END TRY
    BEGIN CATCH
        PRINT N'Failed to create Login';
        THROW;
        SET NOEXEC ON;
    END CATCH
    --PRINT N'Success';

-- Database(s)
DECLARE dbs CURSOR FAST_FORWARD FOR
    SELECT  name 
    FROM    sys.databases
    WHERE   user_access     = 0 -- MULTI_USER
        AND is_read_only    = 0 -- READ_WRITE
        AND state           = 0 -- ONLINE
        AND name            NOT IN ('tempdb','model')
        AND name            NOT LIKE 'dba%'
        AND name            LIKE COALESCE(@database,N'%')
        AND database_id     IN (SELECT id FROM ##targetDBs)

OPEN dbs
FETCH NEXT FROM dbs INTO @db
WHILE (@@FETCH_STATUS = 0)
BEGIN
    -- Database User
    IF NOT EXISTS ( SELECT 1 FROM sys.database_principals WHERE name = @Login)
    BEGIN TRY
        SELECT @sql = N'-- Create User at database: ' + quotename(@db) + CHAR(13) 
                    + N'USE ' + quotename(@db) + CHAR(13)
                    + N'IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''' + @Login + N''')' + CHAR(13)
                    + N'    CREATE USER ' + quotename(@Login)
                    + N' FOR LOGIN ' + quotename(@Login) + N';' + CHAR(13)
        IF @PrintOnly = 0 
            EXEC(@sql)
        ELSE
            PRINT @sql
    END TRY
    BEGIN CATCH
        PRINT N'Failed to create Database User at database: ' + quotename(@db) + CHAR(13);
        THROW;
        SET NOEXEC ON; PRINT N'Huston .... we have a problem'
    END CATCH
    --PRINT N'Success';

    -- Role(s)
    TRUNCATE TABLE #roles
    SELECT @sql = N'USE ' + quotename(@db) + CHAR(13)
                + N'SELECT  dp.name 
                    FROM    sys.database_role_members rm
                    JOIN    sys.database_principals dp
                        ON dp.principal_id = rm.role_principal_id
                WHERE   rm.member_principal_id IN (SELECT principal_id FROM sys.database_principals WHERE name = '''
                + @ReferenceLogin + N''')'
    INSERT INTO #roles (role)
        EXEC(@sql);

    DECLARE roles CURSOR FAST_FORWARD FOR
        SELECT role FROM #roles

    OPEN roles
    FETCH NEXT FROM roles INTO @role
    WHILE (@@FETCH_STATUS=0)
    BEGIN
        SELECT @sql = N'-- Add user ' + quotename(@Login) + N' to role: ' + @role + N' at database: ' + quotename(@db) + CHAR(13)
                    + N'USE ' + @db + CHAR(13)
                    + N'ALTER ROLE ' + @role 
                    + N' ADD MEMBER ' + quotename(@Login) + N';' + CHAR(13) 
        BEGIN TRY
            IF @PrintOnly = 0 
                EXEC(@sql)
            ELSE
                PRINT @sql
        END TRY
        BEGIN CATCH
            PRINT N'Failed to add user ' + quotename(@Login) + N' to role' + @role + N' at database: ' + quotename(@db) + CHAR(13);
            THROW;
            SET NOEXEC ON; PRINT N'Huston .... we have a problem'
        END CATCH
        FETCH NEXT FROM roles INTO @role
    END
    CLOSE roles
    DEALLOCATE roles

    FETCH NEXT FROM dbs INTO @db
END
CLOSE dbs
DEALLOCATE dbs

-- Verify results
IF @PrintOnly = 0
BEGIN
    SELECT N'Permissions after execution' [ ]
    CREATE TABLE ##validation (
        [Login]             SYSNAME NOT NULL
        ,[Database]         SYSNAME NOT NULL
        ,[Role]             SYSNAME NOT NULL
        ,[ReferenceUser]    SYSNAME NULL
        ,[User]             SYSNAME NULL
        )

    SELECT @sql = N'USE [?]
    -- Get New Login Permissions
    INSERT INTO ##validation ([Login] , [Database] , [Role] , [User])
    SELECT p.name, DB_NAME(), u1.name, u2.name
    FROM sys.database_role_members r
    JOIN sys.database_principals u1
        ON u1.principal_id = r.role_principal_id
    JOIN sys.database_principals u2
        ON u2.principal_id = r.member_principal_id
    CROSS APPLY sys.server_principals p
    WHERE u2.name = ''' + @Login + N'''
    AND p.name = ''' + @Login + N'''
    AND DB_ID() IN (SELECT id FROM ##targetDBs)
    
    -- Get Reference Login Permissions
    INSERT INTO ##validation ([Login] , [Database] , [Role] , [ReferenceUser])
    SELECT p.name, DB_NAME(), u1.name, u2.name
    FROM sys.database_role_members r
    JOIN sys.database_principals u1
        ON u1.principal_id = r.role_principal_id
    JOIN sys.database_principals u2
        ON u2.principal_id = r.member_principal_id
    CROSS APPLY sys.server_principals p
    WHERE u2.name = ''' + @ReferenceLogin + N'''
    AND p.name = ''' + @ReferenceLogin + N'''
    AND DB_ID() IN (SELECT id FROM ##targetDBs)    
    ';

    EXEC sp_MSforeachdb @sql

    SELECT  [Database]
            ,[Role] 
            ,COALESCE([ReferenceUser],N'-') [Reference User]
            --,[Login]
            ,COALESCE([User],N'-') [New User]
            ,'Success' [Result] -- Fake it baby
    FROM    ##validation
    ORDER BY 1,2, CASE WHEN ReferenceUser IS NULL THEN 1 ELSE 0 END
END
GO
SET NOEXEC OFF
GO