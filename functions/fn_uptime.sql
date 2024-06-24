IF OBJECT_ID('fn_UpTime','IF') IS NOT NULL
	DROP FUNCTION dbo.fn_UpTime
GO
CREATE FUNCTION dbo.fn_UpTime ()
RETURNS TABLE
--WITH ENCRYPTION
AS
/*
====================================================================================================================
Author:         Marco Assis
Create date:    05/2024
Description:    dbo.fn_UpTime
				Returns instance Uptime in datetime + "pretty" string formats
Tested On:      SQL Server 2012+
Notes:
    <none>
Parameters:
    <none>
Returns:
	uptime			DATETIME
	uptime_string	NVARCHAR

Examples:
    SELECT uptime, uptime_string FROM dbo.fn_uptime()
====================================================================================================================
Change History
v1.0
28.05.24	Marco Assis		Initial Build
====================================================================================================================
License:
    GNU General Public License v3.0
    https://github.com/ptmarco/dbatools/blob/master/LICENSE

Github:
    https://github.com/ptmarco/dbatools/

You can contact me by e-mail at marcoassis@gmail.com
====================================================================================================================
*/
RETURN SELECT 
	sqlserver_start_time [uptime]
	,CAST(
		(CONVERT(VARCHAR(12), DATEDIFF(SS,sqlserver_start_time,getdate())/60/60/24)   + ' Day(s), ' 
		+ CONVERT(VARCHAR(12), DATEDIFF(SS,sqlserver_start_time,getdate())/60/60 % 24) + ' Hour(s), '
		+ CONVERT(VARCHAR(2),  DATEDIFF(SS,sqlserver_start_time,getdate())/60 % 60)    + ' Minute(s), ' 
		+ CONVERT(VARCHAR(2),  DATEDIFF(SS,sqlserver_start_time,getdate())% 60)        + ' Second(s).')
		AS NVARCHAR(50)) [uptime_string]
FROM sys.dm_os_sys_info
GO