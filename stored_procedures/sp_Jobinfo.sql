USE [dba_database]
GO

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

IF OBJECT_ID('dbo.sp_JobInfo') IS  NULL
    EXEC ('CREATE PROCEDURE dbo.sp_JobInfo AS RETURN 138;');
GO

ALTER PROCEDURE dbo.sp_JobInfo
	/* Parameters */
   @type		TINYINT		= 1,		-- 1 Jobs with Schedules (default), 
										-- 2 Last Run Date + Status
										-- 3 Job History ***** NOT DONE ******
										-- 9 Running Jobs
   @jobs		NCHAR(3)	= N'dba',	-- all/dba for All Jobs or DBA_% Only (Default)
   @daysback	TINYINT		= 7,		-- For @type = 3
   @help		TINYINT		= 0
--WITH ENCRYPTION
AS
BEGIN
	
	IF @type NOT IN (1,2,3,9)
		SET @help = 1

	IF @help = 1
	BEGIN
		PRINT '
=====================================================================================================
	Author:        	Marco Assis
	Create date:   	19/01/22
	Description:   	Lists Job Usefull Information
	Tested on:		SQL 2012, 2016, 2019
---------------------------------------------------------------------------------------------------
	Parameters:
	@type		= 1 Jobs with Schedules (default), 
				= 2 Last Day Results Only, 
				= 3 Total Job History (not done yet)
				= 9 Running Jobs
	@jobs		= ''ALL''/''DBA''  All Jobs or DBA Only (default)
	@DaysBack	= # of days to look back
---------------------------------------------------------------------------------------------------
	Change History
	Date   		Version		Author       	Description
				v1.0 		Marco Assis		Initial Build
	08/02/22	v1.01		Marco Assis		Last RunDate/Time improvement
	09/02/22	v1.02		Marco Assis		xxxxx
	02/03/22	v1.03		Marco Assis		Add @type=9 for Running jobs
	07/03/22	v1.04		Marco Assis		Add AvgDuration to @type=9
---------------------------------------------------------------------------------------------------
	To Do:
		1. Fix type 2 to only give LAST
		2. Add code for @type = 3
---------------------------------------------------------------------------------------------------
	You may alter this code for your own *non-commercial* purposes. You may
	republish altered code as long as you include this copyright and give due
	credit, but you must obtain prior permission before blogging this code.
		   
	THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
	ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
	TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
	PARTICULAR PURPOSE.
=====================================================================================================
';
	RETURN;
	END

	DECLARE @sql NVARCHAR(2000) = N'';

	IF @type = 1  /* Option 1 - Extract Jobs & Schedules */
		BEGIN 
			SELECT 'Type 1: Jobs with Schedules' as [Type];
			SELECT 
			S.name AS JobName,
			SC.name as Category,
			CONVERT(VARCHAR(16),S.date_created, 20) as [Created],
			S.enabled AS IsJobEnabled,
			SS.name AS ScheduleName,  
			SS.enabled AS IsScheduleEnable,
			CASE (S.notify_level_email)
			WHEN 0 THEn 'Never'
			WHEN 1 THEN 'OnSuccess'
			WHEN 2 THEN 'OnFail'
			END as AlertEmail,
			ISNULL(O.name,'') as Operator,
			CASE(SS.freq_type)
				WHEN 1  THEN 'Once'
				WHEN 4  THEN 'Daily'
				WHEN 8  THEN (case when (SS.freq_recurrence_factor > 1) then  'Every ' + convert(varchar(3),SS.freq_recurrence_factor) + ' Weeks'  else 'Weekly'  end)
				WHEN 16 THEN (case when (SS.freq_recurrence_factor > 1) then  'Every ' + convert(varchar(3),SS.freq_recurrence_factor) + ' Months' else 'Monthly' end)
				WHEN 32 THEN 'Every ' + convert(varchar(3),SS.freq_recurrence_factor) + ' Months' -- RELATIVE
				WHEN 64 THEN 'SQL Startup'
				WHEN 128 THEN 'SQL Idle'
				ELSE '??'
			END AS Frequency,  
			CASE
				WHEN (freq_type = 1)                       then 'One time only'
				WHEN (freq_type = 4 and freq_interval = 1) then 'Every Day'
				WHEN (freq_type = 4 and freq_interval > 1) then 'Every ' + convert(varchar(10),freq_interval) + ' Days'
				WHEN (freq_type = 8) then (select 'Weekly Schedule' = MIN(D1+ D2+D3+D4+D5+D6+D7 )
											from (select SS.schedule_id,
															freq_interval, 
															'D1' = CASE WHEN (freq_interval & 1  <> 0) then 'Sun ' ELSE '' END,
															'D2' = CASE WHEN (freq_interval & 2  <> 0) then 'Mon '  ELSE '' END,
															'D3' = CASE WHEN (freq_interval & 4  <> 0) then 'Tue '  ELSE '' END,
															'D4' = CASE WHEN (freq_interval & 8  <> 0) then 'Wed '  ELSE '' END,
														'D5' = CASE WHEN (freq_interval & 16 <> 0) then 'Thu '  ELSE '' END,
															'D6' = CASE WHEN (freq_interval & 32 <> 0) then 'Fri '  ELSE '' END,
															'D7' = CASE WHEN (freq_interval & 64 <> 0) then 'Sat '  ELSE '' END
														from msdb..sysschedules ss
													where freq_type = 8
												) as F
											where schedule_id = SJ.schedule_id
										)
				WHEN (freq_type = 16) then 'Day ' + convert(varchar(2),freq_interval) 
				WHEN (freq_type = 32) then (select  freq_rel + WDAY 
											from (select SS.schedule_id,
															'freq_rel' = CASE(freq_relative_interval)
																		WHEN 1 then 'First'
																		WHEN 2 then 'Second'
																		WHEN 4 then 'Third'
																		WHEN 8 then 'Fourth'
																		WHEN 16 then 'Last'
																		ELSE '??'
																		END,
														'WDAY'     = CASE (freq_interval)
																		WHEN 1 then ' Sun'
																		WHEN 2 then ' Mon'
																		WHEN 3 then ' Tue'
																		WHEN 4 then ' Wed'
																		WHEN 5 then ' Thu'
																		WHEN 6 then ' Fri'
																		WHEN 7 then ' Sat'
																		WHEN 8 then ' Day'
																		WHEN 9 then ' Weekday'
																		WHEN 10 then ' Weekend'
																		ELSE '??'
																		END
													from msdb..sysschedules SS
													where SS.freq_type = 32
													) as WS 
											where WS.schedule_id = SS.schedule_id
											) 
			END AS Interval,
			CASE (freq_subday_type)
				WHEN 1 then   left(stuff((stuff((replicate('0', 6 - len(active_start_time)))+ convert(varchar(6),active_start_time),3,0,':')),6,0,':'),8)
				WHEN 2 then 'Every ' + convert(varchar(10),freq_subday_interval) + ' seconds'
				WHEN 4 then 'Every ' + convert(varchar(10),freq_subday_interval) + ' minutes'
				WHEN 8 then 'Every ' + convert(varchar(10),freq_subday_interval) + ' hours'
				ELSE '??'
			END AS [Time],
			h.AVG_Duration_Min,
			CASE SJ.next_run_date
				WHEN 0 THEN cast('n/a' as char(10))
				ELSE convert(char(10), convert(datetime, convert(char(8),SJ.next_run_date)),120)  + ' ' + left(stuff((stuff((replicate('0', 6 - len(next_run_time)))+ convert(varchar(6),next_run_time),3,0,':')),6,0,':'),8)
			END AS NextRunTime,
			CASE SS.enabled 
				WHEN 1 THEN 'Enabled'
				ELSE N'Disabled'
			END AS [Schedule]
			INTO #jobs1
			FROM msdb.dbo.sysjobs S
			LEFT JOIN msdb.dbo.sysjobschedules SJ on S.job_id = SJ.job_id  
			LEFT JOIN msdb.dbo.sysschedules SS on SS.schedule_id = SJ.schedule_id
			LEFT JOIN msdb.dbo.syscategories SC ON SC.category_id = S.category_id
			LEFT JOIN (
			SELECT job_id, 
			((avg(run_duration)/10000*3600 + (avg(run_duration)/100)%100*60 + avg(run_duration)%100 + 31 ) / 60) 
				as 'AVG_Duration_Min' 
			FROM msdb.dbo.sysjobhistory 
			GROUP BY job_id) h 
			ON h.job_id = s.job_id
			LEFT JOIN msdb.dbo.sysoperators O ON S.notify_email_operator_id = O.id

			SET @sql = N'SELECT * FROM #jobs1 WHERE isJobEnabled = 1';
			IF @jobs = N'dba' 
				SET @sql = @sql + N' AND category = ''DBA_KYNDRYL'''
			SET @sql = @sql + N' ORDER BY JobName ASC;'
			EXEC(@sql);
			DROP TABLE #jobs1;

		END
		
	IF @type = 3
		SELECT 'Type 3: ** Work in Progres **' as [Type];
	
	IF @type = 2  /* Option 2 - Last Run Information */
		BEGIN 
			SELECT 'Type 2: Last Day Results Only' as [Type];
			SELECT DISTINCT SJ.Name AS JobName, 
			SC.name as Category,
			SJ.description AS JobDescription,
			CONVERT(VARCHAR(16), msdb.dbo.agent_datetime(SJH.run_date, SJH.run_time), 120) as LastRun,
			--RIGHT(SJH.run_date,2) + '/' + SUBSTRING(CAST(SJH.run_date AS CHAR(8)),5,2) + '/' + LEFT(SJH.run_date,4) + ' ' + FORMAT(SJH.run_time, '00:00:00') as LastRun,
			CASE SJH.run_status 
				WHEN 0 THEN 'Failed' 
				WHEN 1 THEN 'Successful' 
				WHEN 3 THEN 'Cancelled' 
				WHEN 4 THEN 'In Progress' 
				ELSE 'Unknown'
				END AS LastRunStatus,
			((SJH.run_duration/10000*3600 + (SJH.run_duration/100)%100*60 + SJH.run_duration%100 + 31 ) / 60) as Duration_min
			INTO #jobs2
			FROM msdb.dbo.sysjobhistory SJH
			LEFT JOIN msdb.dbo.sysjobs SJ ON SJ.job_id = SJH.job_id
			LEFT JOIN msdb.dbo.syscategories SC ON SC.category_id = SJ.category_id
			WHERE SJH.job_id = SJ.job_id and SJH.run_date = 
			(SELECT MAX(SJH1.run_date) FROM msdb.dbo.sysjobhistory SJH1 WHERE SJH.job_id = SJH1.job_id)
			AND SC.name = 'DBA_KYNDRYL'
			AND SJ.enabled = 1

			SET @sql = N'SELECT * FROM #jobs2 ORDER BY 1, 4 DESC';
			EXEC(@sql);
			DROP TABLE #jobs2;
		END
		
	IF @type = 9 /* Option 9 - Running Jobs */
		BEGIN
			SELECT 'Type 9: Running Jobs' as [Type];
			SELECT
				ja.job_id,
				j.name AS job_name,
				ja.start_execution_date,    
				DATEDIFF(mi,ja.start_execution_date,getdate()) as [ElapsedTime(min)],
				h.[AvgDuration(min)],
				ISNULL(last_executed_step_id,0)+1 AS current_executed_step_id,
				Js.step_name
			FROM msdb.dbo.sysjobactivity ja 
			LEFT JOIN msdb.dbo.sysjobhistory jh 
				ON ja.job_history_id = jh.instance_id
			JOIN msdb.dbo.sysjobs j 
			ON ja.job_id = j.job_id
			JOIN msdb.dbo.sysjobsteps js
				ON ja.job_id = js.job_id
				AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
			LEFT JOIN (
			SELECT job_id, 
			((avg(run_duration)/10000*3600 + (avg(run_duration)/100)%100*60 + avg(run_duration)%100 + 31 ) / 60) 
				as 'AvgDuration(min)' 
			FROM msdb.dbo.sysjobhistory 
			GROUP BY job_id) h
				ON h.job_id = j.job_id
			WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
			AND start_execution_date is not null
			AND stop_execution_date is null;
		END

END
GO