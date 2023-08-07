/****************************************************************/
--Filename:		dbo.SP_ProcPerf.sql					    	   
--Author:		Oliver Flindall								   
--Date:			17/11/22									   
--Description:	Stored proc that analyses the proc and query cache to look at proc and statement level execution statistics.
--Revisions:	

--Usage:		exec dbo.SP_ProcPerf 'usp_DBA_PERF'						   
/****************************************************************/

IF EXISTS(SELECT 1 FROM sys.procedures 
          WHERE Name = 'SP_ProcPerf')
BEGIN
    DROP PROCEDURE dbo.SP_ProcPerf
END
go

create procedure dbo.SP_ProcPerf @proc VARCHAR (MAX) -- = 'usp_DBA_PERF'

as

SELECT d.name AS [DB_Name],
       OBJECT_NAME(ps.object_id, qplan.dbid) AS [ObjectName],
	   ps.object_id,
	   s.name,
       execution_count,
       last_execution_time,
	   CASE
		  WHEN (ps.last_elapsed_time / 1000.) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((ps.last_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (ps.last_elapsed_time / 1000000. ) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.last_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (ps.last_elapsed_time / 60000000.) > 1
                AND ((ps.last_elapsed_time / 60000000)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.last_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN (ps.last_elapsed_time /3600000000.)  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.last_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Last Elapsed Time],
	   	   CASE
		  WHEN (ps.last_worker_time / 1000.) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((ps.last_worker_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (ps.last_worker_time / 1000000. ) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.last_worker_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (ps.last_worker_time / 60000000.) > 1
                AND ((ps.last_worker_time / 60000000)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.last_worker_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN (ps.last_worker_time /3600000000.)  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.last_worker_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Last worker Time],
       CASE
		  WHEN ((ps.total_elapsed_time / 1000.) / ps.execution_count) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((ps.total_elapsed_time / 1000.)/ ps.execution_count,2) AS NUMERIC (36,2))) + ' ms'

           WHEN (ps.total_elapsed_time / 1000000.) / ps.execution_count < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_elapsed_time / 1000000.)/ ps.execution_count,2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN ((ps.total_elapsed_time / 60000000.) / ps.execution_count) > 1
                AND ((ps.total_elapsed_time / 60000000.) / ps.execution_count) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_elapsed_time / 60000000.)/ ps.execution_count,2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((ps.total_elapsed_time / 3600000000.) / ps.execution_count) > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_elapsed_time / 3600000000.)/ ps.execution_count,2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Average elapsed time],
	   CASE
		  WHEN ((ps.max_elapsed_time / 1000.) ) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((ps.max_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN CAST(ps.max_elapsed_time / 1000000. AS DECIMAL(20, 5)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.max_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (ps.total_elapsed_time / 60000000.) > 1
                AND (ps.total_elapsed_time / 60000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.max_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((ps.total_elapsed_time / 3600000000.))  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.max_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Max elapsed time],
       CASE
		  WHEN ((ps.min_elapsed_time / 1000.) ) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((ps.min_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN CAST(ps.min_elapsed_time / 1000000. AS DECIMAL(20, 5)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.min_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (ps.total_elapsed_time / 60000000.) > 1
                AND (ps.total_elapsed_time / 60000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.min_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((ps.total_elapsed_time / 3600000000.))  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.min_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Min elapsed time],
       CASE
		  WHEN ((ps.total_elapsed_time / 1000.) / ps.execution_count) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((ps.total_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (ps.total_elapsed_time / 1000000) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (ps.total_elapsed_time / 60000000.) > 1
                AND (ps.total_elapsed_time /  60000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN (ps.total_elapsed_time / 3600000000.)  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
	   END AS [TotalElapsedTime],
       -- ps.total_elapsed_time,
       CASE
           WHEN ((ps.total_worker_time / 1000.) / ps.execution_count) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((ps.total_worker_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (ps.total_worker_time / 1000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_worker_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (ps.total_worker_time / 60000000.) > 1
                AND ((ps.total_worker_time / 60000000.)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_worker_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((ps.total_worker_time / 3600000000.))  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((ps.total_worker_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Total_CPU_time],
	    ps.last_logical_reads,
       ps.total_logical_reads / ps.execution_count AS [AVG logical Reads],
       ps.last_physical_reads,
	   ps.total_physical_reads / ps.execution_count AS [AVG Physical Reads],
       ps.cached_time,
       qplan.query_plan.value(
                                 'declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";
(//p:ParameterList/p:ColumnReference/@Column)[1]',
                                 'varchar(128)'
                             ) paramName,
       qplan.query_plan.value(
                                 'declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";
(//p:ParameterList/p:ColumnReference/@ParameterCompiledValue)[1]',
                                 'varchar(100)'
                             ) paramValue,
       qplan.query_plan,
       qtext.text
FROM sys.dm_exec_procedure_stats ps
    --JOIN sys.objects o ON o.object_id = ps.object_id
    JOIN sys.databases d
        ON d.database_id = ps.database_id
    OUTER APPLY sys.dm_exec_sql_text(plan_handle) AS qtext
    OUTER APPLY sys.dm_exec_query_plan(plan_handle) AS qplan
	left JOIN sys.objects o ON o.object_id = ps.object_id
	left JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE OBJECT_NAME(ps.object_id, ps.database_id) = @proc


-- second bit gets statement level parts of proc


SELECT DB_NAME(est.dbid) AS [DB_Name],
       OBJECT_NAME(est.objectid, est.dbid) AS [ObjectName],
	   est.objectid,
	   s.name,
       CASE
           WHEN est.encrypted = 1 THEN
               '-- ENCRYPTED'
           WHEN qs.statement_start_offset / 2 >= 0 THEN
               SUBSTRING(
                            est.text,
                            qs.statement_start_offset / 2 + 1,
                            CASE qs.statement_end_offset / 2
                                WHEN 0 THEN
                                    DATALENGTH(est.text)
                                ELSE
                                    qs.statement_end_offset / 2 - qs.statement_start_offset / 2 + 1
                            END
                        )
       END AS Statement,
       qs.execution_count,
       qs.last_execution_time,
        	   case
		  WHEN (qs.last_elapsed_time / 1000.) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.last_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (qs.last_elapsed_time / 1000000. ) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.last_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (qs.last_elapsed_time / 60000000.) > 1
                AND ((qs.last_elapsed_time / 60000000)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.last_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN (qs.last_elapsed_time /3600000000.)  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.last_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Last Elapsed Time],
	           	   case
		  WHEN (qs.last_worker_time / 1000.) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.last_worker_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (qs.last_worker_time / 1000000. ) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.last_worker_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (qs.last_worker_time / 60000000.) > 1
                AND ((qs.last_worker_time / 60000000)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.last_worker_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN (qs.last_worker_time /3600000000.)  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.last_worker_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Last worker Time],
       CASE
		  WHEN ((qs.total_elapsed_time / 1000.) / qs.execution_count) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.total_elapsed_time / 1000.)/ qs.execution_count,2) AS NUMERIC (36,2))) + ' ms'

           WHEN (qs.total_elapsed_time / 1000000.) / qs.execution_count < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_elapsed_time / 1000000.)/ qs.execution_count,2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN ((qs.total_elapsed_time / 60000000.) / qs.execution_count) > 1
                AND ((qs.total_elapsed_time / 60000000.) / qs.execution_count) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_elapsed_time / 60000000.)/ qs.execution_count,2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((qs.total_elapsed_time / 3600000000.) / qs.execution_count) > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_elapsed_time / 3600000000.)/ qs.execution_count,2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Average elapsed time],
	   	   CASE
		  WHEN ((qs.max_elapsed_time / 1000.) ) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.max_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN CAST(qs.max_elapsed_time / 1000000. AS DECIMAL(20, 5)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.max_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (qs.total_elapsed_time / 60000000.) > 1
                AND (qs.total_elapsed_time / 60000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.max_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' mini'
           WHEN ((qs.total_elapsed_time / 3600000000.))  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.max_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Max elapsed time],
       CASE
		  WHEN ((qs.min_elapsed_time / 1000.) ) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.min_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN CAST(qs.min_elapsed_time / 1000000. AS DECIMAL(20, 5)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.min_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (qs.total_elapsed_time / 60000000.) > 1
                AND (qs.total_elapsed_time / 60000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.min_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((qs.total_elapsed_time / 3600000000.))  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.min_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Min elapsed time],
       CASE
		  WHEN ((qs.total_elapsed_time / 1000.)) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.total_elapsed_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (qs.total_elapsed_time / 1000000) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_elapsed_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (qs.total_elapsed_time / 60000000.) > 1
                AND (qs.total_elapsed_time /  60000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_elapsed_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN (qs.total_elapsed_time / 3600000000.)  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_elapsed_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
	   END AS [TotalElapsedTime],
       -- qs.total_elapsed_time,
       CASE
           WHEN ((qs.total_worker_time / 1000.)) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.total_worker_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (qs.total_worker_time / 1000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_worker_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (qs.total_worker_time / 60000000.) > 1
                AND ((qs.total_worker_time / 60000000.)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_worker_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((qs.total_worker_time / 3600000000.))  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.total_worker_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Total_CPU_time],
	   CASE
           WHEN ((qs.max_worker_time / 1000.)) < 60 THEN
               CONVERT(VARCHAR, CAST(ROUND((qs.max_worker_time / 1000.),2) AS NUMERIC (36,2))) + ' ms'

           WHEN (qs.max_worker_time / 1000000.) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.max_worker_time / 1000000.),2) AS NUMERIC (36,2))
                      ) + ' secs'
           WHEN (qs.max_worker_time / 60000000.) > 1
                AND ((qs.max_worker_time / 60000000.)) < 60 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.max_worker_time / 60000000.),2) AS NUMERIC (36,2))
                      ) + ' min'
           WHEN ((qs.max_worker_time / 3600000000.))  > 1 THEN
               CONVERT(
                          VARCHAR,
                          CAST(ROUND((qs.max_worker_time / 3600000000.),2) AS NUMERIC (36,2))
                      ) + ' hours'
       END AS [Max_CPU_time],
       qs.last_rows,
       qs.max_rows,
       qs.total_rows / qs.execution_count AS [AVG Rows],
       qs.last_logical_reads,
       qs.last_physical_reads,
       qs.max_physical_reads,
       qs.total_physical_reads,
       qs.last_used_grant_kb,
       qs.total_used_grant_kb,
       qs.creation_time,
	   qs.total_elapsed_time,
       CAST(query_plan AS XML) AS [Query plan],
	   qs.total_elapsed_time
FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) est
    CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) qp
		left JOIN sys.objects o ON o.object_id = est.objectid
	left JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE OBJECT_NAME(est.objectid, est.dbid) = @proc

ORDER BY qs.total_elapsed_time DESC;



