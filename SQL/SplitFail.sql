-- Aggregate Split Failures ATSPM

-- Variables are commented out because they are replaced when query is run from Python.
-- Uncomment variables to run SQL Query directly
--ALSO, TSSU look-up is commented out temporarily until the table is fixed

/*DECLARE @TSSU as VARCHAR(5) = '03008';
DECLARE @Phase as INT= 1;
DECLARE @Det AS INT = 1;
DECLARE @start DATETIME ='2022-07-12';
DECLARE @end DATETIME  = '2022-07-15';*/
DECLARE @RedTime INT = 5; --number of seconds after start of red for which red occupancy will be calculated

	--Run query!--
--use TSSU to look up device id
/*DECLARE @DeviceID AS INT 
SET @DeviceID= (
SELECT GroupableElements.ID
FROM [MaxView_1.9.0.744].[dbo].[GroupableElements]
WHERE Right(GroupableElements.Number,5) = @TSSU)*/

-----------------ALL EVENTS TABLE---------------------------
--Create temp table of detector actuations and phase events, this includes all events used for the rest of the query
SELECT DISTINCT *, CONVERT(date, TimeStamp) as Date
INTO #ALL
FROM ASCEvents 
WHERE ((EventID IN(81,82) and Parameter = @Det) OR (EventID IN(1,7,10) AND Parameter = @Phase)) --detector on, detector off, phase green begin, phase green end, phase red begin
	AND TimeStamp >= @start
	AND TimeStamp < @end
	AND DeviceID = @DeviceID
--append to temp table events 5 seconds after the phase red events, to mark the end of period where occupancy is calculated
INSERT INTO #ALL (TimeStamp, DeviceId, EventId, Parameter, Date)
SELECT dateadd(second, @RedTime, TimeStamp) as TimeStamp, DeviceID, 11 as EventID, Parameter, Date
FROM #ALL
WHERE EventID = 10

-----------------State Table---------------------------
--Add columns which track the states, that is, whether each event occuring while the detector was on or off, the signal was green, or red
--And track these by cycle number, to be grouped later
select *, 
DATEDIFF(MILLISECOND, TimeStamp, LEAD(TimeStamp) OVER (PARTITION BY Date ORDER BY TimeStamp, EventID DESC)) AS MS,
CASE WHEN EventID = 82 THEN 1 WHEN EventID = 81 THEN 0 END AS DetectorState,
COUNT(CASE WHEN EventID IN(82, 81) THEN 1 END) OVER (PARTITION BY Date ORDER BY timestamp, EventID DESC) AS detector_state_group,
CASE WHEN EventID = 10 THEN 1 WHEN EventID = 11 THEN 0 END AS RedState,
COUNT(CASE WHEN EventID IN(10,11) THEN 1 END) OVER (PARTITION BY Date ORDER BY timestamp, EventID DESC) AS red_state_group,
CASE WHEN EventID = 1 THEN 1 WHEN EventID = 7 THEN 0 END AS GreenState,
COUNT(CASE WHEN EventID IN(1,7) THEN 1 END) OVER (PARTITION BY Date ORDER BY timestamp, EventID DESC) AS green_state_group,
COUNT(CASE WHEN EventID = 1 THEN 1 END) OVER (PARTITION BY Date ORDER BY timestamp, EventID DESC) AS cycle_group
into #State
from #ALL
--order by timestamp

-----------------Persist Table---------------------------
--Transform State table to show what the persistant state for detector/red/green are for each event
select TimeStamp, DeviceID, EventID, Parameter, Date, MS,
MAX(DetectorState) OVER (PARTITION BY Date, detector_state_group)   AS detector_state,
MAX(RedState) OVER (PARTITION BY Date, red_state_group)   AS red_state,
MAX(GreenState) OVER (PARTITION BY Date, green_state_group)   AS green_state,
cycle_group
into #Persist
from #State order by TimeStamp

-----------------Group Table---------------------------
--Add up time durations

select 
	MIN([TimeStamp]) AS Cycle_TimeStamp,
	SUM(CASE WHEN green_state = 1 and detector_state = 1 THEN MS/100 ELSE 0 END) * 100 / NULLIF(SUM(CASE WHEN green_state = 1 THEN MS/100 END),0) AS Green_Occupancy,
	SUM(CASE WHEN red_state = 1 and detector_state = 1 THEN MS/100 ELSE 0 END) * 100 / NULLIF(SUM(CASE WHEN red_state = 1 THEN MS/100 END),0) AS Red_Occupancy
INTO #Group
from #Persist 
where red_state IS NOT NULL and green_state IS NOT NULL
group by Date, cycle_group

-----------------Final Result---------------------------
SELECT 
    dateadd(minute, datediff(minute,0,Cycle_TimeStamp)/15 * 15, 0) as TimeStamp,
    @TSSU as TSSU,
    @Phase as Phase,
    @Det as MT,
    COUNT(*) as Total
FROM #Group
WHERE Green_Occupancy > 80 AND Red_Occupancy > 80
GROUP BY dateadd(minute, datediff(minute,0,Cycle_TimeStamp)/15 * 15, 0)
--ORDER BY '15-Minute_TimeStamp'

DROP TABLE #ALL
DROP TABLE #Persist
DROP TABLE #State
DROP TABLE #Group