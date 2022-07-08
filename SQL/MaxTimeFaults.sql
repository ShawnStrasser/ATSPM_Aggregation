--MaxTime Faults broken into 15-minute Bins
--If there is any MaxTime fault inside a 15-minute period then that time period is classified as a fault

--start/end variables are commented out because they are replaced when query is run from Python
--DECLARE @start AS DATETIME = '2022-07-01'
--DECLARE @end AS DATETIME = '2022-07-02'

-----------------DATE TABLE---------------------------
DECLARE @row DATETIME = @start
CREATE TABLE #T (TimeStamp DATETIME)
WHILE @row < @end
BEGIN
INSERT INTO #T
VALUES (@row)
SET @row = DATEADD(MINUTE, 15, @row)
END;
-----------------ON TABLE---------------------------
--All the shorted loop events
SELECT DISTINCT
    dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
    DeviceID,
    EventID,
    Parameter	
INTO #ON
FROM ASCEvents 
WHERE EventID=87 AND TimeStamp >= @start AND TimeStamp < @end
--and this table of distinct deviceid/detector pairs helps makes things fast as a filter in next step
SELECT DISTINCT DeviceID as D, Parameter as P INTO #List FROM #ON
-----------------ALL TABLE---------------------------
--Union the detector off/restored events
SELECT DISTINCT
    dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
    DeviceID,
    83 AS EventID, --don't care if it's a detector restored or detector off event. looking at both incase missing event
    Parameter
INTO #ALL
FROM ASCEvents
JOIN #List ON #List.D = ASCEvents.DeviceId AND #List.P = ASCEvents.Parameter
WHERE EventID IN(81, 83) AND TimeStamp >= @start AND TimeStamp < @end
UNION ALL
SELECT * FROM #ON
-----------------JOIN TABLE---------------------------
--join date table, ON, and OFF tables
SELECT DISTINCT #T.TimeStamp, DeviceID, Parameter
INTO #D
FROM #T, #ALL

SELECT #D.TimeStamp, #D.DeviceId, EventID, #D.Parameter
INTO #FINAL
FROM #D
left join #ALL
ON #ALL.TimeStamp = #D.TimeStamp and #ALL.DeviceID=#D.DeviceID and #ALL.Parameter=#D.Parameter
ORDER BY DeviceID, Parameter, TimeStamp

SELECT 
    TimeStamp, 
    DeviceID,
    CASE WHEN EventID IS NULL THEN 
    (SELECT TOP 1 EventID FROM #FINAL WHERE TimeStamp < F.TimeStamp AND EventID IS NOT NULL AND DeviceID=F.DeviceId AND Parameter=F.Parameter ORDER BY TimeStamp DESC) --OVER (PARTITION BY F.DeviceID)
    ELSE EventID 
    END AS EventID,
    Parameter
INTO #STUCKON
FROM #FINAL F
-----------------FINAL RESULTS---------------------------
--Combine Stuck On events with Erratic events
SELECT * FROM #STUCKON
WHERE EventID=87
UNION ALL
SELECT DISTINCT
    dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
    DeviceID,
    EventID, 
    Parameter
FROM ASCEvents
WHERE EventID=88 AND TimeStamp >= @start AND TimeStamp < @end
DROP TABLE #ALL
DROP TABLE #T
DROP TABLE #List
DROP TABLE #D
DROP TABLE #FINAL
DROP TABLE #ON
DROP TABLE #STUCKON