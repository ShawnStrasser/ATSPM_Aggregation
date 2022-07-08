--Aggregate Phase Splits in 15-minute Bins

--start/end variables are commented out because they are replaced when query is run from Python
--DECLARE @start AS DATETIME = '2022-07-01'
--DECLARE @end AS DATETIME = '2022-07-02'

SELECT 
	dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp,
	DeviceID,
	EventID,
	COUNT(*) AS Services, 
	AVG(Parameter) AS Average_Split
FROM 
	(SELECT DISTINCT *
	FROM ASCEvents
	WHERE 
		(EventID BETWEEN 300 AND 317) 
		AND TimeStamp >= @start 
		AND TimeStamp < @end) q
GROUP BY 
	dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0),
	DeviceID,
	EventID