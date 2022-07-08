--Coordination Events (Pattern change, Cycle lenght change, Actual cycle length, Actual cycle offset)

--start/end variables are commented out because they are replaced when query is run from Python
--DECLARE @start AS DATETIME = '2022-07-01'
--DECLARE @end AS DATETIME = '2022-07-02'

SELECT
	TimeStamp,
	DeviceID,
	EventID,
	Parameter,
	dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as '15-Minute_TimeStamp'
FROM
	(SELECT DISTINCT *
	FROM ASCEvents
	WHERE
		EventID IN(131,132,316,318)
		and TimeStamp >= @start and TimeStamp < @end) q