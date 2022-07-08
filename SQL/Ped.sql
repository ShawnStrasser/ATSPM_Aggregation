--Aggregate Pedestiran Services and Actuations in 15-minute Bins

--start/end variables are commented out because they are replaced when query is run from Python
--DECLARE @start AS DATETIME = '2022-07-01'
--DECLARE @end AS DATETIME = '2022-07-02'

SELECT
	TimeStamp,
	DeviceID,
	Parameter as Phase,
	IsNull([21],0) + IsNull([67],0) AS PedServices,
	IsNull([90],0) AS PedActuation
FROM
	(SELECT
		dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp,
		DeviceID,
		EventID,
		Parameter,
		COUNT(*) AS Total
	FROM
		(SELECT DISTINCT *
		FROM ASCEvents
		WHERE
			EventID IN(21,90,67)
			AND TimeStamp >= @start
			AND TimeStamp < @end) q
	GROUP BY 
		dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0),
		DeviceID,
		EventID,
		Parameter
	) q
PIVOT( SUM(Total) FOR EventID IN([21], [90], [67])) z