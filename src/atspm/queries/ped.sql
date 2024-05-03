--Aggregate Pedestiran Services and Actuations 
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.

SELECT *
FROM (
	SELECT 
		TimeStamp, 
		DeviceId, 
		Parameter::int16 AS Phase, 
		COALESCE("21", 0)::int16 + COALESCE("67", 0)::int16 AS PedServices, 
		COALESCE("90", 0)::int16 AS PedActuation
	FROM (
		SELECT 
			TIME_BUCKET(interval '15 minutes', TimeStamp) AS TimeStamp, 
			DeviceId, 
			EventId, 
			Parameter,
			COUNT(*) AS Total
		FROM 
			{{from_table}}
		GROUP BY ALL
	) q
	PIVOT (
		SUM(Total) FOR EventID IN (21 AS "21", 90 AS "90", 67 AS "67")
	)
) subquery
WHERE 
	PedServices > 0 
	OR PedActuation > 0


