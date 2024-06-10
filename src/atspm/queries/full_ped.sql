--Aggregate Pedestiran Services, Actuations, Unique Actuations
--MUST BE RUN OVER MULTIPLE TIME PERIODS FOR VOLUMES TO WORK!
--Uses a rolling 15-minute window, data at the end is cut off or distorted. So for example run over a day and 11:15pm-midnight will be cut off.
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.

--The intention of "unique" actuations is that back-to-back actuations from the same person are removed
--The default time lapse before a new unique actuation for a given ped phase may be counted is 15 seconds
--see this study: https://doi.org/10.1177/0361198121994126

-- services and actuations
WITH ped1 AS(
	SELECT 
		TimeStamp, 
		DeviceId, 
		Parameter::int16 AS Phase, 
		COALESCE("21", 0)::int16 + COALESCE("67", 0)::int16 AS PedServices, 
		COALESCE("90", 0)::int16 AS PedActuation
	FROM (
		SELECT 
			TIME_BUCKET(interval '{{bin_size}} minutes', TimeStamp) AS TimeStamp, 
			DeviceId, 
			EventId, 
			Parameter,
			COUNT(*) AS Total
		FROM 
			{{from_table}}
		WHERE 
			EventId IN (21, 90, 67)
		GROUP BY ALL
	) q
	PIVOT (
		SUM(Total) FOR EventID IN (21 AS "21", 90 AS "90", 67 AS "67")
	)
),

--unique actuations
ped2 AS(
    SELECT
        TimeStamp,
        DeviceId, 
        Phase,
        COUNT(*)::int16 as Unique_Actuations
    FROM
        (SELECT 
            TIME_BUCKET(interval '{{bin_size}} minutes', TimeStamp) as TimeStamp,
            DeviceId,
            Parameter::int16 as Phase,
            DATEDIFF('MILLISECOND', LAG(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp), TimeStamp)::float as Diff_Milliseconds    
        FROM 
            {{from_table}}
        WHERE
            EventId = 90
        ) q
    WHERE
        q.Diff_Milliseconds > {{seconds_between_actuations}}000 --convert seconds to milliseconds
    GROUP BY
        TimeStamp, DeviceID, Phase
),

--combined
ped_combined AS(
    SELECT *
    FROM ped1
    FULL JOIN ped2 USING (TimeStamp, DeviceId, Phase)
)

--if variable return_volumes is False, return the combined table below
{% if return_volumes == False %}
SELECT 
    TimeStamp,
    DeviceId,
    Phase, 
    COALESCE(PedServices, 0) AS PedServices,
    COALESCE(PedActuation, 0) AS PedActuation,
    COALESCE(Unique_Actuations, 0) AS Unique_Actuations
FROM ped_combined

--if variable return_volumes is True, return the volumes table below
{% else %}
,
T AS (
    SELECT generate_series AS TimeStamp
    FROM generate_series(
        TIMESTAMP '{{min_timestamp}}',
        TIMESTAMP '{{max_timestamp}}',
        INTERVAL '{{bin_size}} minutes'
    )
),
U AS (
    SELECT DISTINCT DeviceId, Phase
    FROM ped_combined
),
all_times AS (
    SELECT *
    FROM T
    CROSS JOIN U
),
filled_in AS (
    SELECT
        TimeStamp,
        DeviceId,
        Phase, 
        COALESCE(PedServices, 0) AS PedServices,
        COALESCE(PedActuation, 0) AS PedActuation,
        COALESCE(Unique_Actuations, 0) AS Unique_Actuations
    FROM all_times
    LEFT JOIN ped_combined USING (TimeStamp, DeviceId, Phase)
),
add_rolling_sum AS (
    SELECT 
        *,
        SUM(Unique_Actuations) OVER (
            PARTITION BY DeviceId, Phase
            ORDER BY TimeStamp
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS Unique_Actuations_Rolling
    FROM filled_in
),

--Transform to volumes
--Estimated_Hourly = A90C * Hourly_Actuations + A90C_squared * Hourly_Actuations^2 + Intercept
--where A90C = 0.7167, A90C_squared = 0.0599, Intercept = 1.1063
volumes AS (
    SELECT
        * EXCLUDE (Unique_Actuations_Rolling),
        (0.7167 * Unique_Actuations_Rolling + 0.0599 * Unique_Actuations_Rolling^2 + 1.1063)::FLOAT AS Estimated_Hourly
    FROM add_rolling_sum
)
SELECT *
FROM volumes
--FROM T
ORDER BY DeviceId, Phase, TimeStamp DESC

{% endif %}



