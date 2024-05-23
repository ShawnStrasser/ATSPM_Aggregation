--Aggregate Unique Pedestiran Actuations in 15-minute Bins
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.

--The intention of "unique" actuations is that back-to-back actuations from the same person are removed
--The default time lapse before a new unique actuation for a given ped phase may be counted is 15 seconds
--see this study: https://doi.org/10.1177/0361198121994126

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