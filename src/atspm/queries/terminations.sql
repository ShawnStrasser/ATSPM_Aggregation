--Aggregate terminations (GapOut, MaxOut, ForceOff, and also, PhaseCall for some reason)
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.

SELECT
    time_bucket(interval '{{bin_size}} minutes', TimeStamp) as TimeStamp,
    DeviceId as DeviceId,
    Parameter::int16 as Phase,
    CASE 
        WHEN EventId = 4 THEN 'GapOut'
        WHEN EventId = 5 THEN 'MaxOut'
        WHEN EventId = 6 THEN 'ForceOff'
    END AS PerformanceMeasure,
    COUNT(*)::int16 as Total
FROM {{from_table}}
WHERE EventId IN (4, 5, 6) 
GROUP BY ALL