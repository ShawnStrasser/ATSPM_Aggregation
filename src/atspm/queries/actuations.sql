--Aggregate Detector Actuations
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.

SELECT
    TIME_BUCKET(interval '{{bin_size}} minutes', TimeStamp) as TimeStamp,
    DeviceId,
    Parameter as Detector,
    COUNT(*) AS Total
FROM {{from_table}}
WHERE EventID = 82
GROUP BY TIME_BUCKET(interval '{{bin_size}} minutes', TimeStamp), DeviceId, Detector

