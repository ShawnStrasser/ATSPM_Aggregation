--Averaged Communications Statistics
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.

SELECT
    TIME_BUCKET(interval '{{bin_size}} minutes', TimeStamp) as TimeStamp,
    DeviceId,
    EventId::int16 AS EventId,
    AVG(Parameter)::float AS Average
FROM
    {{from_table}}
WHERE
    EventID IN({{event_codes}}) --vendor specific, 400,503,502 for MaxView
GROUP BY
    TIME_BUCKET(interval '{{bin_size}} minutes', TimeStamp),
    DeviceId,
    EventId