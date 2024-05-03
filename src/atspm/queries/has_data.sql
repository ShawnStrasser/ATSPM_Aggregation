-- A table of timestamps for each device to confirm if it has data (proxy for being online).
-- Final Columns are: TimeStamp, DeviceId
-- Having a timestamp for a deviceid means no missing data was found for that device during the bin_size interval.
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.


SELECT
    TIME_BUCKET(interval '{{bin_size}} minutes', interval_timestamp) as TimeStamp,
    DeviceId
FROM (
    SELECT
        TIME_BUCKET(interval '{{no_data_min}} minutes', TimeStamp) as interval_timestamp,
        DeviceId
    FROM {{from_table}}
    WHERE EventID <= 218 -- EventIDs greater than 218 are vendor-specific
    GROUP BY ALL
    HAVING COUNT(*) >= {{min_data_points}} -- Only include groups with a count greater than or equal to 'min_data_points'
) AS subquery
GROUP BY ALL
HAVING COUNT(*) = ({{bin_size}} / {{no_data_min}})
