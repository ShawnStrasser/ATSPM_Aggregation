--Detector Faults broken into 15-minute Bins
--NEEDS MORE WORK!!!!!!! NEED TO SAVE/USE DETECTOR STATES!
-----------------DATE TABLE---------------------------
WITH T AS (
    SELECT generate_series AS TimeStamp
    FROM generate_series(
        TIMESTAMP '{{min_timestamp}}',
        TIMESTAMP '{{max_timestamp}}',
        INTERVAL '{{bin_size}} minutes'
    )
),
-----------------ON TABLE---------------------------
--All the shorted loop events
ON_table AS (
    SELECT DISTINCT
        time_bucket(INTERVAL '{{bin_size}} minutes', TimeStamp) AS TimeStamp,
        DeviceID,
        EventID,
        Parameter
    FROM {{from_table}}
    WHERE EventID = 87
),
list_table AS (
    SELECT DISTINCT
        DeviceID AS D,
        Parameter AS P
    FROM ON_table
),
-----------------ALL TABLE---------------------------
--Union the detector off/restored events
ALL_table AS (
    SELECT DISTINCT
        time_bucket(INTERVAL '{{bin_size}} minutes', TimeStamp) AS TimeStamp,
        DeviceID,
        83 AS EventID,
        Parameter
    FROM {{from_table}}
    JOIN list_table ON list_table.D = {{from_table}}.DeviceId AND list_table.P = {{from_table}}.Parameter
    WHERE EventID IN (81, 83)
    UNION ALL
    SELECT * FROM ON_table
),
-----------------JOIN TABLE---------------------------
--join date table, ON, and OFF tables
D AS (
    SELECT DISTINCT
        T.TimeStamp,
        DeviceID,
        Parameter
    FROM T, ALL_table
),
FINAL AS (
    SELECT
        D.TimeStamp,
        D.DeviceID,
        EventId,
        D.Parameter
    FROM D
    LEFT JOIN ALL_table ON ALL_table.TimeStamp = D.TimeStamp AND ALL_table.DeviceID = D.DeviceID AND ALL_table.Parameter = D.Parameter
),
STUCKON AS (
    SELECT
        TimeStamp,
        DeviceID,
        CASE
            WHEN EventID IS NULL THEN (
                SELECT EventID
                FROM FINAL
                WHERE TimeStamp < F.TimeStamp
                    AND EventID IS NOT NULL
                    AND DeviceID = F.DeviceId
                    AND Parameter = F.Parameter
                ORDER BY TimeStamp DESC
                LIMIT 1
            )
            ELSE EventID
        END AS EventID,
        Parameter
    FROM FINAL F
)

SELECT * FROM STUCKON WHERE EventID = 87
UNION ALL
SELECT DISTINCT
    time_bucket(INTERVAL '{{bin_size}} minutes', TimeStamp) AS TimeStamp,
    DeviceID,
    EventID,
    Parameter
FROM {{from_table}}
WHERE EventID = 88
order by timestamp