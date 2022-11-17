--Aggregate Unique Pedestiran Actuations in 15-minute Bins
--The intention of "unique" actuations is that back-to-back actuations from the same person are removed
--The default time lapse before a new unique actuation for a given ped phase may be counted is 15 seconds
--see this study: https://doi.org/10.1177/0361198121994126


--start/end variables are commented out because they are replaced when query is run from Python
--DECLARE @start AS DATETIME = '2022-11-02'
--DECLARE @end AS DATETIME = '2022-11-03'
DECLARE @threshold AS INT = 15000 --minimum milliseconds between actuations to be counted as unique

SELECT
    TimeStamp,
    DeviceId,
    Phase,
    COUNT(*) as Unique_Actuations
FROM
    (SELECT 
        dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp,
        DeviceId,
        Parameter as Phase,
        DATEDIFF(MILLISECOND, LAG(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp), TimeStamp) as Diff_Milliseconds
    FROM ASCEvents
    WHERE
        EventID = 90
        AND TimeStamp >= @start
        AND TimeStamp < @end
    ) q
WHERE
    Diff_Milliseconds > @threshold
GROUP BY
    TimeStamp, DeviceId, Phase