--Averaged Communications Statistics in 15-minute Bins

--start/end variables are commented out because they are replaced when query is run from Python
--DECLARE @start AS DATETIME = '2022-07-01'
--DECLARE @end AS DATETIME = '2022-07-02'

--No need to remove duplicates since aggregtion method is average
SELECT
    dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp,
    DeviceID,
    EventID,
    AVG(CONVERT(FLOAT,Parameter)) AS Average
FROM
    ASCEvents
WHERE
    EventID IN(400,503,502)
    AND TimeStamp >= @start
    AND TimeStamp < @end
GROUP BY
    dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0),
    DeviceID,
    EventID