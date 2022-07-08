--Aggregate MaxOuts in 15-minute Bins

--start/end variables are commented out because they are replaced when query is run from Python
--DECLARE @start AS DATETIME = '2022-07-01'
--DECLARE @end AS DATETIME = '2022-07-02'

SELECT
    *,
    IsNull((CONVERT(FLOAT, MaxOut + ForceOff)) / NullIf(MaxOut + ForceOff + GapOut,0),0) as Pct_MaxOut
FROM
    (SELECT
        TimeStamp,
        DeviceID,
        Parameter as Phase, 
        IsNull([4], 0) AS GapOut,
        IsNull([5],0) AS MaxOut,
        IsNull([6],0) AS ForceOff,
        IsNull([43],0) AS PhaseCall
    FROM
        (SELECT
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp,
            DeviceID,
            EventID,
            Parameter,
            COUNT(*) AS Total
        FROM
            (SELECT DISTINCT *
            FROM ASCEvents
            WHERE
                EventID IN(4,5,6,43)
                AND TimeStamp >= @start
                AND TimeStamp < @end) t
        GROUP BY
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0),
            DeviceID,
            EventID,
            Parameter
        ) q
        PIVOT( SUM(Total) FOR EventID IN([4], [5], [6], [43])) p
    ) z