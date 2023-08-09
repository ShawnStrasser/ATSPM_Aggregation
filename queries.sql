-- NOTES

-- Each query in this file gets parsed by Aggregations.py and stored in a dicitonary.
-- The first comment line above each query is the name of the query and will be the key stored in the dicitonary.
-- Queries are differentiated using the semi-colon

-- When a query is run by DuckDB and saved to a Python variable, it is saved as an unmaterialized view. 
-- Because the same query may be reused for different ATSPM's with different steps it's important to be able be able to change the primary table in the FROM clause
-- to be referenced by each step, so the FROM clause will reference the variable @table which will be replaced as needed in Aggregations.py
--;


-- detector_with_phase
-- Join Phase Number to each Detector Event
-- Rename parameter as Detector to avoid cofusion
SELECT raw_data.TimeStamp,
    raw_data.DeviceId, 
    raw_data.EventId, 
    raw_data.Parameter as Detector, 
    CAST(configs.Phase AS UTINYINT) AS Phase
FROM raw_data
JOIN configs ON 
    raw_data.DeviceId = configs.DeviceId 
    AND raw_data.Parameter = configs.Parameter
WHERE EventId IN(81,82);


-- impute_actuations
/* 
This is to fill in missing detector on/off events through interpolation
Sometimes detector on events (82) occur in rapic succession without an off event separating them,
    When the gap between two-in-a-row ON events is <= 2000 milliseconds,
    add an OFF event with timestamp matching the next ON event.
If for two-in-a-row ON events more than 2000 milliseconds apart, an OFF event is added half-way between them
For two-in-a-row OFF events, an ON event is added half-way between them regardless of the gap size
*/
WITH lagged as (
    SELECT *,
        LAG(EventId) OVER (PARTITION BY DeviceId, Detector ORDER BY TimeStamp) as PrevEventId,
        --LAG(TimeStamp) OVER (PARTITION BY DeviceId, Detector ORDER BY TimeStamp) as PrevTime,
        datediff('MILLISECOND', LAG(TimeStamp) OVER (PARTITION BY DeviceId, Detector ORDER BY TimeStamp), TimeStamp) AS PrevDiff
    FROM @table
),
interpolate_OFFS_rapid AS (
    SELECT TimeStamp, DeviceId, 81 AS EventId, Detector, Phase
    FROM lagged
    WHERE PrevDiff <= 2000
        and EventId = 82
        and PrevEventId = 82
),
interpolate_OFFs as (
    SELECT
        "TimeStamp" - INTERVAL (PrevDiff / 2) MILLISECOND as TimeStamp,
        DeviceId,
        81 as EventId,
        Detector,
        Phase
    FROM lagged
    WHERE PrevDiff > 2000
        and EventId = 82
        and PrevEventId = 82
),
interpolate_ONs AS (
    SELECT "TimeStamp" - INTERVAL (PrevDiff / 2) MILLISECOND as TimeStamp,
        DeviceId,
        82 AS EventId,
        Detector,
        Phase
    FROM lagged
    WHERE EventId = 81 AND PrevEventId = 81
),
combined AS (
    SELECT * FROM @table
    UNION ALL
    SELECT * FROM interpolate_OFFS_rapid
    UNION ALL
    SELECT * FROM interpolate_OFFs
    UNION ALL
    SELECT * FROM interpolate_ONs
),
--Now remove first row if it is an OFF event
ordered_rows AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY DeviceId, Detector ORDER BY TimeStamp) as row_num
    FROM combined
)
SELECT TimeStamp,
    DeviceId,
    EventId,
    Detector,
    Phase
FROM ordered_rows
WHERE NOT (row_num = 1 AND EventId = 81);




-- phase_with_detector_ByLane
-- Duplicate phase events for each unique detector
-- This is to be able to do group by operations later using the Detector field, so each detector has it's own phase events to be grouped with
-- Detector colum is the detector number for detector events AND associated phase events for that detector
WITH detectors AS (
    SELECT DISTINCT DeviceId,
        Detector,
        Phase FROM @table
),
phase AS (
    SELECT TimeStamp, 
        DeviceId, 
        EventId, 
        Parameter AS Phase 
    FROM raw_data WHERE EventId NOT IN (81, 82)
), 
duplicated_phase AS (
    SELECT TimeStamp, 
        DeviceId, 
        EventId, 
        Detector,
        Phase 
    FROM phase
    NATURAL JOIN detectors
)
SELECT * FROM duplicated_phase
UNION ALL
SELECT TimeStamp,
    DeviceId, 
    EventId, 
    Detector,
    Phase
FROM @table;


-- phase_with_detector_ByApproach
-- Combined phase and detector data without duplicating phase data (no need since detector/phases are combined already)
WITH 
phase AS (
    SELECT TimeStamp, 
        DeviceId, 
        EventId,
        0 as Detector,
        Parameter AS Phase 
    FROM raw_data WHERE EventId NOT IN (81, 82)
)
SELECT * FROM phase
UNION ALL
SELECT TimeStamp,
    DeviceId, 
    EventId, 
    Detector,
    Phase
FROM @table;




-- combine_detectors_ByApproach
-- This is for Approach-based Split Failures
-- Combines detector actuations for all detectors assigned to each phase
-- Detector state is determined as an OR condition, so it's on if ANY detectors are on
WITH with_values AS (
    SELECT *,
        CASE
            WHEN EventId=82 THEN 1
            WHEN EventId=81 THEN -1
        END as Value
    FROM @table
),
with_cumulative AS (
    SELECT *,
        SUM(Value) OVER (PARTITION BY DeviceId, Phase ORDER BY TimeStamp) as CumulativeSum
    FROM with_values
),
with_state AS (
    SELECT *,
        CASE
            WHEN CumulativeSum > 0 THEN 82
            ELSE 81
        END as State
    FROM with_cumulative
)
SELECT TimeStamp,
    DeviceId,
    State as EventId,
    0 as Detector,
    Phase
FROM with_state;




-- with_barrier
--Add 5 second after red time barrior for Split Failures
WITH red_time_barrier AS (
    SELECT 
        "TimeStamp" + INTERVAL '@variable1' SECOND as TimeStamp,
        DeviceId,
        11 as EventId,
        Detector,
        Phase
    FROM 
        @table 
    WHERE 
        EventId = 10
)
SELECT * FROM @table
UNION ALL
SELECT * FROM red_time_barrier;


-- with_cycle
-- Group by cycle and label events as occuring during green, yellow, or red
WITH step1 as (
    SELECT *,
        CASE WHEN EventId = 1 THEN 1 ELSE 0 END AS Cycle_Number_Mask,
        CASE WHEN EventId < 81 THEN EventId ELSE 0 END AS Signal_State_Mask,
        CASE WHEN EventId = 81 THEN 0 WHEN EventId =82 THEN 1 END AS Detector_State_Change
    FROM @table
),
step2 as (
    SELECT *,
        CAST(SUM(Cycle_Number_Mask) OVER (PARTITION BY DeviceId, Detector, Phase ORDER BY TimeStamp, EventId) AS UINTEGER) AS Cycle_Number,
        COUNT(Detector_State_Change) OVER (PARTITION BY DeviceId, Detector, Phase ORDER BY TimeStamp, EventId) AS Detector_Group
        FROM step1
)
SELECT TimeStamp,
    DeviceId, 
    EventId, 
    Detector,
    Phase,
    Cycle_Number,
    CAST(MAX(Signal_State_Mask) OVER (PARTITION BY DeviceId, Detector, Phase, Cycle_Number ORDER BY TimeStamp, EventId) AS UTINYINT) AS Signal_State,
    CAST(MAX(Detector_State_Change) OVER (PARTITION BY DeviceId, Detector, Phase, Detector_Group ORDER BY TimeStamp, EventId) AS BOOL) AS Detector_State--, Detector_Group, Detector_State_Mask
FROM step2;



-- time_diff
-- Calc time diff between events
WITH device_lag AS (
    SELECT *,
    LEAD(TimeStamp) OVER (PARTITION BY DeviceId, Detector, Phase ORDER BY TimeStamp, EventId) AS NextTimeStamp
    FROM @table
)
SELECT TimeStamp, 
    DeviceId, 
    EventId, 
    Detector,
    Phase,
    Cycle_Number, 
    Signal_State, 
    Detector_State, 
       CAST(DATEDIFF('MILLISECOND', TimeStamp, NextTimeStamp) AS INT) AS TimeDiff
FROM device_lag;


-- aggregate
-- Remove cycles with missing data, and Sum the detector on/off time over each phase state
WITH valid_cycles AS(
    SELECT DeviceId,
        Detector,
        Phase, 
        Cycle_Number
    FROM @table
    WHERE Cycle_Number > 0
    GROUP BY DeviceId, Detector, Phase, Cycle_Number
    HAVING 
        COUNT(CASE WHEN EventId = 8 THEN EventId END) = 1 --ensure only 1 yellow change event in cycle
        and COUNT(CASE WHEN EventId = 10 THEN EventId END) = 1 --ensure only 1 red change event in cycle
        --(cycles are deliniated by begin green, so they already are guaranteed to only have 1 green event)
        and COUNT(CASE WHEN Detector_State IS NULL THEN 1 END) = 0
        and COUNT(CASE WHEN TimeDIff IS NULL THEN 1 END) = 0
)
SELECT MIN(TimeStamp) as TimeStamp, 
    DeviceId, 
    Detector,
    Phase,
    Cycle_Number, 
    Signal_State, 
    Detector_State, 
    CAST(SUM(TimeDiff) AS INT) AS TotalTimeDiff
FROM @table
NATURAL JOIN valid_cycles
GROUP BY DeviceId, Detector, Phase, Cycle_Number, Signal_State, Detector_State;


-- final_SF
-- Final Steps
WITH step1 AS (
    SELECT MIN(TimeStamp) as TimeStamp, DeviceId, Detector, Phase, Cycle_Number,
        CAST(SUM(CASE WHEN Detector_State = TRUE AND Signal_State = 1 THEN TotalTimeDiff ELSE 0 END) AS INT) AS Green_ON,
        CAST(SUM(CASE WHEN Detector_State = FALSE AND Signal_State = 1 THEN TotalTimeDiff ELSE 0 END) AS INT) AS Green_OFF,
        CAST(SUM(CASE WHEN Detector_State = TRUE AND Signal_State = 10 THEN TotalTimeDiff ELSE 0 END) AS INT) AS Red_5_ON --red_5_OFF is just the inverse
    FROM @table
    GROUP BY DeviceId, Detector, Phase, Cycle_Number
)
SELECT TimeStamp, DeviceId, Detector, Phase,
    CAST(Green_ON + Green_OFF AS FLOAT) / 1000 AS Green_Time,
    CAST(Green_ON AS FLOAT) / (Green_ON + Green_OFF) AS Green_Occupancy,
    CAST(Red_5_ON AS FLOAT) / 5000 AS Red_Occupancy
FROM step1;

-- split_fail_binned
-- aggregated into 60 minute periods rather than by cycle
SELECT
    time_bucket(interval '60 minutes', TimeStamp) as Rounded_TimeStamp,
    DeviceId,
    Phase,
    AVG(Green_Time) as Green_Time,
    AVG(Green_Occupancy) as Green_Occupancy,
    AVG(Red_Occupancy) as Red_Occupancy,
    SUM(Split_Failure::USMALLINT)::USMALLINT as Split_Failure
FROM sf
GROUP BY Rounded_TimeStamp, DeviceId, Phase
ORDER BY Rounded_TimeStamp;




-- detector_with_phase_ON_ONLY
-- This is for arrival on green, or yellow/red actuation, where we only need detector ON events
-- It shift timestamps back to account for latency
-- And joins Phase Number to each Detector Event
-- Rename parameter as Detector to avoid cofusion
SELECT raw_data.TimeStamp - INTERVAL (@variable1 * 1000) MILLISECOND as TimeStamp,
    raw_data.DeviceId, 
    raw_data.EventId, 
    0 as Detector, --combine detectors by phase
    CAST(configs.Phase AS UTINYINT) AS Phase
FROM raw_data
JOIN configs ON 
    raw_data.DeviceId = configs.DeviceId 
    AND raw_data.Parameter = configs.Parameter
WHERE EventId = 82;


-- arrival_on_green
-- Transforms actuations into arrivals on green after they've been asssigned a signal state
WITH green AS (
    SELECT
        time_bucket(interval '@variable1 minutes', TimeStamp) as Rounded_TimeStamp,
        DeviceId,
        Phase,
        COUNT(*)::uint16 as Green_Actuations
    FROM @table
    WHERE
        EventId = 82
        and Signal_State = 1
     GROUP BY Rounded_TimeStamp, DeviceId, Phase
),
not_green AS(
    SELECT
        time_bucket(interval '@variable1 minutes', TimeStamp) as Rounded_TimeStamp,
        DeviceId,
        Phase,
        COUNT(*)::uint16 as Total_Actuations
    FROM @table
    WHERE
        EventId = 82
    GROUP BY Rounded_TimeStamp, DeviceId, Phase
)
SELECT
    g.Rounded_TimeStamp as TimeStamp,
    g.DeviceId,
    g.Phase,
    ng.Total_Actuations,
    Green_Actuations::float / Total_Actuations as Percent_AOG
FROM green as g
JOIN not_green ng
ON 
    g.Rounded_TimeStamp=ng.Rounded_TimeSTamp
    and g.DeviceId=ng.DeviceId
    and g.Phase=ng.Phase
ORDER BY TimeStamp;


-- red_offset
-- Select Begin Red Timestamps for each cycle
WITH begin_reds AS(
    SELECT
        TimeStamp as Red_TimeStamp,
        DeviceId, 
        Phase, 
        Cycle_Number
    FROM @table
    WHERE EventId = 10 and Cycle_Number > 0
    ORDER BY TimeStamp
),
renamed_table AS (
    SELECT 
        time_bucket(interval '15 minutes', Red_TimeStamp) as New_TimeStamp,
        DeviceId,
        Phase,
        Signal_State,
        --Red Offset is rounded to nearest half second. This was based on visual observation of the smoothness of rounding from 0.2 seconds to 1 second, seemed to look good
        ROUND(DATEDIFF('MILLISECOND', Red_TimeStamp, TimeStamp)::float * 2 / 1000) / 2 AS Red_Offset,
        COUNT(*) as Count
    FROM @table
    NATURAL JOIN begin_reds
    WHERE EventId=82
    GROUP BY
        New_TimeStamp,
        DeviceId,
        Phase,
        Signal_State,
        Red_Offset
)
SELECT 
    New_TimeStamp AS TimeStamp,
    DeviceId,
    Phase,
    Signal_State,
    Red_Offset,
    Count
FROM renamed_table
ORDER BY TimeStamp;
