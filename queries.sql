-- detector_with_phase
-- Join Phase Number to each Detector Event
-- Rename parameter as Detector to avoid cofusion
SELECT raw_data.TimeStamp,
    raw_data.DeviceId, 
    raw_data.EventId, 
    raw_data.Parameter as Detector, 
    CAST(sf_configs.Phase AS UTINYINT) AS Phase
FROM raw_data
JOIN sf_configs ON 
    raw_data.DeviceId = sf_configs.DeviceId 
    AND raw_data.Parameter = sf_configs.Parameter
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
    FROM detector_with_phase
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
    SELECT * FROM detector_with_phase
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
        Phase FROM impute_actuations
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
FROM impute_actuations;


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
FROM combine_detectors_ByApproach;




-- combine_detectors_ByApproach
-- This combines detector actuations for all detectors assigned to each phase
-- Detector state is determined as an OR condition, so it's on if ANY detectors are on
WITH with_values AS (
    SELECT *,
        CASE
            WHEN EventId=82 THEN 1
            WHEN EventId=81 THEN -1
        END as Value
    FROM impute_actuations
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
        "TimeStamp" + INTERVAL '5' SECOND as TimeStamp,
        DeviceId,
        11 as EventId,
        Detector,
        Phase
    FROM 
        phase_with_detector 
    WHERE 
        EventId = 10
)
SELECT * FROM phase_with_detector
UNION ALL
SELECT * FROM red_time_barrier;


-- with_cycle
-- Group by cycle and label events as occuring during green, yellow, or red
WITH step1 as (
    SELECT *,
        CASE WHEN EventId = 1 THEN 1 ELSE 0 END AS Cycle_Number_Mask,
        CASE WHEN EventId < 81 THEN EventId ELSE 0 END AS Signal_State_Mask,
        CASE WHEN EventId = 81 THEN 0 WHEN EventId =82 THEN 1 END AS Detector_State_Change
    FROM with_barrier
),
step2 as (
    SELECT *,
        CAST(SUM(Cycle_Number_Mask) OVER (PARTITION BY DeviceId, Detector, Phase ORDER BY TimeStamp, EventId) AS USMALLINT) AS Cycle_Number,
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
    FROM with_cycle
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


-- agg
-- Remove cycles with missing data, and Sum the detector on/off time over each phase state
WITH valid_cycles AS(
    SELECT DeviceId,
        Detector,
        Phase, 
        Cycle_Number
    FROM time_diff
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
FROM time_diff
NATURAL JOIN valid_cycles
GROUP BY DeviceId, Detector, Phase, Cycle_Number, Signal_State, Detector_State;


-- final
-- Final Steps
WITH step1 AS (
    SELECT MIN(TimeStamp) as TimeStamp, DeviceId, Detector, Phase, Cycle_Number,
        CAST(SUM(CASE WHEN Detector_State = TRUE AND Signal_State = 1 THEN TotalTimeDiff ELSE 0 END) AS INT) AS Green_ON,
        CAST(SUM(CASE WHEN Detector_State = FALSE AND Signal_State = 1 THEN TotalTimeDiff ELSE 0 END) AS INT) AS Green_OFF,
        CAST(SUM(CASE WHEN Detector_State = TRUE AND Signal_State = 10 THEN TotalTimeDiff ELSE 0 END) AS INT) AS Red_5_ON --red_5_OFF is just the inverse
    FROM agg
    GROUP BY DeviceId, Detector, Phase, Cycle_Number
)
SELECT TimeStamp, DeviceId, Detector, Phase,
    CAST(Green_ON + Green_OFF AS FLOAT) / 1000 AS Green_Time,
    CAST(Green_ON AS FLOAT) / (Green_ON + Green_OFF) AS Green_Occupancy,
    CAST(Red_5_ON AS FLOAT) / 5000 AS Red_Occupancy
FROM step1;

