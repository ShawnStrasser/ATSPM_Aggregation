--Arrival on Green
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.


WITH view1 AS (
    -- detector_with_phase_ON_ONLY
    -- This is for arrival on green, or yellow/red actuation, where we only need detector ON events
    -- It shift timestamps back to account for latency
    -- And joins Phase Number to each Detector Event
    -- Rename parameter as Detector to avoid cofusion
    SELECT d.TimeStamp - INTERVAL ({{latency_offset_seconds}} * 1000) MILLISECOND as TimeStamp,
        d.DeviceId, 
        d.EventId, 
        c.Phase::int16 AS Phase,
    FROM {{from_table}} AS d
    JOIN detector_config AS c ON 
        d.DeviceId = c.DeviceId 
        AND d.Parameter = c.Parameter
    WHERE EventId = 82 
        AND c.Function = 'Advance'
),

view2 AS (
    -- phase_with_detector_ByApproach
    -- Combined phase and detector data without duplicating phase data (no need since detector/phases are combined already)
    WITH 
    phase AS (
        SELECT TimeStamp, 
            DeviceId, 
            EventId,
            Parameter::int16 AS Phase 
        FROM {{from_table}}
        WHERE EventId IN (1, 8, 10)
    )
    SELECT * FROM phase
    UNION ALL
    SELECT * FROM view1
),

view3 AS (
    -- with_cycle
    -- Group by cycle and label events as occuring during green, yellow, or red
    WITH step1 as (
        SELECT *,
            CASE WHEN EventId = 1 THEN 1 ELSE 0 END AS Cycle_Number_Mask,
            CASE WHEN EventId < 81 THEN EventId ELSE 0 END AS Signal_State_Mask,
            CASE WHEN EventId = 81 THEN 0 WHEN EventId =82 THEN 1 END AS Detector_State_Change
        FROM view2
    ),
    step2 as (
        SELECT *,
            CAST(SUM(Cycle_Number_Mask) OVER (PARTITION BY DeviceId, Phase ORDER BY TimeStamp, EventId) AS UINTEGER) AS Cycle_Number,
            COUNT(Detector_State_Change) OVER (PARTITION BY DeviceId, Phase ORDER BY TimeStamp, EventId) AS Detector_Group
            FROM step1
    )
    SELECT TimeStamp,
        DeviceId, 
        EventId, 
        Phase,
        Cycle_Number,
        CAST(MAX(Signal_State_Mask) OVER (PARTITION BY DeviceId, Phase, Cycle_Number ORDER BY TimeStamp, EventId) AS UTINYINT) AS Signal_State,
        CAST(MAX(Detector_State_Change) OVER (PARTITION BY DeviceId, Phase, Detector_Group ORDER BY TimeStamp, EventId) AS BOOL) AS Detector_State
    FROM step2
),

view4 AS (
    -- arrival_on_green
    -- Transforms actuations into arrivals on green after they've been asssigned a signal state
    WITH green AS (
        SELECT
            time_bucket(interval '{{bin_size}} minutes', TimeStamp) as Rounded_TimeStamp,
            DeviceId,
            Phase,
            COUNT(*)::int16 as Green_Actuations
        FROM view3
        WHERE
            EventId = 82
            and Signal_State = 1
        GROUP BY Rounded_TimeStamp, DeviceId, Phase
    ),
    not_green AS(
        SELECT
            time_bucket(interval '{{bin_size}} minutes', TimeStamp) as Rounded_TimeStamp,
            DeviceId,
            Phase,
            COUNT(*)::int16 as Total_Actuations
        FROM view3
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
)

SELECT * FROM view4