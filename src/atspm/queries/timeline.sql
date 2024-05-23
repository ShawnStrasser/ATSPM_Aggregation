WITH Transition1 AS 
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID ORDER BY TimeStamp, Parameter DESC) AS EndTime
	FROM {{from_table}}               
	WHERE EventId = 150 AND Parameter IN (0, 1, 2, 3, 4)
	),
Transition AS
	(
	SELECT *
	FROM Transition1
	WHERE Parameter IN (2, 3, 4) AND EndTime IS NOT NULL AND DATE_DIFF('second', TimeStamp, EndTime) < 3600
	),
Preempt1 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp, EventId) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (102, 104)
	),
Preempt AS
	(
	SELECT *
	FROM Preempt1
	WHERE EventID = 102 AND EndTime IS NOT NULL AND DATE_DIFF('second', TimeStamp, EndTime) < (3600 * 4)
	),
TSP1 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp, EventId) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (112, 115)
	),
TSP AS 
	(
	SELECT *
	FROM TSP1
	WHERE EventID = 112 AND EndTime IS NOT NULL AND DATE_DIFF('second', TimeStamp, EndTime) < 180 
	),
TSP_adjust_1 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp, EventId) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (113, 114)
	),
TSP_adjust AS
	(
	SELECT *
	FROM TSP_adjust_1
	WHERE EventID = 113 AND EndTime IS NOT NULL AND DATE_DIFF('second', TimeStamp, EndTime) < 120
	),
Fault1 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (83, 87, 88)
	),
Fault AS
	(
	SELECT *
	FROM Fault1
	WHERE EventID IN (87, 88) AND EndTime IS NOT NULL 
	  AND DATE_DIFF('second', TimeStamp, EndTime) > 0 
	  AND DATE_DIFF('second', TimeStamp, EndTime) < (3600 * 12)
	),
Ped2 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (21, 23)
	),
Ped3 AS
	(
	SELECT *
	FROM Ped2
	WHERE EventId = 21 AND DATE_DIFF('second', TimeStamp, EndTime) < 120
	),
oPed2 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (67, 65)
	),
oPed3 AS
	(
	SELECT * 
	FROM oPed2
	WHERE EventId = 67 AND DATE_DIFF('second', TimeStamp, EndTime) < 120
	),
Coord AS
	( 
	SELECT *,
	TimeStamp + INTERVAL ({{cushion_time}}) SECOND AS EndTime
	FROM {{from_table}}
	WHERE EventId = 131
	),
Splits AS
	(
	SELECT {{from_table}}.TimeStamp + INTERVAL ({{from_table}}.Parameter * -1) SECOND AS TimeStamp,
	{{from_table}}.DeviceID, 
	{{from_table}}.EventID,
	{{from_table}}.Parameter,
	{{from_table}}.TimeStamp AS EndTime
	FROM {{from_table}}
	WHERE {{from_table}}.EventId IN (300, 301, 302, 303, 304, 305, 306, 307)
	),
PhaseCall1 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (43, 44)
	),
PhaseCall AS
	(
	SELECT PhaseCall1.TimeStamp, PhaseCall1.DeviceID, PhaseCall1.EventID, 
	       PhaseCall1.Parameter, PhaseCall1.EndTime
	FROM PhaseCall1
	WHERE PhaseCall1.EventId = 43
	), 
FYA1 AS
	(
	SELECT *,
	LEAD(TimeStamp) OVER (PARTITION BY DeviceID, Parameter ORDER BY TimeStamp) AS EndTime
	FROM {{from_table}}               
	WHERE EventId IN (32, 33)
	),
FYA AS 
	(
	SELECT FYA1.TimeStamp, FYA1.DeviceID, FYA1.EventID, 
	       FYA1.Parameter, FYA1.EndTime  
	FROM FYA1
	WHERE FYA1.EventId = 32
	),
categories AS (
  SELECT * FROM (VALUES
    (150, 'Transition'),
    (102, 'Preempt'),
    (87, 'Fault'),
    (88, 'Fault'),
    (21, 'Ped Service'),
    (131, 'Pattern Change'),
    (99, 'Cycle Fault'),
    (300, 'Splits'),
    (301, 'Splits'),
    (302, 'Splits'),
    (303, 'Splits'),
    (304, 'Splits'),
    (305, 'Splits'),
    (306, 'Splits'),
    (307, 'Splits'),
    (43, 'Phase Call'),
    (32, 'FYA'),
    (67, 'Overlap Ped'),
    (112, 'TSP Call'),
    (113, 'TSP Adjustment')
  ) AS t(EventId, Category)
)

SELECT
  TIME_BUCKET(INTERVAL '{{bin_size}} minutes', t.TimeStamp) AS TimeStamp,
  t.DeviceID::int16 AS DeviceId,
  t.EventID AS EventId, --to be dropped after extracting unmatched events
  t.Parameter AS Parameter, --same as above
  t.TimeStamp AS StartTime,
  t.EndTime,
  DATE_DIFF('millisecond', t.TimeStamp, t.EndTime)::FLOAT / 1000 AS Duration,
  --c.Category AS Category_Basic,
  CASE
    WHEN c.Category IN ('Ped Service', 'FYA', 'Phase Call', 'Preempt', 'TSP Call', 'TSP Adjustment', 'Overlap Ped', 'Pattern Change') 
      THEN c.Category || ' ' || t.Parameter
    WHEN c.Category = 'Splits'
      THEN c.Category || ' ' || (t.EventId - 299)  
    WHEN c.Category = 'Transition' THEN
      CASE t.Parameter
        WHEN 3 THEN 'Transition Shortway'
        WHEN 2 THEN 'Transition Longway'
        WHEN 4 THEN 'Transition Dwell'
        ELSE c.Category
      END
    ELSE c.Category
  END AS Category
FROM 
(
  SELECT * FROM Transition
  --WHERE EndTime > TimeStamp
  UNION ALL
  SELECT * FROM Preempt
  UNION ALL 
  SELECT * FROM TSP
  UNION ALL
  SELECT * FROM TSP_adjust
  UNION ALL
  SELECT * FROM Fault  
  UNION ALL
  SELECT * FROM Ped3
  UNION ALL 
  SELECT * FROM oPed3
  UNION ALL
  SELECT * FROM Coord
  UNION ALL 
  SELECT * FROM Splits
  UNION ALL
  SELECT * FROM PhaseCall
  --WHERE EndTime > TimeStamp 
  UNION ALL
  SELECT * FROM FYA 
) t
LEFT JOIN categories c ON t.EventId = c.EventId