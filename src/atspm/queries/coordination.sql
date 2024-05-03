--Coordination Events (Pattern change, Cycle lenght change, Actual cycle length, Actual cycle offset)
--Written in SQL for DuckDB. This is a jinja2 template, with variables inside curly braces.

SELECT
	TimeStamp,
	DeviceId,
	EventId::int16 AS EventId,
	Parameter::int16 AS Parameter,
	TIME_BUCKET(interval '{{bin_size}} minutes', TimeStamp) as '{{bin_size}}-Minute_TimeStamp'
FROM 
	{{from_table}}
WHERE
	EventId IN(131,132,316,318)
