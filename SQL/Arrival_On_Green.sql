/*
NOTE need to fix the problem at begin and end of the day, when the start or end green phase happens on the previous or next day!
Actually just throw that data out!


-- https://ops.fhwa.dot.gov/publications/fhwahop13031/chap4.htm#:~:text=Platoon%20ratio%20is%20a%20measure,et%20al.%2C%202007).

Platoon Ratio = %Arrival_on_Green * Cycle_Length / Phase_Split


*/


--Variables are commented out because Python script will replace them
/*
DECLARE @TSSU as VARCHAR(5) = '03008';
DECLARE @Phase as INT= 1;
DECLARE @Det AS INT = 1;
DECLARE @start DATETIME ='2022-07-12';
DECLARE @end DATETIME  = '2022-07-15';*/

	--Run query!--
--use TSSU to look up device id
/*DECLARE @DeviceID AS INT 
SET @DeviceID= (
SELECT GroupableElements.ID
FROM [MaxView_1.9.0.744].[dbo].[GroupableElements]
WHERE Right(GroupableElements.Number,5) = @TSSU)
*/

DECLARE @BinSize AS INT = 15;

	--Run query!--
--within this CTE there are multiple steps to take
;WITH 

--Green: phase green on and off events, with start and end timestamps
Green as (
	SELECT Lag(TimeStamp) OVER (PARTITION BY Parameter ORDER BY TimeStamp) as BeginGreen, TimeStamp as EndGreen, EventID
	FROM ASCEvents
	WHERE DeviceId=@DeviceID and (EventID=0 or EventID=7) and Parameter=@Phase and 
	(TimeStamp BETWEEN @start and @end)
),

--Filter Green to only include the end green events. this results in a table with start timestamp and end timestamp for each green service
FilteredGreen as (
	SELECT BeginGreen, EndGreen, CONVERT(DATE,BeginGreen) as GreenDate FROM Green WHERE EventID=7
),

--Actuations: all detector on events for the specified detector, with a rounded timestamp called TimePeriod
Actuations as (
	SELECT TimeStamp as ArrivalTime, dateadd(minute, datediff(minute,0,TimeStamp)/@BinSize * @BinSize, 0) as TimePeriod, CONVERT(DATE,TimeStamp) as ActuationDate
	FROM ASCEvents
	WHERE DeviceID=@DeviceID and EventID=82 and Parameter=@Det and
	(TimeStamp BETWEEN @start and @end)
), 

--Total: Counts total actuations in each time period. This is used in denominator to calculate percent in final select statement. 
Total as (
	SELECT TimePeriod, Count(TimePeriod) as TotalActuations FROM Actuations Group By TimePeriod
),

--AOG_Table: Counts the number of actuations in each time period which occurred between the BeginGreen and EndGreen for the phase
--The ActuationDate=GreenDate in the where clause partitions calcualtions by date, makes it go faster. however i wonder why not just use a join? can't remember ;?
AOG_Table as (
	SELECT TimePeriod, COUNT(TimePeriod) as AOG
	FROM FilteredGreen, Actuations
	WHERE ActuationDate=GreenDate and (ArrivalTime Between BeginGreen and EndGreen)
	Group By TimePeriod
)

	SELECT
		Total.TimePeriod,
		@TSSU as TSSU,
	    @Phase as Phase,
    	@Det as MT,
		AOG as GreenActuations,
		TotalActuations
	FROM Total
	LEFT JOIN AOG_Table ON AOG_Table.TimePeriod=Total.TimePeriod
	--ORDER BY TimePeriod
	
