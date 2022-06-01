# Class to download hi-res signal controller logs of various transformations from SQL Server

import pyodbc 
import pandas as pd
import datetime
import time

class GetData:
    '''
    Opens a connection to SQL Server, sends queries and returns data.
    Query options include:
        Aggregated Detector Actuations
        that's all for now...
    
    '''
    def __init__(self, server='ODOTDWQUERYPROD', database='STG_ITS_SQL_MAXVIEW_EVENTLOG'):
        self.today = datetime.datetime.now().date()
        self.connection = pyodbc.connect('Driver={SQL Server Native Client 11.0};' +\
            f'Server={server};Database={database};Trusted_Connection=yes;')
    
        # Dictionary of avaiable functions for downloading different data types
        self.dict = {'Actuations' : self.actuations, 'Communications' : self.communications, 'MaxOuts' : self.maxouts}

    def run_query(self, sql, index_col):
        return(pd.read_sql_query(sql, self.connection, index_col=index_col))

    def update_data(self, data_type):
        '''Automatically updates data from the Last_Run date through yesterday'''
        function = self.dict[data_type]
        # Read the Last_Run/Actuations date
        with open(f'Last_Run/{data_type}.txt', 'r') as file:
            last_run = datetime.datetime.strptime(file.read(), '%Y-%m-%d').date()
        # Update data 1 day at a time through yesterday
        while last_run < self.today - datetime.timedelta(days=1):
            start = str(last_run + datetime.timedelta(days=1))
            end = str(last_run + datetime.timedelta(days=2))
            print(f'\nWORKING ON: {data_type} {start} at: {datetime.datetime.now()}')
            function(start, end).to_parquet(f'{data_type}/{start}.parquet')
            # After successful run, update Last_Run/Actuations for next time
            with open('Last_Run/Actuations.txt', 'w') as file:
                file.write(start)
            # Now on to the next day
            last_run += datetime.timedelta(days=1)
            # Give the server a rest...
            time.sleep(30)

    def testing(self):
        index_col = ['TimeStamp', 'DeviceId', 'EventId']
        sql = 'select top 3 * from ASCEvents'
        return(self.run_query(sql, index_col))

    def actuations(self, start, end):
        '''Can be manually run to return actuations between start and end datetime parameters'''
        index_col = ['TimeStamp', 'DeviceID', 'MT']
        sql = f'''
        --Aggregate Detector Actuations
        SELECT
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp,
            DeviceID,
            Parameter AS MT,
            COUNT(*) AS Total
        FROM 
            (SELECT DISTINCT *
            FROM ASCEvents 
            WHERE EventID = 82 AND TimeStamp >= '{start}' AND TimeStamp < '{end}') q
        GROUP BY dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0), DeviceID, Parameter
        '''
        return(self.run_query(sql, index_col))

    def communications(self, start, end):
        index_col = ['TimeStamp', 'DeviceID', 'EventID']
        sql = f'''
        --Communications
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
            AND TimeStamp >= '{start}'
            AND TimeStamp < '{end}'
        GROUP BY
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0),
            DeviceID,
            EventID
        '''
        return(self.run_query(sql, index_col))

    def maxouts(self, start, end):
        index_col = ['TimeStamp', 'DeviceID', 'Phase']
        sql = f'''
        --MaxOuts
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
                        AND TimeStamp >= '{start}'
                        AND TimeStamp < '{end}') t
                GROUP BY
                    dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0),
                    DeviceID,
                    EventID,
                    Parameter
                ) q
                PIVOT( SUM(Total) FOR EventID IN([4], [5], [6], [43])) p
            ) z
        '''
        return(self.run_query(sql, index_col))

    def maxtime_faults(self, start, end):
        '''The query was too complex for EDW so I broke it into parts, final transformations done here'''
        date_index_col = ['TimeStamp']
        date_sql = f'''
        DECLARE @row DATETIME = '{start}'
        CREATE TABLE #T (TimeStamp DATETIME)
        WHILE @row < '{end}'
        BEGIN
        INSERT INTO #T
        VALUES (@row)
        SET @row = DATEADD(MINUTE, 15, @row)
        END;
        SELECT * FROM #T
        DROP TABLE #T
        '''
        on_index_col = ['TimeStamp', 'DeviceID', 'EventID']
        on_sql = f'''
        SELECT DISTINCT
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
            DeviceID,
            EventID,
            Parameter	
        FROM ASCEvents 
        WHERE EventID=87 AND TimeStamp >= '{start}' AND TimeStamp < '{end}'       
        '''
        all_index_col = ['TimeStamp', 'DeviceID', 'EventID']
        all_sql = f'''
        SELECT DISTINCT
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
            DeviceID,
            83 AS EventID, --don't care if it's a detector resored or detector off event. looking at both incase missing event
            Parameter
        INTO #ALL
        FROM ASCEvents 
        WHERE EventID IN(81, 83) AND TimeStamp >= @start AND TimeStamp < @end
            AND CONCAT(DeviceID * 100, Parameter) IN(SELECT DISTINCT CONCAT(DeviceID * 100, Parameter) FROM #ON)
        UNION ALL
        SELECT * FROM #ON        
        '''



        date_table = self.run_query(date_sql, date_index_col)

        #return()