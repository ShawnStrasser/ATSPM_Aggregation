# Class to download hi-res signal controller logs of various transformations from SQL Server

import pyodbc 
import pandas as pd
import datetime
import time
from statsmodels.tsa.seasonal import STL
import glob

# Extra modules for Get_RITTIS_DATA()
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import keyring
import pyperclip
import zipfile
import os
import shutil
from pathlib import Path


class Get_SQL_Data:
    '''
    Opens a connection to SQL Server, sends queries and returns data.
    Query options included are included in the dictionary "self.dict" (Communications, Actuations, MaxOuts)
    
    '''
    def __init__(self, server='ODOTDWQUERYPROD', database='STG_ITS_SQL_MAXVIEW_EVENTLOG'):

        self.today = datetime.datetime.now().date()
        self.connection = pyodbc.connect('Driver={SQL Server Native Client 11.0};' +\
            f'Server={server};Database={database};Trusted_Connection=yes;')
    
        # Dictionary of avaiable functions for downloading different data types
        self.dict = {
            'Actuations' : self.actuations,
            'Communications' : self.communications,
            'MaxOuts' : self.maxouts,
            'MaxTimeFaults' : self.maxtime_faults
            }

    def run_query(self, sql, index_col):
        # Turn off any SQL warnings that sometimes cause problems for Pandas
        sql = f'SET NOCOUNT ON; SET ANSI_WARNINGS OFF; {sql}'
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
            with open(f'Last_Run/{data_type}.txt', 'w') as file:
                file.write(start)
            # Now on to the next day
            last_run += datetime.timedelta(days=1)
            # Give the server a rest...
            time.sleep(2)

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
        index_col = ['TimeStamp', 'DeviceID']
        sql = f'''
        DECLARE @start AS DATETIME = '{start}';
        DECLARE @end AS DATETIME = '{end}';
        -----------------DATE TABLE---------------------------
        DECLARE @row DATETIME = @start
        CREATE TABLE #T (TimeStamp DATETIME)
        WHILE @row < @end
        BEGIN
        INSERT INTO #T
        VALUES (@row)
        SET @row = DATEADD(MINUTE, 15, @row)
        END;
        -----------------ON TABLE---------------------------
        --All the shorted loop events
        SELECT DISTINCT
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
            DeviceID,
            EventID,
            Parameter	
        INTO #ON
        FROM ASCEvents 
        WHERE EventID=87 AND TimeStamp >= @start AND TimeStamp < @end
        --and this table of distinct deviceid/detector pairs helps makes things fast as a filter in next step
        SELECT DISTINCT DeviceID as D, Parameter as P INTO #List FROM #ON
        -----------------ALL TABLE---------------------------
        --Union the detector off/restored events
        SELECT DISTINCT
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
            DeviceID,
            83 AS EventID, --don't care if it's a detector restored or detector off event. looking at both incase missing event
            Parameter
        INTO #ALL
        FROM ASCEvents
        JOIN #List ON #List.D = ASCEvents.DeviceId AND #List.P = ASCEvents.Parameter
        WHERE EventID IN(81, 83) AND TimeStamp >= @start AND TimeStamp < @end
        UNION ALL
        SELECT * FROM #ON
        -----------------JOIN TABLE---------------------------
        --join date table, ON, and OFF tables
        SELECT DISTINCT #T.TimeStamp, DeviceID, Parameter
        INTO #D
        FROM #T, #ALL

        SELECT #D.TimeStamp, #D.DeviceId, EventID, #D.Parameter
        INTO #FINAL
        FROM #D
        left join #ALL
        ON #ALL.TimeStamp = #D.TimeStamp and #ALL.DeviceID=#D.DeviceID and #ALL.Parameter=#D.Parameter
        ORDER BY DeviceID, Parameter, TimeStamp

        SELECT 
            TimeStamp, 
            DeviceID,
            CASE WHEN EventID IS NULL THEN 
            (SELECT TOP 1 EventID FROM #FINAL WHERE TimeStamp < F.TimeStamp AND EventID IS NOT NULL AND DeviceID=F.DeviceId AND Parameter=F.Parameter ORDER BY TimeStamp DESC) --OVER (PARTITION BY F.DeviceID)
            ELSE EventID 
            END AS EventID,
            Parameter
        INTO #STUCKON
        FROM #FINAL F
        -----------------FINAL RESULTS---------------------------
        --Combine Stuck On events with Erratic events
        SELECT * FROM #STUCKON
        WHERE EventID=87
        UNION ALL
        SELECT DISTINCT
            dateadd(minute, datediff(minute,0,TimeStamp)/15 * 15, 0) as TimeStamp, 
            DeviceID,
            EventID, 
            Parameter
        FROM ASCEvents
        WHERE EventID=88 AND TimeStamp >= @start AND TimeStamp < @end
        DROP TABLE #ALL
        DROP TABLE #T
        DROP TABLE #List
        DROP TABLE #D
        DROP TABLE #FINAL
        DROP TABLE #ON
        DROP TABLE #STUCKON
        '''
        return(self.run_query(sql, index_col))


class TransformData:
    '''
    Apply time series decomposition to find statistical anomalies
    '''
    pass


class Decompose():
    pass


class Get_RITIS_Data():
    '''
    Download Data using RITIS Massive Data Downloader.
    Uses Edge browser
    '''
    def __init__(self, driver_path="C:\Program Files (x86)\msedgedriver.exe"):
        self.today = datetime.datetime.now().date()
        self.driver_path=driver_path
        self.url='https://pda.ritis.org/suite/download/'
        self.url2='https://pda.ritis.org/suite/my-history/'
        self.driver=webdriver.Edge(self.driver_path)
        # Define xpath notations to be used
        self.select_segment_codes = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[2]/div[2]/div/div[2]/div[1]/div[3]/div/div'
        self.XD_codes_text = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[2]/div[2]/div/div[2]/div[2]/div[3]/div/div/div/div/textarea'
        self.select_add_segments = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[2]/div[2]/div/div[2]/div[2]/div[3]/div/div/div/div/div[2]/div'
        self.select_start_date = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/input'
        self.select_end_date = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[3]/div[1]/div/div/div/input'
        self.select_seconds = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[7]/div[2]/div/div[1]/label/span'
        self.select_15_min = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[9]/div[2]/div/div[4]/label/span'
        self.select_title = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[10]/div[2]/input'
        self.select_email = '/html/body/main/div/div[1]/div/div[1]/form/ol/li[11]/div[2]/label/span'
        self.select_submit_button = '/html/body/main/div/div[1]/div/div[1]/form/button'
        self.select_history_link = '/html/body/main/div/div[2]/div/div[1]/div[2]/div/p/a'
        self.select_status = '/html/body/main/div/div[1]/div[2]/div[2]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/div/div/div/div/span/div'
        self.select_download = '/html/body/main/div/div[1]/div[2]/div[2]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div[3]/div/div/div/div/div/div/div/a'
        # Load XD segments
        with open('RITIS_Data_Download/XD_segments.txt', 'r') as file:
            self.xd_segments = file.read()

    def week_of_month(self, date):
        """ Returns the week of the month for the specified date.
        Note, for datetime.weekday(), Monday=0, Sunday=6
        this converts it so Sun=0: (.weekday() + 1) % 7
        So this function treats Sunday as first day of week
        """
        first_day = date.replace(day=1)
        dom = date.day
        adjusted_dom = dom + (first_day.weekday() + 1) % 7
        return (adjusted_dom - 1) // 7 + 1

    def date_picker(self, start, delta=0):
        '''Generates xpaths for date picker
        Dates provided are inclusive, so delta=0 will be a single day'''
        end = start + datetime.timedelta(days=delta)
        left = 1 #left/begin date picker
        right = 3 #right/end date picker
        year_num_start = start.year
        year_num_end = end.year
        month_num_start = start.month - 1
        month_num_end = end.month - 1
        week_num_start = self.week_of_month(start) #the week number of the month
        week_num_end = self.week_of_month(end)
        day_num_start = (start.weekday() + 1) % 7 + 1 #DAY OF THE WEEK (CONVERT SO Sunday=1, Sat=7)
        day_num_end = (end.weekday() + 1) % 7 + 1
        #xpaths to select start/end year, month, and day
        start_year_xpath = f"/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[{left}]/div[1]/div/div[2]/div/div[2]/div[1]/div[2]/div[2]/select//*[contains(text(), '{year_num_start}')]"
        end_year_xpath = f"/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[{right}]/div[1]/div/div[2]/div/div[2]/div[1]/div[2]/div[2]/select//*[contains(text(), '{year_num_end}')]"
        start_month_xpath = f"/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[{left}]/div[1]/div/div[2]/div/div[2]/div[1]/div[2]/div[1]/select//*[@value={month_num_start}]"
        end_month_xpath = f"/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[{right}]/div[1]/div/div[2]/div/div[2]/div[1]/div[2]/div[1]/select//*[@value={month_num_end}]"
        start_day_xpath = f'/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[{left}]/div[1]/div/div[2]/div/div[2]/div[2]/div[{week_num_start}]/div[{day_num_start}]'
        end_day_xpath = f'/html/body/main/div/div[1]/div/div[1]/form/ol/li[3]/div[2]/div/div[1]/div/div[1]/div[{right}]/div[1]/div/div[2]/div/div[2]/div[2]/div[{week_num_end}]/div[{day_num_end}]'
        return start_year_xpath, start_month_xpath, start_day_xpath, end_year_xpath, end_month_xpath, end_day_xpath
    
    def get_credentials(self):
        try:
            email = keyring.get_password('RITIS', 'email')
            password = keyring.get_password('RITIS', email)
            assert(email != None)
            assert(password != None)
            print('Credentials retreived')
        except Exception:
            email = input('\n\nEnter email for RITIS account:\n')
            password = input('Password: ')
            save = input("\nStore email/password locally for later? Manage them later using the Windows Credential Manager.\nStore email/password in Credential Manger? Type YES or NO: ")
            if save.lower() == 'yes':
                keyring.set_password('RITIS','email', email)
                keyring.set_password('RITIS', email, password)
                print('\n\Email and password saved in Credential Manager under RITIS.')
                print('There are two credentials with that name, one used to look up email, the other uses email to look up password.')
        return email, password 

    def log_in(self, url):
        username, password = self.get_credentials()
        self.driver.get(url)
        try:
            print('Logging in')
            username_form = self.select(css="input[name='username']")
            password_form = self.select(css="input[name='password']")
            username_form.clear()
            password_form.clear()
            username_form.send_keys(username)
            password_form.send_keys(password)
            self.select(css="input[type='submit']").click()
        except Exception:
            print('Already logged in (or login failed).')


    def select(self, wait=10, xpath=None, css=None, sleep=2):
        time.sleep(sleep)
        if xpath is not None:
            return WebDriverWait(self.driver, wait).until(EC.element_to_be_clickable((By.XPATH, xpath)))

        else:
            return WebDriverWait(self.driver, wait).until(EC.element_to_be_clickable((By.CSS_SELECTOR, css)))

    def submit_job(self, date, delta=0):
        self.log_in(url='https://pda.ritis.org/suite/download/')
        # Naviate to Segment Codes
        self.select(xpath=self.select_segment_codes).click()
        XD_Codes = self.select(xpath=self.XD_codes_text)
        XD_Codes.click()
        # Copy segments to clipboard and paste, typing them in takes too long!
        pyperclip.copy(self.xd_segments)
        XD_Codes.send_keys(Keys.CONTROL, 'v')  #Paste using Ctrl+V
        # Click add segments
        self.select(xpath=self.select_add_segments).click()
        # Select Dates. First generate xpaths for each date element
        start_year_xpath, start_month_xpath, start_day_xpath, end_year_xpath, end_month_xpath, end_day_xpath = self.date_picker(date, delta)
        # Select start dates
        self.select(xpath=self.select_start_date).click()
        self.select(xpath=start_year_xpath).click()
        self.select(xpath=start_month_xpath).click()
        self.select(xpath=start_day_xpath).click()
        # Select end dates
        self.select(xpath=self.select_end_date).click()
        self.select(xpath=end_year_xpath).click()
        self.select(xpath=end_month_xpath).click()
        self.select(xpath=end_day_xpath).click()
        # Select 15-min aggregation
        self.select(xpath=self.select_15_min).click()
        # Add a title
        file_name = str(date)
        self.select(xpath=self.select_title).send_keys(file_name)
        # Uncheck send email
        self.select(xpath=self.select_email).click()
        # Submit the requst
        self.select(xpath=self.select_submit_button).click()
        return file_name # to be used as file name for retreiving download
        
    def download_data(self, file_name, url='https://pda.ritis.org/suite/my-history/'):
        check_download = str(Path.home() / f'Downloads//{file_name}.zip')
        self.log_in(url)
        # Check Status, download to PC when done
        try:
            print('Waiting for job to be ready')
            while self.select(xpath=self.select_status, sleep=60, wait=10).text == 'Pending':
                time.sleep(5)
            print('Downloading to download folder')
            self.select(xpath=self.select_download).click()
        except Exception:
            print('Download Failed')
            exit()
        wait_time = 0
        while os.path.isfile(check_download) is False:
            wait_time += 15
            if wait_time > 600:
                exit() # if download takes longer than 10 min, exit program
            else:
                time.sleep(15)
    
    def clean_up(self, file_name):
        download_folder = str(Path.home() / "Downloads")
        # Extract contents of the zipped folder
        with zipfile.ZipFile(f'{download_folder}/{file_name}.zip', 'r') as zip_ref:
            zip_ref.extractall(f'{download_folder}/{file_name}')
        time.sleep(2)
        df = pd.read_csv(f'{download_folder}/{file_name}/{file_name}.csv', index_col=['xd_id', 'measurement_tstamp'], parse_dates=['measurement_tstamp'])
        df.index.names = ['XD', 'TimeStamp']
        # Check that the input date matches date on actual data!
        # This check lets us know if something has changed or gone wrong to ensure data is as expected
        if str(df.index[1][1].date()) == file_name:
            df.to_parquet(f'TravelTime/{file_name}.parquet')
            print('Saved parquet file to folder')
        else:
            print('ERROR!!! Date of data timestamp did not match input date! Debug date picker!')
            print(f'Data date = {str(df.index[1][1].date())} and input date = {file_name}')
            exit()
        time.sleep(3)
        shutil.rmtree(f'{download_folder}/{file_name}')
        os.remove(f'{download_folder}/{file_name}.zip')
        
    def main(self, date, delta=0):
        '''Runs all functions to download RITIS data to local folder
        date must be datetime.date object
        delta is number of days after date'''
        file_name = self.submit_job(date, delta)
        self.download_data(file_name)
        self.clean_up(file_name)

    def update_data(self):
        '''Automatically updates data from the Last_Run date through yesterday'''
        with open('Last_Run/RITIS.txt', 'r') as file:
            last_run = datetime.datetime.strptime(file.read(), '%Y-%m-%d').date()
        # Update data 1 day at a time through yesterday
        while last_run < self.today - datetime.timedelta(days=1):
            start = last_run + datetime.timedelta(days=1)
            print(f'\nWORKING ON: RITIS {str(start)} at: {datetime.datetime.now()}')
            self.main(date=start)
            # After successful run, update Last_Run/RITIS for next time
            with open('Last_Run/RITIS.txt', 'w') as file:
                file.write(str(start))
            # Now on to the next day
            last_run += datetime.timedelta(days=1)
            time.sleep(2)
        self.driver.quit() # quits the browser



class Analytics():

    def __init__(self):
        self.today = datetime.datetime.now().date()

    def load_data(self, folder, date=str(datetime.datetime.now().date()), num_days = 35):
        '''Loads raw data from selected folder and date range'''
        # Get list of file names
        file_names = sorted(glob.glob(f"{folder}/*.parquet"))
        file_dates = [name.split('\\')[1].split('.')[0] for name in file_names]
        file_dates = [datetime.datetime.strptime(date, '%Y-%m-%d').date() for date in file_dates]
        end = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        start = end - datetime.timedelta(days = num_days)
        files = [file for file, date in zip(file_names, file_dates) if date > start and date <= end]
        #print(files)
        data = [pd.read_parquet(file) for file in files]
        return(pd.concat(data, axis=0))

    def normalize_by_group(self, df, group, column):
        '''Returns indexd colum of normalized valuse, using Vectorization for quick calculation
        Assumes group is in index
        Takes a dataframe, a list of columns for which to group the data, and the column for which the normalization is based on
        adapted from https://stackoverflow.com/questions/26046208/normalize-dataframe-by-group'''
        df = df[column]
        groups = df.groupby(level=group)
        # computes group-wise mean/std, then auto broadcasts to size of group chunk
        mean = groups.transform("mean")
        std = groups.transform("std")
        return (df[mean.columns] - mean) / std

    def add_seg_groups(self, df, index='XD', columns = None, file='//scdata2/signalshar/Data_Analysis/Data/Performance/dim_signals_XD.parquet'):
        if columns is None:
            columns = ['group', 'District']
        # Load dimension table with groups
        dim = pd.read_parquet(file)[columns]
        # drop XD duplicates (same one can be assigned to multiple signals)
        dim = dim[~dim.index.duplicated()]
        df_index = df.index.get_level_values(index)
        for column in columns:
            df[column] = dim.loc[df_index][column].values
        return df.reset_index().set_index(['TimeStamp'] + [index] + columns)


    def find_anomalies(self, df, z=4):
        # sourcery skip: default-mutable-arg
        '''Calculates individual z-scores from group z-scores'''

        df['Resid_Z'] = self.normalize_by_group(df=df, group=['XD'], column=['Resid'])
        df['Resid_District_Z'] = self.normalize_by_group(df=df, group=['TimeStamp', 'District'], column=['Resid_Z'])
        
        # Classify Local Anomalies. If |Resid_Z| > 4 and |Resid_District_Z| < 4, then Global, If |Resid_Z| > 4 and |Resid_Distric_Z| > 4, then Local 
        df['LocalAnomaly'] = df.apply(lambda row: abs(row['Resid_District_Z']) > z and abs(row['Resid_Z']) > z, axis=1) #need to vectorize
        
        df['Resid_Corridor_Z'] = self.normalize_by_group(df=df, group=['TimeStamp', 'group'], column=['Resid_Z'])
        df['LocalAnomaly_Within_Corridor'] = df.apply(lambda row: abs(row['Resid_Corridor_Z']) > z and abs(row['Resid_Z']) > z, axis=1) #need to vectorize
        
        #defunct too slow
        # Finding local group anomalies is very slow, 10 min for 2 weeks
        #def group_Z(x):
        #    return 0 if len(x) < 5 else (x-x.mean())/ x.std()
        #r['Resid_group_Z'] = r.groupby(level=['TimeStamp', 'group'])['Resid_Z'].transform(lambda x: group_Z(x))
        return df        
    
    def decompose(self, df, field='travel_time_minutes', freq='15min', smooth='7d',min_periods=50, time_min='6:00', time_max='19:45'):
        '''Robust decomposition using median
        dataframe input must have indexs ID/Attribute, and TimeStamp'''

        # Temp step
        #df = df.loc[[1237049582, 1236860943], slice(None),:]
        df = df[[field]]
        # Filter by Time of Day
        df = df.reset_index().set_index('TimeStamp').between_time(time_min, time_max).reset_index().set_index(['TimeStamp','XD'])
        df.index.levels[1].freq = freq
        print(f'Frequency set at {df.index.levels[1].freq}')
        group = df.reset_index(level=1).groupby('XD')
        df = df.reset_index()
        df['RollingMedian'] = group.transform(lambda x: x.rolling(smooth, min_periods=min_periods).median()).reset_index()[field]
        df['Detrend'] = df.travel_time_minutes - df.RollingMedian
        df['DayPeriod'] = ((df['TimeStamp'].dt.hour * 60 + df['TimeStamp'].dt.minute)/15 + 1).astype(int)
        df['WeekPeriod'] = df['TimeStamp'].dt.isocalendar().day
        df['SeasonDay'] = df.groupby(['XD', 'DayPeriod'])['Detrend'].transform('median')
        df['DeSeason'] = df.Detrend - df.SeasonDay
        df['SeasonWeek'] = df.groupby(['XD', 'WeekPeriod', 'DayPeriod'])['DeSeason'].transform('median')
        df['Resid'] = df.DeSeason - df.SeasonWeek
        df = df.set_index(['XD', 'TimeStamp'])[['travel_time_minutes', 'RollingMedian', 'SeasonDay', 'SeasonWeek', 'Resid']]
        return df.dropna()


    def global_anomaly(self):
        pass



