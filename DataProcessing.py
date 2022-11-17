# Class to download hi-res signal controller logs of various transformations from SQL Server

import pyodbc 
import pandas as pd
import numpy as np
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
#from msedge.selenium_tools import Edge, EdgeOptions #needed to pip install msedge-selenium-tools selenium==3.141
import keyring
import pyperclip
import zipfile
import os
import shutil
from pathlib import Path


class Get_SQL_Data():
    '''
    Opens a connection to SQL Server, sends queries and returns data.
    Query options included are included in the dictionary "self.dict" (Communications, Actuations, MaxOuts)
    
    '''
    def __init__(self, server='ODOTDWQUERYPROD', database='STG_ITS_SQL_MAXVIEW_EVENTLOG'):

        self.today = datetime.datetime.now().date()
        self.path_ = '//scdata2/signalshar/Data_Analysis/Traffic_Signal_Data_and_Analytics/' #this file location
        self.path_save = '//scdata2/signalshar/Data_Analysis/Data/Performance/' #destination to save data to
        self.server = server
        self.database = database    
        # Dictionary of avaiable functions for downloading different data types
        self.dict_ = {
            'Actuations' : self.actuations,
            'Communications' : self.communications,
            'MaxOuts' : self.maxouts,
            'MaxTimeFaults' : self.maxtime_faults,
            'Splits' : self.splits,
            'Ped' : self.ped,
            'Coordination' : self.coordination,
            'Unique_Ped_Actuations' : self.unique_ped
            }

    def run_query(self, sql):
        # Turn off any SQL warnings that sometimes cause problems for Pandas
        sql = f'SET NOCOUNT ON; SET ANSI_WARNINGS OFF; {sql}'
        connection = pyodbc.connect('Driver={SQL Server Native Client 11.0};' +\
            f'Server={self.server};Database={self.database};Trusted_Connection=yes;')
        data = pd.read_sql_query(sql, connection)
        connection.close()
        return data

    def update_data(self, data_type):
        '''Automatically updates data from the Last_Run date through yesterday'''
        function = self.dict_[data_type]
        # Read the Last_Run/Actuations date
        with open(f'{self.path_}Last_Run/{data_type}.txt', 'r') as file:
            last_run = datetime.datetime.strptime(file.read(), '%Y-%m-%d').date()
        # Update data 1 day at a time through yesterday
        while last_run < self.today - datetime.timedelta(days=1):
            start = str(last_run + datetime.timedelta(days=1))
            end = str(last_run + datetime.timedelta(days=2))
            print(f'\nWORKING ON: {data_type} {start} at: {datetime.datetime.now()}')
            function(start, end).to_parquet(f'{self.path_save}{data_type}/{start}.parquet')
            # After successful run, update Last_Run/Actuations for next time
            with open(f'{self.path_}Last_Run/{data_type}.txt', 'w') as file:
                file.write(start)
            # Now on to the next day
            last_run += datetime.timedelta(days=1)
            # Give the server a rest...
            time.sleep(1)

    def read_sql(self, file, start, end):
        '''Opens a .sql file and replaces variables @start & @end with input variables'''
        with open(f'{self.path_}SQL/{file}.sql', 'r') as r:
            sql = r.read().replace('@start', f"'{start}'").replace('@end', f"'{end}'")
        return sql

    def actuations(self, start, end):
        '''Return aggregate actuations between start and end timestamps
        Indexing and optimized datatypes are used to reduce memory consumption
        '''
        index_col = ['TimeStamp', 'DeviceID', 'MT']
        sql = self.read_sql(file='Actuations', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'MT':'uint8', 'Total':'uint16'}).set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space

    def communications(self, start, end):
        '''Return averaged communication stats between start and end timestamps
        Indexing and optimized datatypes are used to reduce memory consumption
        '''
        index_col = ['TimeStamp', 'DeviceID', 'EventID']
        sql = self.read_sql(file='Communications', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'Average':'float32', 'EventID':'category'}).set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space

    def maxouts(self, start, end):
        '''Return averaged phase termination stats between start and end timestamps
        Indexing and optimized datatypes are used to reduce memory consumption
        '''
        index_col = ['TimeStamp', 'DeviceID', 'Phase']
        sql = self.read_sql(file='MaxOuts', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'Phase':'category', 'GapOut':'uint16', 'MaxOut':'uint16', 'ForceOff':'uint16', 'PhaseCall':'uint16', 'Pct_MaxOut':'float32'})
        data = data.set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space        

    def maxtime_faults(self, start, end):
        index_col = ['TimeStamp', 'DeviceID']
        sql = self.read_sql(file='MaxTimeFaults', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'Parameter':'uint8', 'EventID':'category'})
        data = data.set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space

    def ped(self, start, end):
        index_col = ['TimeStamp', 'DeviceID', 'Phase']
        sql = self.read_sql(file='Ped', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'Phase':'category', 'PedServices':'uint16', 'PedActuation':'uint16'})
        data = data.set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space 

    def unique_ped(self, start, end):
        index_col = ['TimeStamp', 'DeviceID', 'Phase']
        sql = self.read_sql(file='Unique_Ped_Actuations', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'Phase':'category', 'Unique_Actuations':'uint16'})
        data = data.set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space 
        
    def splits(self, start, end):
        index_col = ['TimeStamp', 'DeviceID', 'EventID']
        sql = self.read_sql(file='Splits', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'EventID':'category', 'Services':'uint16', 'Average_Split':'uint16'})
        data = data.set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space 

    def coordination(self, start, end):
        index_col = ['15-Minute_TimeStamp', 'DeviceID', 'EventID']
        sql = self.read_sql(file='Coordination', start=start, end=end)
        data = self.run_query(sql)
        data = data.astype({'DeviceID':'uint16', 'EventID':'category', 'Parameter':'uint16'})
        data = data.set_index(index_col)
        return data.sort_index(level=0) #sorted data takes less space


class Aggregate_ATSPMs(Get_SQL_Data): 

    def __init__(self, server='SP2SQLMAX101', database='MAXVIEW_EVENTLOG'):
        Get_SQL_Data.__init__(self, server, database)
        # Dictionary of detector function labels and types
        self.dict = {
            'P' : self.split_fail,
            'Presence' : self.split_fail,
            'Yellow Red' : self.yellow_red,
            'YellowRed' : self.yellow_red,
            }

    def load_config(self):
        df = pd.read_excel(f'{self.path_save}DetConfig.xlsx', converters={'TSSU ID':str}) # .xlsx is deprecated, but works with xlrd==1.2.0
        df = df[(df['Phase'].notnull()) & (df['Det'].notnull())].sort_values('TSSU ID')
        return df
    
    def save_config(self, df):
        with pd.ExcelWriter(f'{self.path_save}DetConfig.xlsx') as writer: #lets turn this into a function later
            df.to_excel(writer, index=False)
    
    def read_sql(self, file, start, end, TSSU, Phase, Det):
        '''Opens a .sql file and replaces SQL variables with function input variables'''
        with open(f'{self.path_}SQL/{file}.sql', 'r') as r:
            sql = r.read().replace('@start', f"'{start}'").replace('@end', f"'{end}'")
            sql = sql.replace('@TSSU', f"'{TSSU}'")
            sql = sql.replace('@Phase', Phase).replace('@Det', Det)
            if self.server == 'ODOTDWQUERYPROD':
                sql = sql.replace('[MaxView_1.9.0.744].[dbo].', '')
        return sql

    def split_fail(self, start, end, TSSU, Phase, Det):
        '''Gets aggregate split failures for input phase/detector, saves it to parquet file'''
        ## Temp Code to return DeviceID since it's broken right now
        connection2 = pyodbc.connect('Driver={SQL Server Native Client 11.0};' +\
            'Server=sp2sqlmax101;Database=maxview_eventlog;Trusted_Connection=yes;')
        DeviceID = pd.read_sql_query(f"SET NOCOUNT ON; SELECT GroupableElements.ID FROM [MaxView_1.9.0.744].[dbo].[GroupableElements] WHERE Right(GroupableElements.Number,5) = '{TSSU}'", connection2)
        connection2.close()
        ## End temp code
        index_col = ['TimeStamp']
        sql = self.read_sql(file='SplitFail', start=start, end=end, TSSU=TSSU, Phase=Phase, Det=Det)
        #more temp code:
        sql = sql.replace('@DeviceID', str(DeviceID.iloc[0,0]))
        data = self.run_query(sql)
        data = data.astype({'TSSU':'category', 'Phase':'uint8', 'MT':'uint8', 'Total':'uint8'}).set_index(index_col)
        path = f'{self.path_save}SplitFail.parquet'
        try:
            data.append(pd.read_parquet(path)).sort_index().to_parquet(path)
        except FileNotFoundError:
            print('First time saving')
            data.sort_index().to_parquet(path)


        return data#.sort_index(level=0) #no point in sorting yet
    
    def yellow_red(self):
        pass

    def update_data(self):  # sourcery skip: remove-empty-nested-block, remove-pass-elif, remove-redundant-if, remove-redundant-pass
        functions = {'P': self.split_fail, 'Presence':self.split_fail, }
        df = self.load_config()
        for index, row in df.iterrows():
            #set variables
            TSSU = str(row['TSSU ID'])
            
            # For now, skip anything not in Sandy!
            #if TSSU not in {'23023', '23037', '23026', '23025', '23024', '23036'}:
            #    continue
            Phase = str(row['Phase'])
            Det = str(row['Det'])
            last_run = row['Last Run']
            earliest_run = row['Earliest Run']
            start_date = row['Start date']
            try:
                function = self.dict[row['Function']]
            except KeyError:
                continue
            if pd.isnull(last_run): #For first time run: Start Date through end of Yesterday
                start = start_date
                end = self.today
                try:
                    print('First Run For:', TSSU, Phase, Det)
                    function(start, end, TSSU, Phase, Det)
                    last_run = self.today
                    df.at[index, 'Last Run'] = last_run
                    df.at[index, 'Earliest Run'] = start_date
                    self.save_config(df)
                    print('done, sleeping')
                    time.sleep(5)
                    continue
                except Exception as e:
                    print("SQL query error, first time update", TSSU, Phase, Det, e)
                    

            if last_run < self.today: #If not first run: Last Run through Yesterday
                start = last_run
                end = self.today #- datetime.timedelta(days=1)
                try:
                    print('Normal Run For:', TSSU, Phase, Det)
                    function(start, end, TSSU, Phase, Det)
                    df.at[index, 'Last Run'] = self.today
                    self.save_config(df)
                except Exception:
                    print("SQL query error, normal update")
                    break

            if start_date < earliest_run: #If Start Date has been changed: Start Date through Earliest Run
                start = start_date
                end = earliest_run
                try:
                    print('EARLIER START DATE Run For:', TSSU, Phase, Det)
                    function(start, end, TSSU, Phase, Det)
                    df.at[index, 'Earliest Run'] = start_date
                    self.save_config(df)
                except Exception as e:
                    print("SQL query error, earlier date update", e, sep='\n')
                    break


class Get_RITIS_Data():
    '''
    Download Data using RITIS Massive Data Downloader.
    Uses Chrome browser, driver at https://chromedriver.chromium.org/downloads
    Driver executable placed at C:\Program Files (x86)
    Executable file needs to be updated periodically when browser is updated
    '''
    def __init__(self, driver_path="C:\Program Files (x86)\msedgedriver.exe"):
        self.today = datetime.datetime.now().date()
        self.path_ = '//scdata2/signalshar/Data_Analysis/Traffic_Signal_Data_and_Analytics/'
        self.driver_path=driver_path
        self.url='https://pda.ritis.org/suite/download/'
        self.url2='https://pda.ritis.org/suite/my-history/'
        
        ##NOTES: Edge stopped working because profile issues, so I'm using Chrome instead
        # Now to set it to use Default profile, to handle login stuff
        #self.edge_options = EdgeOptions()
        #self.edge_options.use_chromium = True    
        #Here you set the path of the profile ending with User Data not the profile folder
        #self.edge_options.add_argument("user-data-dir=C:\\Users\\hwyr67g\\AppData\\Local\\Microsoft\\Edge\\User Data"); 
        #Here you specify the actual profile    
        #self.edge_options.add_argument("user-data-dir=C:\\Users\\hwyr67g\\AppData\\Local\\Microsoft\\Edge\\User Data"); 
        #self.edge_options.add_argument("profile-directory=Default");
        #self.edge_options.binary_location = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
        #self.driver = Edge(options = self.edge_options, executable_path = self.driver_path)

        self.driver = webdriver.Chrome(executable_path=r"chromedriver.exe")

        # Define xpath notations to be used
        # for later, rather than using entire XPATH, i'm using shortcuts/search functions cause wow what a mess!
        # But leave what works in place for now
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
        #self.select_status = '/html/body/main/div/div[1]/div[2]/div[2]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/div/div/div/div/span/div'
        #self.select_download = '/html/body/main/div/div[1]/div[2]/div[2]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div[3]/div/div/div/div/div/div/div/a'
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
        # Make sure XD is the selected option from drop-down
        # Click on drop down
        print('clicking on drop down arrow')
        self.select(xpath='//*[@class="datasource-group"]//*[@class="Select-arrow"]').click()
        print('clicking on XD')
        self.select(xpath='//*[@class="opened-menu"]//*[@title="XD"]').click()
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

        # Limit time range 6:00 AM to 7:59 PM
        print('Entering start time')
        starttime = self.select(xpath='//*[@class="time-range-wrapper lower-time"]/div[1]/input')
        starttime.send_keys(Keys.CONTROL, "a")
        starttime.send_keys('6:00')
        print('Entering end time')
        starttime = self.select(xpath='//*[@class="time-range-wrapper upper-time"]/div[1]/input')
        starttime.send_keys(Keys.CONTROL, "a")
        starttime.send_keys('7:59', Keys.TAB)

        # Uncheck confidence score 20 & 30
        # input:checked[type='checkbox'] #css selector for checked box
        historical = self.driver.find_element(By.XPATH, '//*[@class="confidence-score-option"][contains(., "Historical Average:")]/label/input')
        if historical.is_selected():
            print('unchecking the historical box')
            self.select(xpath='//*[@class="confidence-score-option"][contains(., "Historical Average:")]/label').click()
        else:
            print('historical already unchecked')
        # Uncheck reference
        reference = self.driver.find_element(By.XPATH, '//*[@class="confidence-score-option"][contains(., "Reference Speed:")]/label/input')
        if reference.is_selected():
            print('unchecking the reference box')
            self.select(xpath='//*[@class="confidence-score-option"][contains(., "Reference Speed:")]/label').click()
        else:
            print('reference already unchecked')


        # Select units of Seconds
        print('clicking on Seconds')
        self.select(xpath='//*[@class="TravelTimeUnitsSelector"]/div/div').click()
        # Select 15-min aggregation
        print('clicking on 15-minute')
        self.select(xpath=self.select_15_min).click()
        # Add a title
        file_name = str(date)
        self.select(xpath=self.select_title).send_keys(file_name)
        # Uncheck send email
        self.select(xpath=self.select_email).click()
        # Submit the reqeust
        self.select(xpath=self.select_submit_button).click()
        return file_name # to be used as file name for retreiving download
        
    def download_data(self, file_name, url='https://pda.ritis.org/suite/my-history/'):
        check_download = str(Path.home() / f'Downloads//{file_name}.zip')
        self.log_in(url)
        # Check Status, download to PC when done
        try:
            print('Waiting for job to be ready')
            while self.select(xpath=f"//*[@class='fixedDataTableCellGroupLayout_cellGroup' ][contains(.,'{file_name}')]//*[@class='TooltipWrapper']/div", sleep=30, wait=10).text == 'Pending':
                time.sleep(5)
            print('Downloading to download folder')
            self.select(xpath=f"//*[@class='description-text' ][contains(.,'{file_name}')]//a").click()
        except Exception as e:
            print('Download Failed, error: ', e)
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
        print('Reading data and optimizing data types')
        df = pd.read_csv(f'{download_folder}/{file_name}/{file_name}.csv', parse_dates=['measurement_tstamp'])
        df = df.astype({'speed':'float32', 'reference_speed':'float32', 'travel_time_seconds':'float32'}) #'xd_id':'uint32' pandas upscales index dtypes
        df = df.set_index(['xd_id', 'measurement_tstamp'])
        df.index.names = ['XD', 'TimeStamp']
        df = df[['speed','reference_speed','travel_time_seconds']]
        # Check that the input date matches date on actual data!
        # This check lets us know if something has changed or gone wrong to ensure data is as expected
        if str(df.index[1][1].date()) == file_name:
            df.sort_index(level=0).to_parquet(f'TravelTime/{file_name}.parquet')
            print('Saved parquet file to folder')
        else:
            print('ERROR!!! Date of data timestamp did not match input date! Debug date picker!')
            print(f'Data date = {str(df.index[1][1].date())} and input date = {file_name}')
            quit()
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
        with open(f'{self.path_}Last_Run/RITIS.txt', 'r') as file:
            last_run = datetime.datetime.strptime(file.read(), '%Y-%m-%d').date()
        # Update data 1 day at a time through yesterday
        while last_run < self.today - datetime.timedelta(days=1):
            start = last_run + datetime.timedelta(days=1)
            print(f'\nWORKING ON: RITIS {str(start)} at: {datetime.datetime.now()}')
            self.main(date=start)
            # After successful run, update Last_Run/RITIS for next time
            with open(f'{self.path_}Last_Run/RITIS.txt', 'w') as file:
                file.write(str(start))
            # Now on to the next day
            last_run += datetime.timedelta(days=1)
            time.sleep(2)
        self.driver.quit() # quits the browser


class Analytics():
    
    def __init__(self):
        self.today = datetime.datetime.now().date()
        self.path_ = '//scdata2/signalshar/Data_Analysis/Traffic_Signal_Data_and_Analytics/'

    @classmethod
    def load_data(cls, folder, date=str(datetime.datetime.now().date()), num_days = 35+7):
        '''Loads parquet files from selected folder and date range'''
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

    def normalize_by_group(self, df, group, column, dtype='float32'):
        '''Returns normalized values, using Vectorization for quick calculation
        Assumes an index already in place, resets the index and sets it to the group
        Takes a dataframe, a list of columns for which to group the data, and the column for which the normalization is based on
        adapted from https://stackoverflow.com/questions/26046208/normalize-dataframe-by-group'''
        df = df.reset_index().set_index(group)
        df = df[column]
        groups = df.groupby(level = group)
        # computes group-wise mean/std, then auto broadcasts to size of group chunk
        mean = groups.transform("mean")
        std = groups.transform("std")
        new_df = (df[mean.columns] - mean) / std
        return new_df.astype(dtype).values

    def add_seg_groups(self, df, columns = None, file='//scdata2/signalshar/Data_Analysis/Data/Performance/dim_signals_XD.parquet'):
        '''Add grouping categories'''
        if columns is None:
            columns = ['group', 'District']
        # Load dimension table with groups
        dim = pd.read_parquet(file)[columns]
        dim = dim.astype('category')
        print(f'The data types of groups are both supposed to be category: {dim.dtypes}')
        # drop XD duplicates (same one can be assigned to multiple signals)
        dim = dim[~dim.index.duplicated()]
        return df.join(dim)


    def find_anomalies(self, df, z=4):
        '''Calculates individual z-scores from group z-scores'''
        #df.reset_index(inplace=True)
        df['Resid_Z'] = self.normalize_by_group(df=df, group=['XD'], column=['Resid'])
        df['Resid_District_Z'] = self.normalize_by_group(df=df, group=['TimeStamp', 'District'], column=['Resid_Z'])
        
        # Classify Local Anomalies. If |Resid_Z| > 4 and |Resid_District_Z| < 4, then Global, If |Resid_Z| > 4 and |Resid_Distric_Z| > 4, then Local 
        # "Local Anomaly" is when a specific XD segment is an anomaly in regards to itself, AND those in the same district
        df['LocalAnomaly'] = df.apply(lambda row: abs(row['Resid_District_Z']) > z and abs(row['Resid_Z']) > z, axis=1) #need to vectorize
        
        # "Local Anomaly within Corridor" is when the XD segment is additionally an anomaly within it's continuous group of segments
        df['Resid_Corridor_Z'] = self.normalize_by_group(df=df, group=['TimeStamp', 'group'], column=['Resid_Z'])
        df['LocalAnomaly_Within_Corridor'] = df.apply(lambda row: abs(row['Resid_Corridor_Z']) > z and abs(row['Resid_Z']) > z, axis=1) #need to vectorize
        
        #defunct too slow
        # Finding local group anomalies is very slow, 10 min for 2 weeks
        #def group_Z(x):
        #    return 0 if len(x) < 5 else (x-x.mean())/ x.std()
        #r['Resid_group_Z'] = r.groupby(level=['TimeStamp', 'group'])['Resid_Z'].transform(lambda x: group_Z(x))
        return df        
    
    def decompose(self, df, field='travel_time_seconds', freq='15min', window='7d',min_periods=56*4, time_min='6:00', time_max='19:45'):
        '''Robust decomposition using median
        dataframe input must have indexs XD and TimeStamp'''

        # Temp step
        #df = df.loc[[1237049582, 1236860943], slice(None),:]
        df = df[[field]]
        # Filter by Time of Day (step no longer needed because time is now filtered by RITIS massive data downloader first?)
        df = df.reset_index().set_index('TimeStamp').between_time(time_min, time_max).reset_index().set_index(['TimeStamp','XD'])
        df.index.levels[1].freq = freq
        print(f'Frequency set at {df.index.levels[1].freq}')
        print('create group', datetime.datetime.now())
        group = df.reset_index(level=1).groupby('XD')
        print('reset index', datetime.datetime.now())
        df = df.reset_index()
        print('working on rolling median', datetime.datetime.now())
        df['RollingMedian'] = group.transform(lambda x: x.rolling(window=window, min_periods=min_periods, closed='both').median()).reset_index()[field]
        #print('rolling median',datetime.datetime.now(), df.head())
        # drop first 7 days because the rolling median needs a full week
        min_date = df['TimeStamp'].min() + datetime.timedelta(days = 7)
        df = df[df['TimeStamp'] > min_date]
        #print('rolling median', df.head())

        df['Detrend'] = df.travel_time_seconds - df.RollingMedian
        df['DayPeriod'] = ((df['TimeStamp'].dt.hour * 60 + df['TimeStamp'].dt.minute)/15 + 1).astype(int)
        df['WeekPeriod'] = df['TimeStamp'].dt.isocalendar().day
        df['SeasonDay'] = df.groupby(['XD', 'DayPeriod'])['Detrend'].transform('median')
        df['DeSeason_temp_step'] = df.Detrend - df.SeasonDay
        df['SeasonWeek'] = df.groupby(['XD', 'WeekPeriod', 'DayPeriod'])['DeSeason_temp_step'].transform('median')
        df['Resid'] = df.DeSeason - df.SeasonWeek
        df = df.set_index(['XD', 'TimeStamp'])[['travel_time_seconds', 'RollingMedian', 'SeasonDay', 'SeasonWeek', 'Resid']]
        return df.dropna().astype('float32')


    def global_anomaly(self):
        pass


class DetectorHealth():#Analytics):

    def __init__(self):
        self.today = datetime.datetime.now().date()
        self.path_ = '//scdata2/signalshar/Data_Analysis/Traffic_Signal_Data_and_Analytics/'
        
    def load_all_data(self, days=35, date='2022-03-07'):
        '''Load each data type'''
        def quick_load(folder, date, num_days):
                try:
                        now = datetime.datetime.now()
                        df = Analytics.load_data(folder, date, num_days)
                        #print(f'{folder} took {datetime.datetime.now() - now} seconds')
                        return df.reset_index()
                except Exception:
                        print(f'{folder} not loaded, missing data')
        path = '//scdata2/signalshar/Data_Analysis/Data/Performance'
        self.actuations = quick_load(folder=f'{path}/Actuations', date=date, num_days=days).astype({'DeviceID':'uint16', 'MT':'uint8', 'Total':'UInt16'}) #UInt handles NaN, for when missing values are added in
        self.comm = quick_load(folder=f'{path}/Communications', date=date, num_days=days).astype({'DeviceID':'uint16', 'Average':'float32', 'EventID':'category'})
        #self.maxout = quick_load(folder=f'{path}/MaxOuts', date=date, num_days=days)#add datatypes later
        self.maxtime = quick_load(folder=f'{path}/MaxTimeFaults', date=date, num_days=days).rename(columns={'Parameter': 'MT', 'EventID':'Fault_MaxTime'}).astype({'DeviceID':'uint16', 'MT':'uint8', 'Fault_MaxTime':'category'})

    def cleanse(self):
        # Get distinct DeviceID and Parameter pairs from Actuations
        detector_pairs = self.actuations[['DeviceID', 'MT']].drop_duplicates()
        # Get table of time periods with zero comm loss for each device
        comm = self.comm[self.comm['EventID'] == 502] #EventID 502 is %comm loss
        comm = comm[comm['Average'] == 0] #Filter to %comm loss is zero
        comm = comm[['TimeStamp', 'DeviceID']]
        # Generate timestamp for each detector where the signal had no comm loss
        # This is meant to be a cross product between timestamps with zero comm loss and each detector at a given signal
        # This table represents each time period where there should have been good data recorded
        Actuations_All = pd.merge(comm, detector_pairs)
        #Actuations_All[(Actuations_All['DeviceID']==2) & (Actuations_All['MT']==1)].sort_values('TimeStamp', ascending=False)
        # Merge previous table with actual actuations. There will be NaN's where there were no actuations (due to no volume or detector faults)
        Actuations_All = pd.merge(Actuations_All, self.actuations, how="left")
        # Fill NAs with zeros. Keep datatype as int
        Actuations_All = Actuations_All.fillna(0)
        Actuations_All['Total'] = Actuations_All['Total'].astype('uint16') #back to Numpy dtype to have Numpy operations work
        del self.actuations
        del self.comm
        return Actuations_All

    def GEH(self, df):
        '''Calculates GEH Statistic between Total and Median
        https://en.wikipedia.org/wiki/GEH_statistic'''
        M = df['Total']
        C = df['Median']
        sign = np.sign(M - C)
        GEH = np.where(C == 0, 0, ((2 * (M - C)**2) / (M + C))**0.5)
        # add sign back to GEH for use in Z-score calc
        return np.where(C == 0, np.NaN, GEH * sign)

    def dims(self, server='SP2SQLMAX101', database='MaxView_1.9.0.744'):
        '''Get traffic signal dimension table with names and parent groups'''
        sql = 'SELECT ID as DeviceID, ParentID FROM GroupableElements WHERE ParentID IS NOT NULL'
        return Get_SQL_Data(server=server, database=database).run_query(sql=sql).astype({'DeviceID':'uint16', 'ParentID':'category'})

    def normalize(self, df):
        '''Returns indexd colum of normalized valuse, using Vectorization
        NO INDEXED COLUMNS'''
        mean = df.groupby(['TimeStamp', 'ParentID'])['GEH'].transform("mean")
        std = df.groupby(['TimeStamp', 'ParentID'])['GEH'].transform("std")
        return (df['GEH'] - mean) / std

    def ratio_faults(self, df):
        '''Calculates ratio of Total to the Average for each detector on each day
        The average is limited to periods from 6am to 6pm
        Returns weather the ratio is a fault, given conditions'''
        df2 = df.copy()
        # Need date column for groupby
        df2['Date'] = df2['TimeStamp'].dt.date
        # Need temporary Total column for averaging Total between 6am and 6pm
        df2['Day_Total'] = np.where(df2['DayPeriod'] >= 25, np.where(df2['DayPeriod'] < 73, df2['Total'], np.NaN), np.NaN)
        # Average between 6am and 6pm
        mean = df2.groupby(['DeviceID', 'MT', 'Date'])['Day_Total'].transform("mean")
        ratio = df2['Total'] / mean
        # Set conditions for a fault. These numbers can be adjusted
        conditions = [
                (ratio >= 3) & (df2['Total'] > 40),
                (df2['DayPeriod'] < 17) & (ratio > 0.75) & (df2['Total'] > 10)
                ]
        choices = [True, True]
        return np.select(conditions, choices, default=False)

    def fault_category(self, df):
        '''Categorizes the type of fault'''
        conditions = [
                (df['Fault_MaxTime']==87),
                (df['Fault_MaxTime']==88),
                (df['Fault_Z_score']==True),
                (df['Fault_Ratio']==True)
                ]
        choices = ['Stuck On', 'Erratic', 'Anomaly', 'Excessive']
        return np.select(conditions, choices, default='None')

    def seasonal_median(self, df):
        '''Calculate the median within each seasonal period'''
        # Add seasonal periods
        df['WeekPeriod'] = df['TimeStamp'].dt.isocalendar().day.astype('int8')
        df['DayPeriod'] = ((df['TimeStamp'].dt.hour * 60 + df['TimeStamp'].dt.minute)/15 + 1).astype('int8')
        # Return median
        return df.groupby(['DeviceID', 'MT', 'WeekPeriod', 'DayPeriod'])['Total'].transform('median')        

    def transform(self, df):
        '''Apply transformation steps to classify outliers
        and return final dataframe'''
        # Calculate median and GEH per detector
        df['Median'] = self.seasonal_median(df).astype('uint16')
        df['GEH'] = self.GEH(df).astype('float16')
        # Add ParentID from MaxView, calc Z-scores grouped by ParentID
        df = pd.merge(df, self.dims())
        df['Z_Score'] = self.normalize(df).astype('float16')
        df['Fault_Z_score'] = np.where(abs(df['Z_Score']) >= 3.5, np.where(abs(df['GEH']) > 5, True, False), False)
        # Find "Ratio Faults" due to excessive nighttime actuations
        df['Fault_Ratio'] = self.ratio_faults(df)
        # Add MaxTime Faults. Drop duplicates in case there are "Stuck On" and "Erratic" during same timestamp
        m = self.maxtime.drop_duplicates(subset=['TimeStamp', 'DeviceID', 'MT'])
        df = pd.merge(df, m, how='left')
        # Categorize Faults
        df['Fault_Type'] = self.fault_category(df)
        df['Fault_Type'] = df['Fault_Type'].astype('category')
        df = df[['TimeStamp', 'DeviceID', 'MT', 'Total', 'Fault_Type']].set_index(['TimeStamp', 'DeviceID', 'MT', 'Fault_Type'])
        return df.sort_index(level=0)

    def update_data(self):
        '''Automatically updates data from the Last_Run date through yesterday'''
        # Read the Last_Run/Actuations date
        with open('Last_Run/DetectorHealth.txt', 'r') as file:
                last_run = datetime.datetime.strptime(file.read(), '%Y-%m-%d').date()
        # Update data 1 day at a time through yesterday
        while last_run < self.today - datetime.timedelta(days=1):
                date = str(last_run + datetime.timedelta(days=1))
                print(f'\nWORKING ON: Detector Health {date} at: {datetime.datetime.now()}')
                self.load_all_data(days=35, date=date)
                df = self.cleanse()
                df = self.transform(df)
                df.loc[date].to_parquet(f'//scdata2/signalshar/Data_Analysis/Data/Performance/DetectorHealth/{date}.parquet')
                # After successful run, update Last_Run/Actuations for next time
                with open('Last_Run/DetectorHealth.txt', 'w') as file:
                        file.write(date)
                # Now on to the next day
                last_run += datetime.timedelta(days=1)

