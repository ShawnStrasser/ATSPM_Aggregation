import pandas as pd
import duckdb
import os

# Additional libraries are imported inside of optional functions: query_mssql & 

class Aggregations:
    def __init__(self, phase_detector_config, data=None, mssql_server=None, mssql_database=None, duckdb_threads=None):
        # Connect to DuckDB and register table
        self.duck_con = duckdb.connect(database=':memory:', read_only=False)

        # Load data if provided, ensuring proper format
        try:
            if data is not None:
                # Set data types
                data = data.astype({'DeviceId':'uint16', 'EventId':'uint8', 'Parameter':'uint8'})
                data = duckdb.query('SELECT DISTINCT * FROM data WHERE EventId IN(1,8,10,81,82)').fetchdf()
                self.duck_con.register('raw_data', data)
        except Exception as e:
            print(e)
            print('Data must be a pandas dataframe with columns: DeviceId, EventId, Parameter, Timestamp')

        # Option to limit CPU use if needed
        if isinstance(duckdb_threads, int):
            duckdb.query(f"SET threads to {duckdb_threads}")
            #print(duckdb.query(f"SELECT current_setting('threads');"))

        # Define phase-detector configurations dictionary
        # First entry is configurations dataframe, second is devices
        self.configs = dict()
        def declare_config(measure_detection):
            measure, detection = measure_detection
            try:
                self.configs[f'{measure}_config'] = phase_detector_config[phase_detector_config.Function == detection][['Phase', 'Parameter', 'DeviceId']]
                self.configs[f'{measure}_devices'] = set(self.configs[f'{measure}_config'].DeviceId)
                assert len(self.configs[f'{measure}_devices']) > 0
            except Exception as e:
                print(f'{measure} Detection Not Found!')
                print(e)
        for item in [('split_fail', 'Presence'), ('yellow_red', 'Yellow_Red'), ('arrival_on_green', 'Advance')]:
            declare_config(item)
 
        self.mssql_server = mssql_server
        self.mssql_database = mssql_database

        # Get the absolute path of the current file
        current_file_path = os.path.abspath(__file__)
        # Construct the absolute path to the queries.sql file
        queries_file_path = os.path.join(os.path.dirname(current_file_path), 'queries.sql')
        # Load SQL Queries Into Dicitonary
        with open(queries_file_path, 'r') as file:
            content = file.read()
        queries = content.split(';')  # Splits queries by ';' which ends a SQL command
        self.queries_dict = {}
        for query in queries:
            if query.strip() != '':  # Ignore empty lines
                lines = query.strip().split('\n')  # Split lines
                name = lines[0].strip('- ').strip()  # Extract query name from the first line
                sql_query = '\n'.join(lines[1:]).strip()  # Join the remaining lines to form the query
                self.queries_dict[name] = sql_query

    # Run queries in MS SQL Server
    def query_mssql(self, query, server, database):
        from sqlalchemy import create_engine
        import warnings
        connection_string = f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=SQL+Server"
        engine = create_engine(connection_string)
        conn = engine.raw_connection() # Uses DBAPI
        # Supress warning from Pandas where it says it's only tested on sqlalchemy
        # This method is MUCH faster, so I'll stick with it
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql_query(query, conn)
        conn.close()
        engine.dispose()
        return df
    
    # Get raw event data from SQL Server
    def get_mssql_data(self, start, end, filtered_devices=None):
        
        if filtered_devices is None:
            device_filter = ''
        else:
            device_filter = f"AND DeviceId IN{str(filtered_devices).replace('[','(').replace(']',')')}"
        
        query = f"""
        SELECT DISTINCT *
        FROM ASCEvents 
        WHERE ASCEvents.TimeStamp >= '{start}' 
        AND ASCEvents.TimeStamp < '{end}'
        AND EventId IN(1,8,10,81,82)
        {device_filter}
        """
        # Load raw data and downsize the dtypes for efficiency
        df = self.query_mssql(query, self.mssql_server, self.mssql_database)
        df = df.astype({'DeviceId':'uint16', 'EventId':'uint8', 'Parameter':'uint8'})
        # Register the data in DuckDB
        self.duck_con.register('raw_data', df)

    # Get raw event data from SQL Server
    def get_mssql_data(self, start, end, filtered_devices=None):
        
        if filtered_devices is not None:
            # Start constructing a long SQL script
            sql_script = "SET NOCOUNT ON; CREATE TABLE #TempDeviceTable (DeviceId int); "
            
            # Add an INSERT statement to the script for each device
            for device in filtered_devices:
                sql_script += f"INSERT INTO #TempDeviceTable (DeviceId) VALUES ({device}); "

            # Modify the device filter to use a JOIN instead of IN
            device_filter = """
            INNER JOIN #TempDeviceTable 
            ON ASCEvents.DeviceId = #TempDeviceTable.DeviceId
            """
        else:
            device_filter = ''
            sql_script = ''

        # Add the main SELECT statement to the script
        sql_script += f"""
        SELECT DISTINCT *
        FROM ASCEvents 
        {device_filter}
        WHERE ASCEvents.TimeStamp >= '{start}' 
        AND ASCEvents.TimeStamp < '{end}'
        AND EventId IN(1,8,10,81,82);
        """

        if filtered_devices is not None:
            # Add a statement to drop the temp table to the script
            sql_script += "DROP TABLE #TempDeviceTable;"
        #print('\n'*3,sql_script,'\n'*3)
        # Load raw data and downsize the dtypes for efficiency
        df = self.query_mssql(sql_script, self.mssql_server, self.mssql_database)
        df = df.astype({'DeviceId':'uint16', 'EventId':'uint8', 'Parameter':'uint8'})
        # Register the data in DuckDB
        self.duck_con.register('raw_data', df)
        #print(sql_script)


    # Helper function to modify and run DuckDB queries
    def create_view(self, query_name, view_name, from_table=None, variable1=None):
        '''
        query_name: name of query to run
        view_name: name of view to create
        from_table: table to use in query
        variable1: variable to use in query'''

        query = self.queries_dict[query_name]
        if from_table is not None:
            query = query.replace('@table', from_table)
        if variable1 is not None:
            query = query.replace('@variable1', variable1)
        # Create the view (drop if it already exists)
        self.duck_con.execute(f"DROP VIEW IF EXISTS {view_name}")
        self.duck_con.execute(f"CREATE TEMPORARY VIEW {view_name} AS {query}")
    
    # Function to clear all tables and cache in DuckDB
    def clean_duck(self):
        #self.duck_con.execute('DROP ALL OBJECTS;')
        self.duck_con.close()

        # Clear any pandas DataFrames stored as instance variables.
        for attr_name, attr_value in self.__dict__.items():
            if isinstance(attr_value, pd.DataFrame):
                print(f"Clearing {attr_name}")
                delattr(self, attr_name)

        # Clear SQL queries dictionary
        if hasattr(self, 'queries_dict'):
            print('Clearing queries_dict')
            self.queries_dict.clear()

        # Clear configuration dictionary
        if hasattr(self, 'configs'):
            print('Clearing configs')
            self.configs.clear()

    # Function to check if data is loaded
    def check_data(self):
        tables = [x[0] for x in self.duck_con.execute("SHOW TABLES").fetchall()]
        if 'raw_data' not in tables:
            print('Data is not loaded yet!')
            raise ValueError("Data is not loaded yet!")
        # Check if data table is empty
        if self.duck_con.execute("SELECT COUNT(*) FROM raw_data LIMIT 1").fetchall()[0][0] == 0:
            print('Data is empty!')
            raise ValueError("Data is empty!")
        #print('Data is loaded and ready to go!')


    # Aggregate Split Failures, by approach is default, set to false to do by lane
    # Based on research, about 70% may be good threshold for 20ft long zones with approach based
    def split_failure(self, by_approach=True, green_occupancy_threshold=0.80, red_occupancy_threshold=0.80):
        # Check if data table exists in DuckDB
        self.check_data()
        # Now transform data into split failures
        # NOTE: TABLE NAMES ARE HARD CODED INTO queries.sql
        # DON'T CHANGE THESE UNLESS YOU DO IT IN BOTH FILES

        # Register configs in DuckDB
        self.duck_con.register('configs', self.configs['split_fail_config'])

        # Run SQL Queries to transform data
        # Each step is an immaterialized view that will be optimized together at the end
        self.create_view('detector_with_phase', view_name='view1')
        self.create_view('impute_actuations',view_name='view2', from_table='view1')
        # by_approach combines detectors accross phase
        if by_approach: 
            self.create_view('combine_detectors_ByApproach', view_name='view3a', from_table='view2')
            self.create_view('phase_with_detector_ByApproach', view_name='view3', from_table='view3a')       
        else:
            self.create_view('phase_with_detector_ByLane', view_name='view3', from_table='view2')
        # Remaining queries are same for by approach or by lane
        self.create_view('with_barrier', view_name='view4', from_table='view3', variable1='5')#add the barrier at 5 seconds
        self.create_view('with_cycle', view_name='view5', from_table='view4')
        self.create_view('time_diff', view_name='view6', from_table='view5')
        self.create_view('aggregate', view_name='view7', from_table='view6')
        self.create_view('final_SF', view_name='view8', from_table='view7')
        # Apply red/green occupancy thresholds for classification
        query = f"""
            SELECT *,
            CASE WHEN 
                Red_Occupancy>={red_occupancy_threshold} 
                AND Green_Occupancy>={green_occupancy_threshold}
                THEN True ELSE False END AS Split_Failure
            FROM view8
        """
        return self.duck_con.query(query).fetchdf()
        

    # Yellow and Red Actuations
    def yellow_red(self, bin_size=15, latency_offset=1.5):
        # Check if data table exists in DuckDB
        self.check_data()    
        # NOTE: TABLE NAMES ARE HARD CODED INTO queries.sql
        # DON'T CHANGE THESE UNLESS YOU DO IT IN BOTH FILES
        # Register configs in DuckDB
        self.duck_con.register('configs', self.configs['yellow_red_config'])
        # Run SQL Queries to transform data
        # Each step is an immaterialized view that will be optimized together at the end
        self.create_view('detector_with_phase_ON_ONLY', view_name='view1', variable1=str(latency_offset)) #only contains detector on events, shifted by 1.5 seconds for latency
        self.create_view('phase_with_detector_ByApproach', view_name='view2', from_table='view1') #contains phase data and detector data together
        self.create_view('with_cycle', view_name='view3', from_table='view2')        
        self.create_view('red_offset', view_name='view4', from_table='view3')
        return self.duck_con.query('SELECT * FROM view4').fetchdf()
    

    # Arrival on Green
    def arrival_on_green(self, bin_size=15, latency_offset=0):
        # Check if data table exists in DuckDB
        self.check_data()
        # NOTE: TABLE NAMES ARE HARD CODED INTO queries.sql
        # DON'T CHANGE THESE UNLESS YOU DO IT IN BOTH FILES
        # Register configs in DuckDB
        self.duck_con.register('configs', self.configs['arrival_on_green_config'])
        # Run SQL Queries to transform data
        # Each step is an immaterialized view that will be optimized together at the end
        self.create_view('detector_with_phase_ON_ONLY', view_name='view1', variable1=str(latency_offset)) #only contains detector on events. latency offset=0?
        self.create_view('phase_with_detector_ByApproach', view_name='view2', from_table='view1') #contains phase data and detector data together
        self.create_view('with_cycle', view_name='view3', from_table='view2')
        self.create_view('arrival_on_green', view_name='view4', from_table='view3', variable1=str(bin_size))
        return self.duck_con.query('SELECT * FROM view4').fetchdf()


    # Optional, plot occupancy
    def plot_occupancy(self, sf, DeviceId, Phase=None, Detector=None):
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.ticker import FuncFormatter
        from matplotlib.lines import Line2D
        # Filter DataFrame to include only rows with the given DeviceId and Detector
        if Phase is None and Detector is not None:
            sf_filtered = sf[(sf['DeviceId'] == DeviceId) & (sf['Detector'] == Detector)].sort_values('TimeStamp')
            name = f"Detector {Detector}"
        elif Detector is None and Phase is not None:
            sf_filtered = sf[(sf['DeviceId'] == DeviceId) & (sf['Phase'] == Phase)].sort_values('TimeStamp')
            name = f"Phase {Phase}"
        else:
            sf_filtered = sf[(sf['DeviceId'] == DeviceId) & (sf['Phase'] == Phase) & (sf['Detector'] == Detector)].sort_values('TimeStamp')
            name = f"Detector {Detector}, Phase {Phase}"
        sf_filtered.set_index('TimeStamp', inplace=True)

        # Create a scatter plot for Green_Occupancy and Red_Occupancy
        plt.figure(figsize=(10,5))

        # Calculate average occupancy in 15-minute intervals and fill missing data with zero
        average_green = sf_filtered['Green_Occupancy'].resample('15T').mean().fillna(0)
        average_red = sf_filtered['Red_Occupancy'].resample('15T').mean().fillna(0)

        # Plot the average occupancy as a stepped line
        plt.step(average_green.index, average_green, where='post', color='green', linestyle='-', label='Average Green Occupancy')
        plt.step(average_red.index, average_red, where='post', color='red', linestyle='-', label='Average Red Occupancy')

        # Find all timestamps where both Green_Occupancy and Red_Occupancy are above 0.79
        timestamps = sf_filtered[sf_filtered['Split_Failure']].index

        # Add a vertical line for each of those timestamps with a thinner line
        for timestamp in timestamps:
            plt.axvline(x=timestamp, color='yellow', linewidth=0.5)

        plt.scatter(sf_filtered.index, sf_filtered['Green_Occupancy'], color='green', s=3, label='Green Occupancy')
        plt.scatter(sf_filtered.index, sf_filtered['Red_Occupancy'], color='red', s=3, label='Red Occupancy')

        plt.xlabel('Timestamp')
        plt.ylabel('Occupancy')

        # Use FuncFormatter to display y-axis values as percentages
        plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

        # Use DateFormatter and HourLocator for a cleaner x-axis
        ax = plt.gca()
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

        # Add gridlines
        plt.grid(True)

        # Remove whitespace by setting limits
        plt.xlim(sf_filtered.index.min(), sf_filtered.index.max())
        plt.ylim(0, 1)

        # Rotate x-axis labels
        plt.xticks(rotation=45)

        # Create a custom legend entry
        custom_line = Line2D([0], [0], color='yellow', lw=2, label='Split Failure (vertical line)')

        # When calling legend(), append the custom entry to the list of handles
        handles, labels = plt.gca().get_legend_handles_labels()
        handles.append(custom_line)
        plt.legend(handles=handles, loc='lower left', framealpha=1)

        plt.title(f'Split Failures for DeviceId {DeviceId}, {name}')
        plt.tight_layout()
        plt.show()