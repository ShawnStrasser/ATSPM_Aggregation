import pandas as pd
import duckdb
import os

# Additional libraries are imported inside of optional functions: query_mssql & 

class Aggregations:
    def __init__(self, phase_detector_config, data=None, mssql_server=None, mssql_database=None, duckdb_threads=None):

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

        self.data = data
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
        self.data = self.query_mssql(query, self.mssql_server, self.mssql_database)
        self.data = self.data.astype({'DeviceId':'uint16', 'EventId':'uint8', 'Parameter':'uint8'})

    # Helper function to modify and run DuckDB queries
    def run_duck(self, query_name, from_table=None, variable1=None):
        #print(f'running query: {query_name}')
        query = self.queries_dict[query_name]
        if from_table is not None:
            query = query.replace('@table', from_table)
        if variable1 is not None:
            query = query.replace('@variable1', variable1)
        return duckdb.query(query)


    # Aggregate Split Failures, by approach is default, set to false to do by lane
    # Based on research, about 70% may be good threshold for 20ft long zones with approach based
    def split_failure(self, by_approach=True, green_occupancy_threshold=0.80, red_occupancy_threshold=0.80):
        if self.data is None:
            raise ValueError("Data is not loaded yet!")
        # Now transform data into split failures
        # NOTE: TABLE NAMES ARE HARD CODED INTO queries.sql
        # DON'T CHANGE THESE UNLESS YOU DO IT IN BOTH FILES
        raw_data = self.data # Make it visible to DuckDB
        sf_configs = self.configs['split_fail_config'] # Make it visible to DuckDB

        # Run SQL Queries to transform data
        # Each step is an immaterialized view that will be optimized together at the end
        view1 = self.run_duck('detector_with_phase')
        view2 = self.run_duck('impute_actuations','view1')
        # by_approach combines detectors accross phase
        if by_approach: 
            view3a = self.run_duck('combine_detectors_ByApproach', 'view2')
            view3 = self.run_duck('phase_with_detector_ByApproach', 'view3a')            
        else:
            view3 = self.run_duck('phase_with_detector_ByLane', 'view2')
        # Remaining queries are same for by approach or by lane
        view4 = self.run_duck('with_barrier', 'view3', '5')#add the barrier at 5 seconds
        view5 = self.run_duck('with_cycle', 'view4')
        view6 = self.run_duck('time_diff', 'view5')
        view7 = self.run_duck('aggregate', 'view6')
        view8 = self.run_duck('final_SF', 'view7')
        # Apply red/green occupancy thresholds for classification
        query = f"""
            SELECT *,
            CASE WHEN 
                Red_Occupancy>={red_occupancy_threshold} 
                AND Green_Occupancy>={green_occupancy_threshold}
                THEN True ELSE False END AS Split_Failure
            FROM view8
        """
        return duckdb.query(query).fetchdf()
        

    # Yellow and Red Actuations
    def yellow_red(self, bin_size=15, latency_offset=0):
        if self.data is None:
            raise ValueError("Data is not loaded yet!")
        # NOTE: TABLE NAMES ARE HARD CODED INTO queries.sql
        # DON'T CHANGE THESE UNLESS YOU DO IT IN BOTH FILES
        raw_data = self.data # Make it visible to DuckDB
        configs = self.configs['yellow_red_config'] # Make it visible to DuckDB
        # Run SQL Queries to transform data
        # Each step is an immaterialized view that will be optimized together at the end

        view1 = self.run_duck('detector_with_phase_ON_ONLY', variable1='1.5') #only contains detector on events, shifted by 1.5 seconds for latency
        view2 = self.run_duck('phase_with_detector_ByApproach', 'view1') #contains phase data and detector data together
        view3 = self.run_duck('with_cycle', 'view2')        
        view4 = self.run_duck('red_offset', 'view3')
        return view4.fetchdf()
    

    # Arrival on Green
    def arrival_on_green(self, bin_size=15, latency_offset=0):
        if self.data is None:
            raise ValueError("Data is not loaded yet!")
        # NOTE: TABLE NAMES ARE HARD CODED INTO queries.sql
        # DON'T CHANGE THESE UNLESS YOU DO IT IN BOTH FILES
        raw_data = self.data # Make it visible to DuckDB
        configs = self.configs['arrival_on_green_config'] # Make it visible to DuckDB
        # Run SQL Queries to transform data
        # Each step is an immaterialized view that will be optimized together at the end
        view1 = self.run_duck('detector_with_phase_ON_ONLY', variable1=str(latency_offset)) #only contains detector on events. latency offset=0?
        view2 = self.run_duck('phase_with_detector_ByApproach', 'view1') #contains phase data and detector data together
        view3 = self.run_duck('with_cycle', 'view2')
        view4 = self.run_duck('arrival_on_green', 'view3', variable1=str(bin_size))#set aggregation level to 60 minutes
        return view4.fetchdf()


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