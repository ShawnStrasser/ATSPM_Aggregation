import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import duckdb
import warnings

# for optional plotting
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from matplotlib.lines import Line2D

class Aggregations:
    def __init__(self, phase_detector_config, data=None, mssql_server=None, mssql_database=None, duckdb_threads=None):

        if isinstance(duckdb_threads, int):
            duckdb.query(f"SET threads to {duckdb_threads}")
            #print(duckdb.query(f"SELECT current_setting('threads');"))
        
        self.config = phase_detector_config
        try:
            # Filter configs to Presence function for split failures only
            # Only include only necessary columns to assign detector to phase
            self.split_fail_config = phase_detector_config[phase_detector_config.Function == 'Presence'][['Phase', 'Parameter', 'DeviceId']]
            self.split_fail_devices = set(self.split_fail_config.DeviceId)
        except:
            print('No Presence Detection Found!')
        
        self.data = data
        self.mssql_server = mssql_server
        self.mssql_database = mssql_database

        # Load SQL Queries Into Dicitonary
        with open('queries.sql', 'r') as file:
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
    
    # Aggregate Split Failures, by approach is default, set to false to do by lane
    def split_failure(self, by_approach=True, green_occupancy_threshold=0.80, red_occupancy_threshold=0.80):
        if self.data is None:
            raise ValueError("Data is not loaded yet!")
        # Now transform data into split failures
        # NOTE: TABLE NAMES ARE HARD CODED INTO queries.sql
        # DON'T CHANGE THESE UNLESS YOU DO IT IN BOTH FILES
        raw_data = self.data # Make it visible to DuckDB
        sf_configs = self.split_fail_config # Make it visible to DuckDB
        detector_with_phase = duckdb.query(self.queries_dict['detector_with_phase'])
        impute_actuations = duckdb.query(self.queries_dict['impute_actuations'])

        # by_approach combines detectors accross phase
        if by_approach: 
            combine_detectors_ByApproach = duckdb.query(self.queries_dict['combine_detectors_ByApproach'])
            phase_with_detector = duckdb.query(self.queries_dict['phase_with_detector_ByApproach'])            
        else:
            phase_with_detector = duckdb.query(self.queries_dict['phase_with_detector_ByLane'])

        # Remaining queries are same for by approach or by lane
        with_barrier = duckdb.query(self.queries_dict['with_barrier'])
        with_cycle = duckdb.query(self.queries_dict['with_cycle'])
        time_diff = duckdb.query(self.queries_dict['time_diff'])
        agg = duckdb.query(self.queries_dict['agg'])
        final = duckdb.query(self.queries_dict['final']).fetchdf()
        final['Spit_Failure'] = np.where((final.Red_Occupancy >= red_occupancy_threshold) & 
                                        (final.Green_Occupancy >= green_occupancy_threshold), True, False)
        return final
    


    # Optional, plot occupancy
    def plot_occupancy(self, sf, DeviceId, Phase=None, Detector=None, green_occupancy_threshold=0.79):
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
        timestamps = sf_filtered[(sf_filtered['Green_Occupancy'] > green_occupancy_threshold) & (sf_filtered['Red_Occupancy'] > 0.79)].index

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
