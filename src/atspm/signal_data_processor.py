import duckdb
import time
import traffic_anomaly
from .data_loader import load_data
from .data_aggregator import aggregate_data
from .data_saver import save_data


class SignalDataProcessor:
    '''
    Main class in atspm package, used to call other class functions to process signal data, turning raw hi-res data into aggregated data.

    Attributes
    ----------
    raw_data_path : str
        The path to the raw data file.
    detector_config_path : str
        The path to the detector configuration file.
    output_dir : str
        The directory where the output files will be saved.
    output_to_separate_folders : bool
        If True, output files will be saved in separate folders.
    output_format : str
        The format of the output files. Options are "parquet", "csv", etc.
    aggregations : list
        A list of dictionaries, each containing the name of an aggregation function and its parameters.

    Methods
    -------
    run():
        Loads the data, runs the aggregations, saves the output, and closes the database connection.
    '''

    def __init__(self, **kwargs):
        """Initializes the SignalDataProcessor with the provided keyword arguments."""
        # Optional parameters
        self.raw_data = None
        self.detector_config = None
        self.unmatched_events = None # For timeline aggregation, future use for other aggregations that track phase state (split failure, arrival on green, etc.)
        self.binned_actuations = None # For detector_health aggregation
        self.device_groups = None # For detector_health aggregation if groups are provided
        self.remove_incomplete = False
        
        # Extract parameters from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Check for valid bin_size and no_data_min combo
        if self.remove_incomplete:
            # extract has_data parameters
            no_data_min = next(x['params']['no_data_min'] for x in self.aggregations if x['name'] == 'has_data')
            assert self.bin_size % no_data_min == 0, "bin_size / no_data_min must be a whole number"
            # Make sure that has_data is the first aggregation
            idx = [d['name'] for d in self.aggregations].index('has_data')
            self.aggregations.insert(0, self.aggregations.pop(idx))

        # Check if detector_health is in aggregations
        if any(d['name'] == 'detector_health' for d in self.aggregations):
            try:
                idx = [d['name'] for d in self.aggregations].index('detector_health')
                self.binned_actuations = self.aggregations[idx]['params']['data']
                self.device_groups = self.aggregations[idx]['params']['device_groups']
            except KeyError:
                raise ValueError("detector_health aggregation requires 'data' and 'device_groups' parameters. 'device_groups' can be set to None.")
     
        # Establish a connection to the database
        self.conn = duckdb.connect()
        # Track whether data has been loaded
        self.data_loaded = False

    def load(self):
        """Loads raw data and detector configuration into DuckDB tables."""
        if self.data_loaded:
            print("Data already loaded! Reinstantiate the class to reload data.")
            return
        load_data(self.conn,
                self.raw_data,
                self.detector_config,
                self.unmatched_events)
        # delete self.raw_data and self.detector_config to free up memory
        del self.raw_data
        del self.detector_config
        self.data_loaded = True
        self.min_timestamp = self.conn.execute("SELECT MIN(timestamp) FROM raw_data").fetchone()[0]
        self.max_timestamp = self.conn.execute("SELECT MAX(timestamp) FROM raw_data").fetchone()[0]
        print(f'Data loaded from {self.min_timestamp} to {self.max_timestamp}')
        
    def aggregate(self):
        """Runs all aggregations."""
        # Instantiate a dictionary to store runtimes
        self.runtimes = {}
        for aggregation in self.aggregations:
            start_time = time.time()

            #######################
            ### Detector Health ###
            ### Does Not Use aggregate_data function
            ### Relies on traffic-anomaly package instead
            # Decompose data
            if aggregation['name'] == 'detector_health':
                decomp = traffic_anomaly.median_decompose(
                    self.binned_actuations,
                    **aggregation['params']['decompose_params']
                )
                del self.binned_actuations
                # Join groups to decomp
                if self.device_groups is not None:
                    device_groups = self.device_groups # DuckDB needs a direct pointer to see the table
                    decomp = self.conn.sql("SELECT * FROM decomp NATURAL JOIN device_groups").df()
                    # Exclude group_grouping_columns in anomaly_params
                    exclude_col = ', '.join(["'{}'".format(x) for x in aggregation['params']['anomaly_params']['group_grouping_columns']])
                    exclude_col = f"EXCLUDE ({exclude_col})"
                    
                else:
                    exclude_col = ""
                # Find Anomalies
                anomaly = traffic_anomaly.find_anomaly(
                    decomposed_data=decomp,
                    **aggregation['params']['anomaly_params']
                )
                # Extract max date from anomaly table and subtract return_last_n_days
                sql = f"""
                    SELECT CAST(MAX(TimeStamp)::DATE - INTERVAL '{aggregation['params']['return_last_n_days']-1}' DAY AS VARCHAR) AS max_date_minus_one
                    FROM anomaly
                    """
                max_date = self.conn.query(sql).fetchone()[0]

                # Save anomaly table to DuckDB
                query = f"""CREATE OR REPLACE TABLE detector_health AS
                        SELECT * {exclude_col}
                        FROM anomaly
                        WHERE TimeStamp >= '{max_date}'
                        """
                self.conn.execute(query)
                # no external sql file like other aggregations, so just continue
                end_time = time.time()
                self.runtimes[aggregation['name']] = end_time - start_time
                continue

            # Add bin_size and remove_incomplete to params
            aggregation['params']['bin_size'] = self.bin_size
            aggregation['params']['remove_incomplete'] = self.remove_incomplete
            aggregation['params']['from_table'] = 'raw_data'

            #######################
            ### Detector Faults / Full Pedestrian ###
            # Add min_timestamp and max_timestamp to params if detector_faults or full_ped
            if aggregation['name'] == 'detector_faults' or aggregation['name'] == 'full_ped':
                aggregation['params']['min_timestamp'] = self.min_timestamp
                aggregation['params']['max_timestamp'] = self.max_timestamp

            #######################
            ### Timeline ##########
            # If timeline and self.unmatched_events is not None, update from_table to 'all_events' view
            if aggregation['name'] == 'timeline' and self.unmatched_events is not None:
                aggregation['params']['from_table'] = 'all_events'

            #######################
            ### Run Aggregation ###
            aggregate_data(self.conn,
                    aggregation['name'],
                    **aggregation['params'])
            end_time = time.time()
            self.runtimes[aggregation['name']] = end_time - start_time

        print(f"\n\nTotal aggregation runtime: {sum(self.runtimes.values()):.2f} seconds.")
        print("\nIndividual Query Runtimes:")
        for name, runtime in self.runtimes.items():
            print(f"{name}: {runtime:.2f} seconds")
    
    def save(self):
        """Saves the processed data."""
        save_data(**self.__dict__)
        
    def close(self):
        """Closes the database connection."""
        self.conn.close()

    def run(self):
        """Runs the complete data processing pipeline."""
        self.load()
        self.aggregate()
        self.save()
        self.close()