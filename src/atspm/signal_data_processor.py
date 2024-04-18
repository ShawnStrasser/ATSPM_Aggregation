import duckdb
from .data_loader import load_data
from .data_aggregator import aggregate_data
from .data_saver import save_data

class SignalDataProcessor:
    '''
    A class used to process signal data, turning raw hi-res data into aggregated data.

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
        # detector_config_path is optional
        self.detector_config = None
        # Extract parameters from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Establish a connection to the database
        self.conn = duckdb.connect()

    def load(self):
        """Loads raw data and detector configuration into DuckDB tables."""
        load_data(self.conn,
                self.raw_data,
                self.detector_config)
        
    def aggregate(self):
        """Runs all aggregations."""
        for aggregation in self.aggregations:
            aggregate_data(self.conn,
                    aggregation['name'],
                    **aggregation['params'])
    
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