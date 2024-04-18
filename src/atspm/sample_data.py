# sample_data.py inside the atspm package
import pandas as pd
import os

# Assuming this file is in the same directory as the `data` directory
data_dir = os.path.join(os.path.dirname(__file__), 'data')

class SampleData:
    def __init__(self):
        self.config = pd.read_parquet(os.path.join(data_dir, 'sample_config.parquet'))
        self.data = pd.read_parquet(os.path.join(data_dir, 'sample_raw_data.parquet'))

# Create an instance of the class
sample_data = SampleData()