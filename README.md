# ATSPM Aggregation

`atspm` is a production ready Python package to transform hi-res ATC signal controller data into aggregate ATSPMs (Automated Traffic Signal Performance Measures). It runs locally using the [DuckDB](https://duckdb.org/) SQL engine, but future development could include integration with [Ibis](https://ibis-project.org/) for compatability with any SQL backend. Ideas and contributions are welcome!

## Aggregate Performance Measures
Documentation is coming, for now these are the available performance measures:
- Actuations
- Arrival on Green
- Communications (MaxView Specific, otherwise "Has Data" tells when controller generated data)
- Coordination (MaxTime Specific)
- Detector Faults
- Detector Health
- Pedestrian Actuations
- Pedestrian Unique Actuations
- *Total Pedestrian Delay is Coming Soon*
- *Pedestrian Detector Health is Coming Soon*
- Pedestrian Services
- Pedestrian Estimated Volumes
- Split Failures
- Splits (MaxTime Specific)
- Terminations
- Timeline
- Yellow and Red Actuations



## Installation

```bash
pip install atspm
```

## Usage

After installing the package, you should be able to run the following code and explore the output files from provided sample data.

Try out a self-contained example in Colab!<br> [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/14SPXPjpwbBEPpjKBN5s4LoqtHWSllvip?usp=sharing)


Here is a simple, more thorough documentation is coming soon.
```python
# Import libraries
from atspm import SignalDataProcessor, sample_data

# Set bin size to 15 minutes
bin_size = 15

# Set up all parameters
params = {
    # Global Settings
    'raw_data': sample_data.data, # dataframe or file path to csv/parquet/json
    'detector_config': sample_data.config,
    'bin_size': 15, # in minutes
    'output_dir': 'test_folder',
    'output_to_separate_folders': True,
    'output_format': 'csv', # csv/parquet/json
    'output_file_prefix': 'test_prefix',
    'remove_incomplete': True, # Remove periods with incomplete data by joining to the has_data table
    # Performance Measures
    'aggregations': [
        {'name': 'has_data', 'params': {'no_data_min': 3, 'min_data_points': 10}}, # in minutes, ie remove bins with less than 10 rows every 3 minutes
        {'name': 'actuations', 'params': {}},
        {'name': 'arrival_on_green', 'params': {'latency_offset_seconds': 0}},
        {'name': 'communications', 'params': {'event_codes': '400,503,502'}}, # MAXVIEW Specific
        {'name': 'coordination', 'params': {}}, # MAXTIME Specific
        # detector faults and detector health almost ready
        {'name': 'ped', 'params': {}},
        {'name': 'split_failures', 'params': {'red_time': 5, 'red_occupancy_threshold': 0.80, 'green_occupancy_threshold': 0.70, 'by_approach': True}},
        {'name': 'splits', 'params': {}}, # MAXTIME Specific
        {'name': 'terminations', 'params': {}},
        {'name': 'unique_ped', 'params': {'seconds_between_actuations': 15}},
        {'name': 'yellow_red', 'params': {'latency_offset_seconds': 1.5, 'min_red_offset': -8}}, # min_red_offset is optional, it filters out actuations occuring -n seconds before start of red
    ]
}

processor = SignalDataProcessor(**params)
processor.run()
```

After running the `SignalDataProcessor` with the provided parameters, the output directory (`output_dir`) will have the following structure:

```bash
test_folder/
├── actuations/
├── yellow_red/
├── arrival_on_green/
├── coordination/
├── terminations/
├── split_failures/
├── ped/
├── unique_ped/
└── splits/
```
Inside each folder, there will be a CSV file named `test_prefix.csv` with the aggregated performance data. The prefix can be used for example by setting it to the run date. Or you can output everything to a single folder.

A good way to use the data is to output as parquet to separate folders, and then a data visualization tool like Power BI can read in all the files in each folder and create a dashboard. For example see: [Oregon DOT ATSPM Dashboard](https://app.powerbigov.us/view?r=eyJrIjoiNzhmNTUzNDItMzkzNi00YzZhLTkyYWQtYzM1OGExMDk3Zjk1IiwidCI6IjI4YjBkMDEzLTQ2YmMtNGE2NC04ZDg2LTFjOGEzMWNmNTkwZCJ9)

