# ATSPM Aggregation

`atspm` is a Python package to transform hi-res ATC signal controller data into aggregate ATSPMs (Automated Traffic Signal Performance Measures). It works on multiple devices/detectors at once.

## Installation

```bash
pip install atspm
```

## Quick Usage Guide

After installing the package, you should be able to run the following code and explore the output files from provided sample data.

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
    'output_dir': 'test_folder',
    'output_to_separate_folders': True,
    'output_format': 'csv', # csv/parquet/json
    'output_file_prefix': 'test_prefix',
    # Performance Measure Settings
    'aggregations': [
        {'name': 'actuations', 'params': {'bin_size': bin_size}},
        {'name': 'yellow_red', 'params': {'bin_size': bin_size, 'latency_offset_seconds': 1.5}},
        {'name': 'arrival_on_green', 'params': {'bin_size': bin_size, 'latency_offset_seconds': 0}},
        #{'name': 'communications', 'params': {'bin_size': bin_size, 'event_codes': '400,503,502'}}, #MaxView Specific
        {'name': 'coordination', 'params': {'bin_size': bin_size}},
        {'name': 'terminations', 'params': {'bin_size': bin_size}},
        {'name': 'split_failures', 'params': {'bin_size': bin_size, 'red_time': 5, 'red_occupancy_threshold': 0.80, 'green_occupancy_threshold': 0.80, 'by_approach': True}},
        {'name': 'ped', 'params': {'bin_size': bin_size}},
        {'name': 'unique_ped', 'params': {'bin_size': bin_size, 'seconds_between_actuations': 15}},
        {'name': 'splits', 'params': {'bin_size': bin_size}},
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

