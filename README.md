# ATSPM Aggregation

This repository contains code for efficient and scalable aggregation of Automated Traffic Signal Performance Measures (ATSPMs). It so far only includes split failures, but will soon also have Yellow/Red actuations, Percent Arrival on Green, and Platoon Ratio. And volumes. 

## Quick Usage Overview for Split Failures

```python
# Import libraries
import pandas as pd
from Aggregations import Aggregations

# Load hi-res and detector-phase configuration data
raw_data = pd.read_parquet('sample_hi-res_data.parquet')
config = pd.read_parquet('sample_detector-config.parquet')

# Instantiate Aggregations class and load hi-res data and detector configurations into it
aggr = Aggregations(data=raw_data, phase_detector_config=config)

# Return aggregate split failures
sf = aggr.split_failure()

# Plot to inspect results (optional)
aggr.plot_occupancy(sf, DeviceId=240, Phase=1)
```
![Alt text](example-SF-chart.png)


## Background

ATSPMs can be computationally expensive, making it difficult to scale simutaniously accross all traffic signals at an agency. Initially, for-loops were utilized to produce aggregations for a single detector at a time taking several seconds each, and this proved to be way too slow to scale to the entire signal system. That code is still available in the master branch. This branch focuses on vectorizing code to be able to run operations on every detector at every signal at the same time. The operations are done using SQL queries, which are executed using DuckDB, which is a library that is open source, fast, and utilizes all CPU cores. 



<br>
Stay tuned for more!


