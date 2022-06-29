# This script is to be run each morning, to update signal controller data

import DataProcessing as dp
# Get aggregate data tables from SQL Server
dp.Get_SQL_Data().update_data('Actuations')
dp.Get_SQL_Data().update_data('Communications')
dp.Get_SQL_Data().update_data('MaxOuts')
dp.Get_SQL_Data(server='SP2SQLMAX101', database='MaxView_EventLog').update_data('MaxTimeFaults')

# Run Detector Health Analytics
dp.DetectorHealth().update_data()

# Get Travel Time data from RITIS
dp.Get_RITIS_Data().update_data()

# Travel Time Analytics
#a = dp.Analytics()
#days = 7*5
#date = '2022-06-20'
#folder = 'TravelTime'
#r = a.load_data(folder=folder, date=date, num_days=days)
#r = a.decompose(r)
#r = a.add_seg_groups(r)
#r = a.find_anomalies(r)
#r.to_parquet('//scdata2/signalshar/Data_Analysis/Data/Performance/TravelTime.parquet')
