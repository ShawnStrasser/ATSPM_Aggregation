# This script is to be run each morning, to update signal controller data

import DataProcessing as dp
# Get aggregate data tables from SQL Server
dp.Get_SQL_Data().update_data('Actuations')
dp.Get_SQL_Data().update_data('Communications')
dp.Get_SQL_Data().update_data('MaxOuts')
dp.Get_SQL_Data(server='SP2SQLMAX101', database='MaxView_EventLog').update_data('MaxTimeFaults')

# Get Travel Time data from RITIS
dp.Get_RITIS_Data().update_data()
