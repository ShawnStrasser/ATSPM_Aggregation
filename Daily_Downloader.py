# This script is to be run each morning, to update signal controller data

from GetData import GetData
GetData().update_data('Actuations')
GetData().update_data('Communications')
GetData().update_data('MaxOuts')