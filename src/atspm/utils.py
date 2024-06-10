import numpy as np

def undo_rolling_sum(data):
    '''
    Function to undo a rolling sum on a Numpy array
    Array must be in reverse order
    This is for estimated pedestrian volumes,
    to convert hourly rolling volume estimates to 15-minute volume estimates.
    '''
    values = data.values
    result = np.zeros_like(values)
    result[0] = values[0]
    for i in range(1, len(values)):
        start = max(0, i-3)
        result[i] = values[i] - np.sum(result[start:i])
    # Replace negative values with 0
    result[result < 0] = 0
    # Round to single decimal place
    result = np.round(result, 1)
    return result


# round datetime object down to nearest 15 minutes using datetime
def round_down_15(dt):
    minutes = (dt.minute // 15) * 15
    return dt.replace(minute=minutes, second=0, microsecond=0)