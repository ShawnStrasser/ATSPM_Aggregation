def load_data(conn, raw_data, detector_config):
    """
    Loads raw data and detector configuration into DuckDB tables.

    This function creates or replaces two tables in the DuckDB database: 'raw_data' and 'detector_config'.
    The data for these tables is loaded from the provided raw_data and detector_config parameters.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        The connection to the DuckDB database.
    raw_data : str or pandas.DataFrame
        The raw data to be loaded. This can be a string representing the path to a file containing the raw data,
        or a pandas DataFrame containing the raw data.
    detector_config : str or pandas.DataFrame, optional
        The detector configuration to be loaded. This can be a string representing the path to a file containing the detector configuration,
        or a pandas DataFrame containing the detector configuration. If this parameter is None, no detector configuration is loaded.

    Raises
    ------
    ValueError
        If raw_data or detector_config is a string and the file does not exist.
    """
    # Check if raw_data is an instance of str
    if isinstance(raw_data, str):
        conn.execute(f"CREATE OR REPLACE TABLE raw_data AS SELECT * FROM '{raw_data}'")
    else:
        conn.execute(f"CREATE OR REPLACE TABLE raw_data AS SELECT * FROM raw_data")
    print("Raw data loaded successfully")
    # Check that detector_config is not None
    if detector_config is not None:
        if isinstance(detector_config, str):
            conn.execute(f"CREATE OR REPLACE TABLE detector_config AS SELECT * FROM '{detector_config}'")
        else:
            conn.execute(f"CREATE OR REPLACE TABLE detector_config AS SELECT * FROM detector_config")