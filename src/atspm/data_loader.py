def load_data(conn,
              raw_data=None,
              detector_config=None,
              unmatched_events=None):
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
    unmatched_events : str or pandas.DataFrame, optional
        The unmatched events to be loaded. This can be a string representing the path to a file containing the unmatched events,
        or a pandas DataFrame containing the unmatched events. If this parameter is None, no unmatched events are loaded.
    """
    # Load Raw Data
    load_sql = """
        CREATE OR REPLACE TABLE raw_data AS
        SELECT TimeStamp, DeviceId, EventId::INT16 as EventId, Parameter::INT16 as Parameter
        """
    if raw_data is not None:
        if isinstance(raw_data, str):
            conn.execute(f"{load_sql} FROM '{raw_data}'")
        else:
            conn.execute(f"{load_sql} FROM raw_data")

    # Load Configurations (if provided)
    load_sql = """
        CREATE OR REPLACE TABLE detector_config AS
        SELECT DeviceId, Phase::INT16 as Phase, Parameter::INT16 as Parameter, Function::STRING as Function
        """
    if detector_config is not None:
        if isinstance(detector_config, str):
            conn.execute(f"{load_sql} FROM '{detector_config}'")
        else:
            conn.execute(f"{load_sql} FROM detector_config")

    # Load unmatched_events (if provided)
    load_sql = """
        CREATE OR REPLACE TABLE unmatched_events AS
        SELECT DeviceId, EventId::INT16 as EventId, Parameter::INT16 as Parameter
        """
    if unmatched_events is not None:
        if isinstance(unmatched_events, str):
            conn.execute(f"{load_sql} FROM '{unmatched_events}'")
        else:
            conn.execute(f"{load_sql} FROM unmatched_events")
        # Create a view that unions the raw_data and unmatched_events tables
        conn.execute("CREATE OR REPLACE VIEW all_events AS SELECT * FROM raw_data UNION ALL SELECT * FROM unmatched_events")
