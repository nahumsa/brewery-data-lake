def generate_aggregate_query(
    read_filepath: str, read_method: str = "read_parquet"
) -> str:
    """Query to create the aggregation of the quantity
    of brewery types and location using DuckDB.

    Args:
        read_filepath(str): file path for reading the data.
        read_method(str): Method to read the file. Defaults to `read_parquet`.

    Returns:
        str: generated query.
    """
    return f"""
        SELECT 
            brewery_type
            , country
            , state
            , city
            , COUNT(*)
        FROM {read_method}('{read_filepath}')
        GROUP BY
            brewery_type
            , country
            , state
            , city
        ORDER BY
            brewery_type DESC
            , country DESC
            , state DESC
            , city DESC
        """
