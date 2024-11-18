def generate_id_uniquiness_query(
    read_filepath: str, read_method: str = "read_parquet"
) -> str:
    """Query to verify uniquiness of ID using DuckDB.

    Args:
        read_filepath(str): file path for reading the data.
        read_method(str): Method to read the file. Defaults to `read_parquet`.

    Returns:
        str: generated query.
    """
    return f"""
        SELECT id
        , COUNT(*) AS count
        FROM {read_method}('{read_filepath}')
        GROUP BY id
        HAVING COUNT(*) > 1;
        """


def generate_valid_brewery_type_query(
    read_filepath: str, read_method: str = "read_parquet"
) -> str:
    """Query to verify if the brewery_type is within the defined values
    from the website is valid using DuckDB.

    Args:
        read_filepath(str): file path for reading the data.
        read_method(str): Method to read the file. Defaults to `read_parquet`.

    Returns:
        str: generated query.
    """
    return f"""
        SELECT id
        , name
        , brewery_type
        FROM {read_method}('{read_filepath}')
        WHERE brewery_type NOT IN ('taproom', 'micro', 'contract',
        'location', 'planning', 'bar', 'regional', 'large', 'beergarden', 'brewpub',
        'proprietor', 'closed', 'nano');
        """
