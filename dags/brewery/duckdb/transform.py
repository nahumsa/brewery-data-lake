def generate_transform_query(
    read_filepath: str, read_method: str = "read_json_auto"
) -> str:
    """Query to transform JSON data using DuckDB.

    Args:
        read_filepath(str): file path for reading the data.
        read_method(str): Method to read the file. Defaults to `read_json_auto`.

    Returns:
        str: generated query.
    """
    return f"""
        SELECT
            id
            , name
            , address_1
            , address_2
            , address_3
            , LOWER(brewery_type) AS brewery_type
            , LOWER(city) AS city
            , LOWER(state_province) AS state_province
            , LOWER(state) AS state
            , LOWER(country) AS country
            , postal_code
            , CAST(longitude AS FLOAT) AS longitude
            , CAST(latitude AS FLOAT) AS latitude
            , phone
            , CASE
                WHEN website_url LIKE '%@gmail.com%' THEN NULL
                ELSE website_url
            END AS website_url
        FROM {read_method}('{read_filepath}')
        """
