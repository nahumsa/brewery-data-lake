import requests
from brewery.data_model.brewery_api import APIMetadata, BreweryData
from pydantic import TypeAdapter


def get_api_metadata() -> APIMetadata:
    """Fetches metadata from the openbrewerydb API and converts to a pydantic
    model.

    Returns:
        APIMetadata: Information regarding the number of entries.
    """
    url = "https://api.openbrewerydb.org/v1/breweries/meta"
    return APIMetadata(**requests.request("GET", url).json())


def get_api_data(page: int, per_page: int = 50) -> list[BreweryData]:
    """Fetches data from the openbrewerydb API and converts to a pydantic
    model.

    Args:
        page(int): Page to get data.
        per_page(int): Number of entries per page. Defaults to `50`.

    Returns:
        list[BreweryData]: List of entries from the API.
    """
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.request("GET", url, params={"per_page": per_page, "page": page})
    return TypeAdapter(list[BreweryData]).validate_python(response.json())
