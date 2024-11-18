from typing import Optional

from pydantic import BaseModel


class APIMetadata(BaseModel):
    total: int
    page: int
    per_page: int


class BreweryData(BaseModel):
    id: str
    name: str
    brewery_type: str
    state_province: str
    postal_code: str
    country: str
    state: str
    city: str
    address_1: Optional[str]
    address_2: Optional[str]
    address_3: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    phone: Optional[str]
    website_url: Optional[str]
    street: Optional[str]
