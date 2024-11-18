from uuid import UUID

import duckdb

from dags.brewery.duckdb.aggregate import generate_aggregate_query
from dags.brewery.duckdb.transform import generate_transform_query


class TestGenerateAggregateQuery:
    def test_query(self):
        conn = duckdb.connect(":memory:")
        query = generate_aggregate_query("tests/dags/sample/raw.json", "read_json_auto")
        conn.execute(query)
        expected_output = [
            {
                "brewery_type": "micro",
                "country": "United States",
                "state": "Wisconsin",
                "city": "Mount Pleasant",
                "count_star()": 1,
            },
            {
                "brewery_type": "micro",
                "country": "United States",
                "state": "Texas",
                "city": "Austin",
                "count_star()": 1,
            },
            {
                "brewery_type": "micro",
                "country": "United States",
                "state": "Oklahoma",
                "city": "Norman",
                "count_star()": 1,
            },
            {
                "brewery_type": "large",
                "country": "United States",
                "state": "Oregon",
                "city": "Portland",
                "count_star()": 1,
            },
            {
                "brewery_type": "large",
                "country": "United States",
                "state": "Oregon",
                "city": "Bend",
                "count_star()": 3,
            },
            {
                "brewery_type": "large",
                "country": "United States",
                "state": "Idaho",
                "city": "Boise",
                "count_star()": 1,
            },
            {
                "brewery_type": "large",
                "country": "United States",
                "state": "Colorado",
                "city": "Denver",
                "count_star()": 1,
            },
            {
                "brewery_type": "large",
                "country": "United States",
                "state": "California",
                "city": "San Diego",
                "count_star()": 1,
            },
        ]

        results = conn.fetchall()
        columns = [desc[0] for desc in conn.description]  # type: ignore
        dict_results = [dict(zip(columns, row)) for row in results]

        assert dict_results == expected_output


class TestGenerateTransformedQuery:
    def test_query(self):
        conn = duckdb.connect(":memory:")
        query = generate_transform_query("tests/dags/sample/raw.json", "read_json_auto")
        conn.execute(query)
        expected_output = [
            {
                "id": UUID("5128df48-79fc-4f0f-8b52-d06be54d0cec"),
                "name": "(405) Brewing Co",
                "address_1": "1716 Topeka St",
                "address_2": None,
                "address_3": None,
                "brewery_type": "micro",
                "city": "norman",
                "state_province": "oklahoma",
                "state": "oklahoma",
                "country": "united states",
                "postal_code": "73069-8224",
                "longitude": -97.46818542480469,
                "latitude": 35.257389068603516,
                "phone": "4058160490",
                "website_url": "http://www.405brewing.com",
            },
            {
                "id": UUID("9c5a66c8-cc13-416f-a5d9-0a769c87d318"),
                "name": "(512) Brewing Co",
                "address_1": "407 Radam Ln Ste F200",
                "address_2": None,
                "address_3": None,
                "brewery_type": "micro",
                "city": "austin",
                "state_province": "texas",
                "state": "texas",
                "country": "united states",
                "postal_code": "78745-1197",
                "longitude": None,
                "latitude": None,
                "phone": "5129211545",
                "website_url": "http://www.512brewing.com",
            },
            {
                "id": UUID("34e8c68b-6146-453f-a4b9-1f6cd99a5ada"),
                "name": "1 of Us Brewing Company",
                "address_1": "8100 Washington Ave",
                "address_2": None,
                "address_3": None,
                "brewery_type": "micro",
                "city": "mount pleasant",
                "state_province": "wisconsin",
                "state": "wisconsin",
                "country": "united states",
                "postal_code": "53406-3920",
                "longitude": -87.88336181640625,
                "latitude": 42.72010803222656,
                "phone": "2624847553",
                "website_url": "https://www.1ofusbrewing.com",
            },
            {
                "id": UUID("ef970757-fe42-416f-931d-722451f1f59c"),
                "name": "10 Barrel Brewing Co",
                "address_1": "1501 E St",
                "address_2": None,
                "address_3": None,
                "brewery_type": "large",
                "city": "san diego",
                "state_province": "california",
                "state": "california",
                "country": "united states",
                "postal_code": "92101-6618",
                "longitude": -117.12959289550781,
                "latitude": 32.714813232421875,
                "phone": "6195782311",
                "website_url": "http://10barrel.com",
            },
            {
                "id": UUID("6d14b220-8926-4521-8d19-b98a2d6ec3db"),
                "name": "10 Barrel Brewing Co",
                "address_1": "62970 18th St",
                "address_2": None,
                "address_3": None,
                "brewery_type": "large",
                "city": "bend",
                "state_province": "oregon",
                "state": "oregon",
                "country": "united states",
                "postal_code": "97701-9847",
                "longitude": -121.28170776367188,
                "latitude": 44.08683395385742,
                "phone": "5415851007",
                "website_url": "http://www.10barrel.com",
            },
            {
                "id": UUID("e2e78bd8-80ff-4a61-a65c-3bfbd9d76ce2"),
                "name": "10 Barrel Brewing Co",
                "address_1": "1135 NW Galveston Ave Ste B",
                "address_2": None,
                "address_3": None,
                "brewery_type": "large",
                "city": "bend",
                "state_province": "oregon",
                "state": "oregon",
                "country": "united states",
                "postal_code": "97703-2465",
                "longitude": -121.32880401611328,
                "latitude": 44.05756378173828,
                "phone": "5415851007",
                "website_url": None,
            },
            {
                "id": UUID("e432899b-7f58-455f-9c7b-9a6e2130a1e0"),
                "name": "10 Barrel Brewing Co",
                "address_1": "1411 NW Flanders St",
                "address_2": None,
                "address_3": None,
                "brewery_type": "large",
                "city": "portland",
                "state_province": "oregon",
                "state": "oregon",
                "country": "united states",
                "postal_code": "97209-2620",
                "longitude": -122.68550872802734,
                "latitude": 45.525978088378906,
                "phone": "5032241700",
                "website_url": "http://www.10barrel.com",
            },
            {
                "id": UUID("9f1852da-c312-42da-9a31-097bac81c4c0"),
                "name": "10 Barrel Brewing Co - Bend Pub",
                "address_1": "62950 NE 18th St",
                "address_2": None,
                "address_3": None,
                "brewery_type": "large",
                "city": "bend",
                "state_province": "oregon",
                "state": "oregon",
                "country": "united states",
                "postal_code": "97701",
                "longitude": -121.28095245361328,
                "latitude": 44.091209411621094,
                "phone": "5415851007",
                "website_url": None,
            },
            {
                "id": UUID("ea4f30c0-bce6-416b-8904-fab4055a7362"),
                "name": "10 Barrel Brewing Co - Boise",
                "address_1": "826 W Bannock St",
                "address_2": None,
                "address_3": None,
                "brewery_type": "large",
                "city": "boise",
                "state_province": "idaho",
                "state": "idaho",
                "country": "united states",
                "postal_code": "83702-5857",
                "longitude": -116.20292663574219,
                "latitude": 43.61851501464844,
                "phone": "2083445870",
                "website_url": "http://www.10barrel.com",
            },
            {
                "id": UUID("1988eb86-f0a2-4674-ba04-02454efa0d31"),
                "name": "10 Barrel Brewing Co - Denver",
                "address_1": "2620 Walnut St",
                "address_2": None,
                "address_3": None,
                "brewery_type": "large",
                "city": "denver",
                "state_province": "colorado",
                "state": "colorado",
                "country": "united states",
                "postal_code": "80205-2231",
                "longitude": -104.98536682128906,
                "latitude": 39.75925064086914,
                "phone": "7205738992",
                "website_url": None,
            },
        ]
        results = conn.fetchall()
        columns = [desc[0] for desc in conn.description]  # type: ignore
        dict_results = [dict(zip(columns, row)) for row in results]

        assert dict_results == expected_output
