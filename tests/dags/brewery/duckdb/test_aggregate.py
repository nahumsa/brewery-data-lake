import duckdb

from dags.brewery.duckdb.aggregate import generate_aggregate_query


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
