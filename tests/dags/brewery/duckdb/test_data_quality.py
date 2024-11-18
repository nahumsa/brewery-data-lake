from uuid import UUID

import duckdb

from dags.brewery.duckdb.data_quality import (
    generate_id_uniquiness_query,
    generate_valid_brewery_type_query,
)


class TestGenerateIDUniquinessQuery:
    def test_no_duplicate(self):
        conn = duckdb.connect(":memory:")
        query = generate_id_uniquiness_query(
            "tests/dags/sample/raw.json", "read_json_auto"
        )
        conn.execute(query)
        expected_output = []
        results = conn.fetchall()
        columns = [desc[0] for desc in conn.description]  # type: ignore
        dict_results = [dict(zip(columns, row)) for row in results]

        assert dict_results == expected_output

    def test_duplicate_entries(self):
        conn = duckdb.connect(":memory:")
        query = generate_id_uniquiness_query(
            "tests/dags/sample/duplicated.json", "read_json_auto"
        )
        conn.execute(query)
        expected_output = [
            {
                "id": UUID("5128df48-79fc-4f0f-8b52-d06be54d0cec"),
                "count": 2,
            }
        ]
        results = conn.fetchall()
        columns = [desc[0] for desc in conn.description]  # type: ignore
        dict_results = [dict(zip(columns, row)) for row in results]

        assert dict_results == expected_output


class TestGenerateValidURLQuery:
    def test_valid_url(self):
        conn = duckdb.connect(":memory:")
        query = generate_valid_brewery_type_query(
            "tests/dags/sample/raw.json", "read_json_auto"
        )
        conn.execute(query)
        expected_output = []
        results = conn.fetchall()
        columns = [desc[0] for desc in conn.description]  # type: ignore
        dict_results = [dict(zip(columns, row)) for row in results]

        assert dict_results == expected_output

    def test_invalid_url(self):
        conn = duckdb.connect(":memory:")
        query = generate_valid_brewery_type_query(
            "tests/dags/sample/invalid_brewery_type.json", "read_json_auto"
        )
        conn.execute(query)
        expected_output = [
            {
                "id": UUID("5128df48-79fc-4f0f-8b52-d06be54d0cec"),
                "name": "(405) Brewing Co",
                "brewery_type": "any",
            }
        ]
        results = conn.fetchall()
        columns = [desc[0] for desc in conn.description]  # type: ignore
        dict_results = [dict(zip(columns, row)) for row in results]

        assert dict_results == expected_output
