PROJECT = "brewery-data-lake"
VERSION = "1.0.0"

setup:
	pip install -r requirements.dev.txt

test:
	pytest -vv

fmt:
	isort dags/ tests/ plugins/
	black dags/ tests/ plugins/
	ruff check --fix dags/ tests/ plugins/

check_style:
	isort --check --diff dags/ tests/ plugins/
	black --check --diff dags/ tests/ plugins/
	ruff check dags/ tests/ plugins/
