PROJECT = "brewery-data-lake"
VERSION = "1.0.0"

test:
	pip install -r requirements.dev.txt
	pytest -vv
