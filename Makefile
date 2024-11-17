PROJECT = "brewery-data-lake"
VERSION = "1.0.0"

setup:
	pip install -r requirements.dev.txt

test:
	pytest -vv
