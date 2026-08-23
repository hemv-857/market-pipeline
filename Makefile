.PHONY: setup test demo calibrate lint

setup:
	pip install -e ".[dev]"

test:
	pytest

demo:
	python -m mktflow.cli demo --paths 40000

calibrate:
	python -m mktflow.cli calibrate

lint:
	ruff check src tests
