VENV ?= .venv
PYTHON ?= $(VENV)/bin/python
PIP := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest
RUFF := $(PYTHON) -m ruff
MYPY := $(PYTHON) -m mypy

.PHONY: install test lint fmt docker-build

install:
	python3.11 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

test:
	$(PYTEST)

lint:
	$(RUFF) check src tests
	$(MYPY) --strict src

fmt:
	$(RUFF) format src tests

docker-build:
	docker compose build
