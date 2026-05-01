.PHONY: setup install test test-unit test-integration lint format docker-up docker-down clean pipeline research dagster-dev dagster-webserver dagster-daemon help

VENV_NAME = jurisdiction-env
PYTHON = $(VENV_NAME)/bin/python
PIP = $(VENV_NAME)/bin/pip

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Create virtual environment and install all dependencies
	python3.9 -m venv $(VENV_NAME)
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements-dev.txt
	@echo "Activate with: source $(VENV_NAME)/bin/activate"

install: ## Install dependencies into existing virtual environment
	$(PIP) install -r requirements-dev.txt

test: ## Run all tests
	$(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing

test-unit: ## Run unit tests only (no Docker or network required)
	$(PYTHON) -m pytest tests/unit/ -v

test-integration: ## Run integration tests (requires Docker services)
	$(PYTHON) -m pytest tests/integration/ -v -m integration

lint: ## Run linting and type checking
	$(PYTHON) -m ruff check src/ tests/
	$(PYTHON) -m mypy src/

format: ## Auto-format code
	$(PYTHON) -m black src/ tests/
	$(PYTHON) -m ruff check --fix src/ tests/

docker-up: ## Start local Docker services (PostgreSQL)
	docker compose -f docker-compose.local.yml up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3
	@docker compose -f docker-compose.local.yml exec postgres pg_isready -U jurisdiction_user -d jurisdiction_db

docker-down: ## Stop local Docker services
	docker compose -f docker-compose.local.yml down

docker-reset: ## Stop services and remove volumes (full reset)
	docker compose -f docker-compose.local.yml down -v

migrate: ## Run Alembic migrations
	$(PYTHON) -m alembic -c database/migrations/alembic.ini upgrade head

pipeline: ## Run the full jurisdiction taxonomy pipeline
	$(PYTHON) -m src.pipeline.orchestrator

research: ## Run the state research tool
	$(PYTHON) -m src.research.state_researcher

dagster-dev: ## Start Dagster webserver + daemon for local development
	@mkdir -p .dagster_home
	DAGSTER_HOME=$(CURDIR)/.dagster_home dagster dev -w workspace.yaml

dagster-webserver: ## Start Dagster webserver only (no daemon)
	@mkdir -p .dagster_home
	DAGSTER_HOME=$(CURDIR)/.dagster_home dagster-webserver -w workspace.yaml -p 3000

dagster-daemon: ## Start Dagster daemon (for schedules/sensors)
	@mkdir -p .dagster_home
	DAGSTER_HOME=$(CURDIR)/.dagster_home dagster-daemon run -w workspace.yaml

clean: ## Remove cache, test artifacts, and compiled files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache htmlcov .coverage .mypy_cache .ruff_cache
	rm -rf data/cache/* data/output/*
