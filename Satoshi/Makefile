# Satoshi HFT Infrastructure - Development Makefile
# Provides convenient commands for development, testing, and deployment

.PHONY: help setup clean lint format test test-unit test-integration test-performance
.PHONY: start stop build deploy check metrics coverage docs
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
POETRY := poetry
PROJECT_NAME := satoshi

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

define print_help
	@echo "$(BLUE)Satoshi HFT Infrastructure - Development Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Setup & Environment:$(NC)"
	@echo "  setup           Setup development environment"
	@echo "  check           Check dependencies and system requirements"
	@echo "  clean           Clean build artifacts and cache"
	@echo ""
	@echo "$(GREEN)Development:$(NC)"
	@echo "  lint            Run linting and type checking"
	@echo "  format          Fix code formatting with ruff"
	@echo "  test            Run all tests with coverage"
	@echo "  test-unit       Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-performance Run performance tests only"
	@echo ""
	@echo "$(GREEN)Services:$(NC)"
	@echo "  start           Start all development services"
	@echo "  stop            Stop all development services"
	@echo "  kafka           Start Kafka development cluster"
	@echo "  api             Start FastAPI server"
	@echo "  dagster         Start Dagster development server"
	@echo ""
	@echo "$(GREEN)Build & Deploy:$(NC)"
	@echo "  build           Build project and run quality checks"
	@echo "  deploy-staging  Deploy to staging environment"
	@echo ""
	@echo "$(GREEN)Monitoring:$(NC)"
	@echo "  metrics         Show development metrics"
	@echo "  coverage        Generate and open coverage report"
	@echo "  docs            Generate and serve documentation"
	@echo ""
	@echo "$(GREEN)Examples:$(NC)"
	@echo "  make setup      # Initial project setup"
	@echo "  make lint       # Check code quality"
	@echo "  make test-unit  # Run unit tests"
	@echo "  make start      # Start all services"
endef

help:
	$(call print_help)

# ==============================
# SETUP & ENVIRONMENT
# ==============================

setup: check
	@echo "$(BLUE)[SETUP]$(NC) Setting up development environment..."
	$(POETRY) install --with dev,test
	@if [ -f ".pre-commit-config.yaml" ]; then \
		$(POETRY) run pre-commit install; \
		echo "$(GREEN)[SUCCESS]$(NC) Pre-commit hooks installed"; \
	fi
	@echo "$(GREEN)[SUCCESS]$(NC) Development environment ready"

check:
	@echo "$(BLUE)[CHECK]$(NC) Checking dependencies..."
	@command -v $(PYTHON) >/dev/null 2>&1 || { echo "$(RED)[ERROR]$(NC) Python 3.11+ required"; exit 1; }
	@command -v $(POETRY) >/dev/null 2>&1 || { echo "$(RED)[ERROR]$(NC) Poetry required. Install: curl -sSL https://install.python-poetry.org | python3 -"; exit 1; }
	@echo "$(GREEN)[SUCCESS]$(NC) Dependencies check passed"

clean:
	@echo "$(BLUE)[CLEAN]$(NC) Cleaning build artifacts..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf build/ dist/ *.egg-info/ .coverage htmlcov/ .pytest_cache/ .mypy_cache/
	@rm -f .api.pid .dagster.pid
	@echo "$(GREEN)[SUCCESS]$(NC) Project cleaned"

# ==============================
# DEVELOPMENT
# ==============================

lint:
	@echo "$(BLUE)[LINT]$(NC) Running code quality checks..."
	$(POETRY) run ruff check .
	$(POETRY) run ruff format --check .
	$(POETRY) run mypy engines/ infra/ orchestration/ --ignore-missing-imports
	@echo "$(GREEN)[SUCCESS]$(NC) Linting passed"

format:
	@echo "$(BLUE)[FORMAT]$(NC) Fixing code formatting..."
	$(POETRY) run ruff format .
	$(POETRY) run ruff check --fix .
	@echo "$(GREEN)[SUCCESS]$(NC) Code formatting fixed"

test: test-unit
	@echo "$(GREEN)[SUCCESS]$(NC) All tests completed"

test-unit:
	@echo "$(BLUE)[TEST]$(NC) Running unit tests..."
	$(POETRY) run pytest tests/unit/ -v --tb=short --cov=engines --cov=infra --cov=orchestration --cov-report=html --cov-report=term-missing

test-integration:
	@echo "$(BLUE)[TEST]$(NC) Running integration tests..."
	$(POETRY) run pytest tests/integration/ -v --tb=short

test-performance:
	@echo "$(BLUE)[TEST]$(NC) Running performance tests..."
	$(POETRY) run pytest tests/performance/ -v --tb=short -m performance

test-all:
	@echo "$(BLUE)[TEST]$(NC) Running all tests..."
	$(POETRY) run pytest tests/ -v --tb=short --cov=engines --cov=infra --cov=orchestration --cov-report=html --cov-report=term-missing

# ==============================
# SERVICES
# ==============================

start: kafka api dagster
	@echo "$(GREEN)[SUCCESS]$(NC) All services started"
	@echo "$(BLUE)[INFO]$(NC) API Documentation: http://localhost:8000/docs"
	@echo "$(BLUE)[INFO]$(NC) Dagster UI: http://localhost:3000"
	@echo "$(BLUE)[INFO]$(NC) Kafka UI: http://localhost:8080"

stop:
	@echo "$(BLUE)[STOP]$(NC) Stopping development services..."
	@if [ -f ".api.pid" ]; then \
		kill `cat .api.pid` 2>/dev/null || true; \
		rm -f .api.pid; \
		echo "$(GREEN)[SUCCESS]$(NC) API server stopped"; \
	fi
	@if [ -f ".dagster.pid" ]; then \
		kill `cat .dagster.pid` 2>/dev/null || true; \
		rm -f .dagster.pid; \
		echo "$(GREEN)[SUCCESS]$(NC) Dagster server stopped"; \
	fi
	@cd infra/bus && docker-compose -f docker-compose.kafka.yml down 2>/dev/null || true
	@echo "$(GREEN)[SUCCESS]$(NC) All services stopped"

kafka:
	@echo "$(BLUE)[KAFKA]$(NC) Starting Kafka development cluster..."
	@cd infra/bus && ./start-kafka-dev.sh
	@echo "$(GREEN)[SUCCESS]$(NC) Kafka cluster ready"

api:
	@echo "$(BLUE)[API]$(NC) Starting FastAPI server..."
	@$(POETRY) run uvicorn infra.api.main:app --reload --host 0.0.0.0 --port 8000 &
	@echo $$! > .api.pid
	@echo "$(GREEN)[SUCCESS]$(NC) API server started on http://localhost:8000"

dagster:
	@echo "$(BLUE)[DAGSTER]$(NC) Starting Dagster development server..."
	@$(POETRY) run dagster dev -f orchestration/pipeline.py --port 3000 &
	@echo $$! > .dagster.pid
	@echo "$(GREEN)[SUCCESS]$(NC) Dagster UI started on http://localhost:3000"

# ==============================
# BUILD & DEPLOY
# ==============================

build: lint test-unit
	@echo "$(BLUE)[BUILD]$(NC) Building project..."
	$(POETRY) build
	@echo "$(GREEN)[SUCCESS]$(NC) Project built successfully"

deploy-staging: build
	@echo "$(BLUE)[DEPLOY]$(NC) Deploying to staging..."
	@echo "$(YELLOW)[TODO]$(NC) Implement staging deployment"
	@echo "$(GREEN)[SUCCESS]$(NC) Deployment completed"

# ==============================
# MONITORING
# ==============================

metrics:
	@echo "$(BLUE)[METRICS]$(NC) Development metrics:"
	@echo "  Python files: $$(find engines/ infra/ orchestration/ -name '*.py' | wc -l)"
	@echo "  Total lines: $$(find engines/ infra/ orchestration/ -name '*.py' -exec wc -l {} + | tail -1 | awk '{print $$1}')"
	@echo "  Test files: $$(find tests/ -name '*.py' | wc -l)"
	@echo "  Dependencies: $$($(POETRY) show | wc -l)"
	@if [ -f "htmlcov/index.html" ]; then \
		echo "  Coverage report: file://$$(pwd)/htmlcov/index.html"; \
	fi

coverage: test-unit
	@echo "$(BLUE)[COVERAGE]$(NC) Opening coverage report..."
	@if command -v open >/dev/null 2>&1; then \
		open htmlcov/index.html; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open htmlcov/index.html; \
	else \
		echo "$(BLUE)[INFO]$(NC) Coverage report: file://$$(pwd)/htmlcov/index.html"; \
	fi

docs:
	@echo "$(BLUE)[DOCS]$(NC) Generating documentation..."
	@echo "$(YELLOW)[TODO]$(NC) Implement documentation generation"

# ==============================
# DEVELOPMENT SHORTCUTS
# ==============================

# Quick development cycle
dev: format lint test-unit
	@echo "$(GREEN)[DEV]$(NC) Development cycle complete"

# Full CI/CD simulation
ci: clean setup lint test-all build
	@echo "$(GREEN)[CI]$(NC) CI/CD simulation complete"

# Performance benchmark
benchmark: test-performance
	@echo "$(GREEN)[BENCHMARK]$(NC) Performance benchmark complete"

# Install development tools
install-tools:
	@echo "$(BLUE)[TOOLS]$(NC) Installing development tools..."
	@command -v poetry >/dev/null 2>&1 || curl -sSL https://install.python-poetry.org | python3 -
	@command -v docker >/dev/null 2>&1 || echo "$(YELLOW)[WARNING]$(NC) Docker not found. Please install Docker manually."
	@echo "$(GREEN)[SUCCESS]$(NC) Development tools ready"
