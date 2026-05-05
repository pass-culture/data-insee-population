.PHONY: install dev test test-integration lint format run info clean help
.DEFAULT_GOAL := run

UV := uv

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies
	$(UV) sync

dev:  ## Install dev dependencies
	$(UV) sync --all-extras

test:  ## Run tests
	$(UV) run python -m pytest tests/ -v

test-integration:  ## Run integration tests (downloads INSEE data)
	$(UV) run python -m pytest tests/ -v --run-integration -m integration

lint:  ## Run linter
	$(UV) run ruff check .

format:  ## Format code
	$(UV) run ruff format .

run:  ## Run full pipeline (ages 15-24, 2019-2027)
	$(UV) run insee-population population --min-age 15 --max-age 24 -o data/output

info:  ## Show available years and schema
	$(UV) run insee-population info

clean:  ## Clean cache and temp files
	rm -rf data/cache/*.parquet
	rm -rf data/output/*.parquet
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
