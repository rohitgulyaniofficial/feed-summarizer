.PHONY: all pipeline pipeline-local fetcher summarizer publish publish-html publish-rss publish-all upload upload-force clean deploy-production deploy logs deploy-kata-secrets help encrypt-secrets decrypt-secrets test lint
export APP_NAME=summarizer
export PRODUCTION_SERVER=paas
export PYTHONUNBUFFERED=1

PYTHON=$(shell which python)

.DEFAULT_GOAL := help

all: pipeline ## Run complete pipeline with upload

# Complete pipeline (recommended for production)
pipeline: ## Run end-to-end pipeline (fetch + summarize + publish + upload)
	@echo "🚀 Running complete Feed Summarizer pipeline..."
	$(PYTHON) -u main.py run

pipeline-local: ## Run complete pipeline locally (fetch + summarize + publish, no Azure upload)
	@echo "🏠 Running Feed Summarizer pipeline locally (no Azure upload)..."
	$(PYTHON) -u main.py run --no-azure

fetcher: ## Run only the feed fetcher (no summarization/publish)
	@echo "📡 Running FeedFetcher..."
	$(PYTHON) -u main.py fetcher

summarizer: ## Run only the summarizer (no publish)
	@echo "🤖 Running Summarizer..."
	$(PYTHON) -u main.py summarizer

publish: publish-all ## Alias for publish-all

publish-html: ## Publish HTML bulletins only (no RSS) - not supported, runs full publisher
	@echo "📄 Publishing HTML bulletins only (note: current implementation publishes both HTML and RSS)..."
	$(PYTHON) -u main.py publish --no-azure

publish-rss: ## Publish RSS feeds only (no HTML) - not supported, runs full publisher
	@echo "📡 Publishing RSS feeds only (note: current implementation publishes both HTML and RSS)..."
	$(PYTHON) -u main.py publish --no-azure

publish-all: ## Publish all content (HTML + RSS) for existing DB
	@echo "📰 Publishing all content (HTML bulletins and RSS feeds)..."
	$(PYTHON) -u main.py publish --no-azure

test: ## Run pytest-based test suite
	@echo "🧪 Running pytest test suite..."
	$(PYTHON) -m pytest

lint: ## Run Ruff linter
	@echo "🧹 Running Ruff linter..."
	$(PYTHON) -m ruff check .

upload: ## Upload changed files to Azure Storage
	@echo "☁️  Uploading to Azure Storage..."
	$(PYTHON) -u main.py upload

upload-force: ## Force upload all files to Azure Storage
	@echo "☁️  Force uploading all files to Azure Storage..."
	$(PYTHON) -u main.py upload --force-upload

clean: ## Remove local database and generated public content
	@echo "🧹 Cleaning Database..."
	rm -f feeds.db*
	@echo "🧹 Cleaning published content..."
	rm -rf public/
	@echo "🧹 Cleaning __pycache__..."
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +

deploy-production: ## Push master to production remote
	git push production main

deploy: deploy-production ## Redeploy service on production host
	ssh -t kata@$(PRODUCTION_SERVER) docker service update --force $(APP_NAME)_worker
	ssh -t kata@$(PRODUCTION_SERVER) docker service logs --tail 20 -f $(APP_NAME)_worker

logs: ## Tail production logs
	ssh -t kata@$(PRODUCTION_SERVER) docker service logs --tail 20 -f $(APP_NAME)_worker
	git gc --aggressive --prune=now

deploy-kata-secrets: ## Deploy secrets.yaml to kata@paas for feed_summarizer
	@cat secrets.yaml | ssh kata@paas secrets:set feed_summarizer

encrypt-secrets: ## Encrypt secrets.yaml to secrets.yaml.gpg (symmetric password; you will be prompted)
	@if [ ! -f secrets.yaml ]; then echo "❌ secrets.yaml not found"; exit 1; fi
	@echo "🔐 Encrypting secrets.yaml with symmetric AES256 ..."
	gpg --yes --symmetric --cipher-algo AES256 --output secrets.yaml.gpg secrets.yaml

decrypt-secrets: ## Decrypt secrets.yaml.gpg to secrets.yaml (you will be prompted for the password)
	@if [ ! -f secrets.yaml.gpg ]; then echo "❌ secrets.yaml.gpg not found"; exit 1; fi
	@echo "🔓 Decrypting secrets.yaml.gpg to secrets.yaml ..."
	gpg --yes --output secrets.yaml --decrypt secrets.yaml.gpg

fetch-database: ## Fetch latest feeds.db from server
	@echo "📥 Fetching latest feeds.db from Azure..."
	ssh paas 'sudo cp -r /home/kata/data/summarizer/ .'
	scp 'paas:summarizer/feeds.db*' .

sync-github: ## Rsync non-confidential files to GitHub mirror
	@echo "📤 Syncing to GitHub mirror..."
	rsync -av --delete \
		--exclude='.env' \
		--exclude='feeds.yaml' \
		--exclude='kata-compose.yaml' \
		--exclude='secrets.yaml' \
		--exclude='secrets.yaml.gpg' \
		--exclude='feeds.db*' \
		--exclude='*.db' \
		--exclude='*.db-*' \
		--exclude='__pycache__/' \
		--exclude='.git/' \
		--exclude='public/' \
		--exclude='.venv/' \
		--exclude='*.pyc' \
		./ ~/Sync/Development/GitHub/feed-summarizer/

help: ## Show this help
	@grep -hE '^[A-Za-z0-9_ \-]*?:.*##.*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
