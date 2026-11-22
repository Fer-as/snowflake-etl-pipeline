# Snowflake ETL Pipeline - common commands.
# On Windows run these from Git Bash or WSL with GNU make installed,
# or copy the command after the target name into PowerShell.

COMPOSE = docker compose
# Runs a throwaway container from the project image with the repo mounted read-only.
AIRFLOW_RUN = docker run --rm --entrypoint bash -e PYTHONDONTWRITEBYTECODE=1 -v "$(CURDIR):/project:ro" -w /project retail-etl-airflow:3.3.0 -c

.PHONY: help up down restart ps logs test test-local test-dags tf-check \
        healthcheck alert-test dashboard dashboard-local

help:            ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-16s %s\n", $$1, $$2}'

up:              ## Build and start Airflow (UI on http://localhost:8080)
	$(COMPOSE) up -d --build

down:            ## Stop containers (keeps the metadata volume)
	$(COMPOSE) down

restart: down up ## Restart the stack

ps:              ## Show container status
	$(COMPOSE) ps

logs:            ## Follow scheduler, worker and DAG-processor logs
	$(COMPOSE) logs -f airflow-scheduler airflow-worker airflow-dag-processor

test: test-local test-dags ## Run every test

test-local:      ## Unit, lineage and dashboard tests on local Python
	python -m pytest

test-dags:       ## DAG integrity + unit tests inside the Airflow image
	$(AIRFLOW_RUN) "pip install -q pytest && python -m pytest -p no:cacheprovider tests/ -v"

tf-check:        ## Terraform format check and validation (no plan/apply)
	cd terraform && terraform fmt -check && terraform validate

healthcheck:     ## Trigger the read-only connection health-check DAG
	$(COMPOSE) exec airflow-api-server airflow dags unpause connection_healthcheck
	$(COMPOSE) exec airflow-api-server airflow dags trigger connection_healthcheck

alert-test:      ## Send one test SNS alert through the production callback
	$(COMPOSE) exec airflow-worker python /opt/airflow/python_scripts/send_test_sns_alert.py

dashboard:       ## Run the dashboard against Snowflake
	streamlit run dashboard/app.py

dashboard-local: ## Run the dashboard from the local CSVs (no Snowflake needed)
	DASHBOARD_DATA_SOURCE=local streamlit run dashboard/app.py
