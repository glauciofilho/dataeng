.PHONY: up down restart logs ps build clean

## Start all services
up:
	docker compose up -d --build

## Stop all services
down:
	docker compose down

## Stop and remove volumes (full reset)
clean:
	docker compose down -v --remove-orphans

## Rebuild images without cache
build:
	docker compose build --no-cache

## Restart a specific service: make restart svc=trino
restart:
	docker compose restart $(svc)

## Follow logs for all services
logs:
	docker compose logs -f

## Follow logs for a specific service: make log svc=airflow-webserver
log:
	docker compose logs -f $(svc)

## Show running containers
ps:
	docker compose ps

## Open Trino CLI
trino-cli:
	docker exec -it trino trino --catalog iceberg --schema gold

## Run dbt inside Airflow container
dbt-run:
	docker exec -it airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run"

dbt-test:
	docker exec -it airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test"

dbt-docs:
	docker exec -it airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate && dbt docs serve --port 8088"

## Trigger full pipeline DAG via Airflow CLI
trigger-pipeline:
	docker exec -it airflow-scheduler airflow dags trigger 04_full_pipeline
