# Data Engineering Pipeline

A Dockerized data pipeline project that:

- extracts data from a Postgres database and CSV files,
- stores extracted data by execution date,
- loads data into a final Postgres database,
- and materializes a query result for analysis.

## Tech Stack

- Apache Airflow (DAG orchestration)
- PostgreSQL
- Docker / Docker Compose
- Python (Pandas, SQLAlchemy, Psycopg2)

## Pipeline Flow

1. `data_extraction_and_local_storage`
2. `data_loading_to_final_database`

Both DAGs are date-parameterized and designed to be rerunnable.

## Repository Layout

- `dags/data_engineering_pipeline.py`: DAG definitions and ETL logic
- `data/`: source SQL/CSV and generated output folders
- `docker-compose.yml`: local platform (Airflow + Postgres + Redis)
- `main.ipynb`: local notebook execution and inspection
- `utils/health_check.py`: database container health check

## Prerequisites

- Docker Engine + Docker Compose plugin
- GNU Make

## Quickstart

1. Clone and enter the repository.
2. Create local environment files:

```bash
cp .env.example .env
cp dags/.env.example dags/.env
```

3. Start everything:

```bash
make start
```

4. Open Airflow: `http://localhost:8080`

- User: `airflow`
- Password: `airflow`

5. Trigger DAGs in this order:

- `data_extraction_and_local_storage`
- `data_loading_to_final_database`

Stop services with:

```bash
make stop
```

## Running Locally Without Airflow

Use `main.ipynb` to execute the pipeline logic directly and inspect generated query results.

Install local Python dependencies when running outside Docker:

```bash
pip install -r requirements-local.txt
```

## Troubleshooting

- Airflow startup can take 1-2 minutes on first boot.
- If tasks fail, inspect task logs in Airflow UI.
- Ensure `dags/.env` exists and points to reachable database values.

## Data Quality Gates

The pipeline now includes explicit quality gates:

- `validate_extracted_files` (DAG 1): verifies extracted CSV files exist, are non-empty, and are parseable.
- `run_data_quality_checks` (DAG 2): verifies expected tables were loaded and checks that `order_details` is not empty.

`data_loading_to_final_database` waits for the extraction quality gate before loading.

## Notes on Environment Files

- `.env.example` and `dags/.env.example` are committed as templates.
- Real `.env` files are intentionally ignored by git.
