# India air quality — final delivery

Samuel Flores · Santiago Becerra · ETL G51

This extends the [second delivery](README.md). It preserves the three sources (Kaggle air-quality measurements, Wikipedia city coordinates, Open-Meteo ERA5 weather), Airflow, Great Expectations and the PostgreSQL star schema. It adds a next-day AQI model, real Kafka producer/consumer, a Python static report and a Streamlit prediction dashboard.

The Kafka stream is a **historical replay**, not live sensor data. Model accuracy must be assessed separately from the correctness of the data architecture.

## Read and demonstrate

- [Technical report and presentation guide](docs/entrega_final.md)
- [Architecture diagram](docs/arquitectura_final.mmd)
- [Relationship to classroom materials](docs/material_clase.md)
- [Measured results and verification evidence](docs/resultados_final.md)
- [ML notebook](notebooks/03_modelo_predictivo.ipynb)
- [Streaming demonstration notebook](notebooks/04_demo_streaming.ipynb)
- Generated static report: `reports/final/report.html`
- Model, metadata and holdout predictions: `artifacts/`
- Previous EDA notebooks remain in `notebooks/`.

## Reproduce locally

Requirements: Docker Desktop with Linux containers; allocate adequate memory for Kafka and Airflow. Run from this repository directory. The development passwords below are for isolated local demo services only. Ports are bound to localhost. Existing services are not reused.

```sh
docker compose up -d postgres kafka
docker compose build tools
docker compose run --rm tools python src/final_setup.py
```

### Full extraction and Airflow path

This fetches the three sources and runs quality checks before loading. Wikipedia/Open-Meteo must be reachable. The final DAG includes training, the static report and a finite 100-event Kafka replay.

```sh
docker compose run --rm tools airflow db migrate
docker compose run --rm tools airflow dags test etl_india_final 2026-11-22
```

`dags test` is a real single-DAG execution, not evidence of unattended scheduling. To use the scheduler/webserver:

```sh
docker compose --profile airflow up -d airflow
```

Open http://localhost:8088. `airflow standalone` generates a local admin password in `runtime/airflow/standalone_admin_password.txt`. Keep that runtime file private. Unpause only `etl_india_final` if scheduled runs are desired; the preserved second-delivery DAG is paused by default. The final DAG is daily, with `catchup=False`, one active run, and no automatic retries for quality-contract failures.

### Offline snapshot alternative

If a source is unavailable during a demonstration, the included PostgreSQL dump preserves the earlier integration of all three sources. Restore **once into a new, empty project database**, then run the current GX checks before training. A restored snapshot is explicitly labelled; it is not presented as a fresh API extraction.

```sh
docker compose exec -T postgres psql -U india -d india -v ON_ERROR_STOP=1 -f /opt/project/reports/warehouse.sql
docker compose run --rm tools python src/final_snapshot.py --run-id snapshot_revalidation
docker compose run --rm tools python src/final_ml.py
docker compose run --rm tools python src/final_report.py
docker compose run --rm tools python src/final_stream.py create-topic
```

Do not restore the old dump on top of a populated schema. Use the full DAG or simply reuse the validated existing warehouse instead.

The exact final-run snapshot is also included as `reports/final/warehouse_final.sql`. To reproduce the measured final results offline, restore that file instead of the older dump, into a new empty database. It already includes the final run's saved validation metadata. The older `reports/warehouse.sql` remains preserved from the second delivery.

### Live inference demonstration

After either path has produced `artifacts/model.joblib` and `artifacts/metadata.json`:

```sh
docker compose --profile live up -d consumer dashboard
docker compose run --rm tools python src/final_stream.py produce --limit 100 --delay 0.5 --replay-id classroom-demo-1
```

Open http://localhost:8502. Keep it open while publishing: the counter, chart and rows change as the consumer processes messages. To run another replay, choose a new replay ID. Reusing the same replay ID and source run makes event IDs deterministic, so the consumer stores no duplicate prediction. After retraining, restart the consumer to load the new model once:

```sh
docker compose restart consumer
```

The consumer does not query the warehouse and never retrains. It validates schema/calendar/features, predicts once per unique event and durably writes SQLite before committing its Kafka offset. Malformed events are recorded in `rejected_events`. Unexpected inference/storage failures stop the consumer without acknowledging the message. Streamlit reads only the prediction store; it has no analytical database query.

### Verification and shutdown

```sh
docker compose run --rm tools python -m pytest tests/test_final_ml.py tests/test_final_stream.py tests/test_final_report.py -q
docker compose logs consumer
docker compose --profile live --profile airflow down
```

The shutdown preserves the named PostgreSQL volume, project artifacts and prediction store. The runtime directory and credentials are excluded from Git. Do not run `down -v` unless you intentionally want to discard the project's database.

## Host Python / Jupyter option

The final notebooks run in a **local Python 3.12 Jupyter kernel**, with the infrastructure from Compose. They do not claim that Docker is available inside Google Colab. The original second-delivery Colab notebook is preserved unchanged.

```sh
python -m venv .venv
# Activate .venv in your operating system, then:
python -m pip install -r requirements-final.txt
```

Set `INDIA_DATABASE_URL=postgresql://india:india_demo_local@localhost:55432/india`, `KAFKA_BOOTSTRAP_SERVERS=localhost:19092` and `PYTHONPATH` to the absolute `src` directory. CLI entry points are `src/final_ml.py`, `src/final_report.py`, `src/final_stream.py`, and `streamlit run app/dashboard.py`.

## Prediction contract

Target: next local day's numeric AQI per city. Inputs: city, target month and weekday, origin-day AQI/PM2.5/PM10/NO2/weather, and trailing seven-day mean AQI. `aqi_lag1` is the **previous day relative to the target**. The next-day observed AQI and AQI category never enter events or input features.

All cities share date cutoffs: train 2015–2021, validation 2022, test 2023–2024. Ridge and Random Forest are selected using validation MAE, then the chosen pipeline is refitted through 2022 and evaluated on 2023–2024. A mean baseline and previous-day persistence baseline are reported with MAE, RMSE and R². The mean is fitted on training for validation, then on training plus validation for the final test. Missing/non-finite source values are rejected; incomplete seven-day histories are excluded. The model is saved with preprocessing, its SHA-256 and library versions.

Topic `india.aqi.features.v1` has one partition for the demonstration. Events include schema version, deterministic UUID, simulation flag, replay ID, publication timestamp, historical origin/target dates, city/measurement/run provenance and the documented `features` object. See the technical report for the field definitions.

## Value and limits

The static report supports retrospective comparisons across cities/time and assesses model error. The operational path demonstrates per-message scoring and auditable prediction delivery. Neither successful streaming nor a fitted model proves useful early warning ability. Kaggle provenance/units, India local-day assumptions, ERA5 reanalysis availability and weak predictive relationships remain material limitations. There is no causal interpretation and no claim of real-world deployment.
