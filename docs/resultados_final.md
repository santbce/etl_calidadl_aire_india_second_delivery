# Final-delivery execution results

Executed on 22 November 2026. These results describe the tested local Docker stack, not an unattended production deployment.

## ETL and quality

The real Airflow test run `manual__2026-11-22T00:00:00+00:00` of `etl_india_final` finished successfully with all twelve tasks successful. It extracted the preserved air-quality database, refreshed Wikipedia coordinates and fetched all fifty city/year Open-Meteo responses over HTTP. The merge retained 18,265 city/day observations across five cities, and Great Expectations approved the integrated batch before PostgreSQL loading.

The audit passed 21 checks, including successful DAG/task states, row counts and unique city/day keys, approval fingerprints, source provenance and model provenance. The preserved quality-gate test rejected negative precipitation and a file modified after approval. The analytical fact-table fingerprint was unchanged after the rejection tests.

Evidence: `evidencias/final/audit.json`, `evidencias/final/airflow_task_states.csv`, `evidencias/final/validation.json` and `evidencias/final/approval.json`. The run was executed with `airflow dags test`; it does not establish that the scheduler has run continuously.

## Model performance

The selection used 2015–2021 for training, 2022 for validation and 2023–2024 for held-out testing. Ridge had the best validation MAE among the trained candidates and was refitted on 2015–2022. Both model and preprocessing were serialized together.

| Test estimator | MAE (AQI points) | RMSE (AQI points) | R² |
|---|---:|---:|---:|
| Selected Ridge pipeline | 98.7733 | 114.1729 | -0.0015 |
| Constant development-mean baseline | 98.6803 | 114.0930 | -0.0001 |
| Previous-day persistence | 132.8741 | 162.5509 | -1.0301 |

The trained model improves on previous-day persistence but **does not improve on the constant-mean baseline**. Its R² is approximately zero. The system demonstrates reproducible model evaluation and per-message inference; this dataset and feature set do not establish useful forecasting skill. These results should be explained during the presentation, not hidden by selecting a different test period.

Evidence: `artifacts/metadata.json`, `artifacts/test_predictions.csv`, the executed ML notebook and the generated static report.

## Kafka and dashboard

The integration test used an isolated real Kafka topic and 43 messages: 20 valid records, the same 20 records repeated, and three invalid records (malformed JSON, invalid schema and a tombstone). The consumer stored exactly 20 predictions, ignored 20 duplicates, quarantined all three invalid messages and committed offset 43 only after durable processing. Its predictions matched the serialized model, it had no warehouse connection credentials, and the PostgreSQL source fingerprint remained unchanged.

The main dashboard displayed the first 100 predictions published by Airflow. A further 50 records were published with a new replay ID. While the same page remained open, the counter advanced from 100 to 150 without reloading. Chart and processing timestamps updated from consumer output. This is a simulation with historical records, not current measurements.

Evidence: `evidencias/final/integration.json` and the executed streaming notebook. The notebook separately demonstrates 50 new predictions and 50 redeliveries without extra rows.

## Verification and reproduction

All 25 new unit tests passed in the Linux application image. Both new notebooks executed successfully using that same image and the real services. The original EDA notebooks remain preserved.

Start with `README_FINAL.md`. The static report is `reports/final/report.html`; the separate Streamlit dashboard runs at `http://localhost:8502` while the project services are active. The model version and environment versions are recorded in its metadata.
