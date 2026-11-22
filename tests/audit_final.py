"""Audit an executed final DAG without retraining or requesting external sources.

Run after the successful DAG inside the tools image:
    python tests/audit_final.py

This records actual Airflow metadata, warehouse integrity, the trained artifact,
the original negative-quality test and a target-free example of a Kafka event.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys

ROOT = Path(os.environ.get("INDIA_PROJECT_ROOT", Path(__file__).resolve().parents[1])).resolve()
os.environ.setdefault("INDIA_PROJECT_ROOT", str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import psycopg
from final_ml import FEATURE_COLUMNS, build_features
from final_stream import TOPIC, event_from_row, read_warehouse, validate_event
import pipeline_india


def now():
    return datetime.now(timezone.utc).isoformat()


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def warehouse_fingerprint(database_url):
    """Hash every persisted fact field, not only the row count or aggregate AQI."""
    digest = hashlib.sha256()
    count = 0
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT row_to_json(f)::text FROM india_dw.fact_air_quality f ORDER BY measurement_id")
            for row in cursor:
                digest.update(row[0].encode("utf-8"))
                digest.update(b"\n")
                count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def audit(dag_id="etl_india_final"):
    output = ROOT / "evidencias" / "final"
    output.mkdir(parents=True, exist_ok=True)
    report = {"started_at_utc": now(), "success": False, "dag_id": dag_id, "checks": {}}

    def require(name, condition, reason):
        report["checks"][name] = bool(condition)
        if not condition:
            raise RuntimeError(reason)

    try:
        database_url = os.environ["INDIA_DATABASE_URL"]
        airflow_home = Path(os.environ.get("AIRFLOW_HOME", ROOT / "runtime" / "airflow"))
        metadata_path = airflow_home / "airflow.db"
        require("airflow_metadata_exists", metadata_path.is_file(), "Airflow metadata is missing")
        # URI read-only mode prevents this audit from changing scheduling state.
        with sqlite3.connect(metadata_path.resolve().as_uri() + "?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            last_run = connection.execute(
                "SELECT id, dag_id, run_id, state, execution_date, start_date, end_date "
                "FROM dag_run WHERE dag_id=? ORDER BY id DESC LIMIT 1", (dag_id,)
            ).fetchone()
            require("final_dag_has_run", last_run is not None, "No final DAG run exists")
            last_run = dict(last_run)
            report["airflow_run"] = last_run
            run_id = last_run["run_id"]
            tasks = [dict(row) for row in connection.execute(
                "SELECT dag_id, run_id, task_id, state, start_date, end_date, duration, try_number "
                "FROM task_instance WHERE dag_id=? AND run_id=? ORDER BY task_id", (dag_id, run_id)
            ).fetchall()]
        with (output / "airflow_task_states.csv").open("w", encoding="utf-8", newline="") as stream:
            fields = ["dag_id", "run_id", "task_id", "state", "start_date", "end_date", "duration", "try_number"]
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(tasks)
        report["airflow_tasks"] = {"count": len(tasks), "states": {row["task_id"]: row["state"] for row in tasks}}
        require("dag_success", last_run["state"] == "success", "The latest final DAG did not succeed")
        require("twelve_tasks_succeeded", len(tasks) == 12 and len({r["task_id"] for r in tasks}) == 12
                and all(r["state"] == "success" for r in tasks), "Expected twelve distinct successful tasks")

        frame = read_warehouse(database_url)
        require("warehouse_rows_18265", len(frame) == 18265, "Warehouse row count differs")
        require("warehouse_city_day_unique", not frame.duplicated(["city_id", "date"]).any(), "Duplicate city/day")
        require("warehouse_measurement_unique", frame.measurement_id.is_unique, "Duplicate measurement identifier")
        require("warehouse_five_cities", frame.city_name.nunique() == 5, "Expected five cities")
        source_run_ids = sorted(frame.run_id.astype(str).unique().tolist())
        require("warehouse_matches_latest_run", source_run_ids == [run_id], "Warehouse belongs to another run")
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT rows, quality_result FROM india_dw.etl_runs WHERE run_id=%s", (run_id,))
                etl_run = cursor.fetchone()
        require("etl_run_validation_saved", etl_run is not None and etl_run[0] == 18265
                and etl_run[1].get("success") is True, "ETL run has no successful recorded validation")
        report["warehouse"] = {"rows": len(frame), "city_count": int(frame.city_name.nunique()),
                               "source_run_ids": source_run_ids, "etl_rows_recorded": etl_run[0]}

        workspace = pipeline_india.workspace(run_id)
        approval = json.loads((workspace / "approval.json").read_text(encoding="utf-8"))
        validation = json.loads((workspace / "validation.json").read_text(encoding="utf-8"))
        csv_sha = hashlib.sha256((workspace / "enriched.csv").read_bytes()).hexdigest()
        require("validated_csv_approval_matches", approval.get("success") is True
                and approval.get("run_id") == run_id and approval.get("sha256") == csv_sha,
                "The local CSV has no matching approval")
        require("local_quality_passed", validation.get("success") is True
                and validation.get("coverage_exact") is True, "Local quality result is unsuccessful")
        # Exercise the actual guard, so this check follows the same code as load.
        require("load_guard_accepts_approved_batch", len(pipeline_india.approved_dataframe(run_id)) == 18265,
                "Approved batch cannot pass the load guard")
        for name in ["approval.json", "validation.json", "expectation_suite.json"]:
            if (workspace / name).is_file():
                shutil.copy2(workspace / name, output / name)
        report["approved_batch"] = {"run_id": run_id, "csv_sha256": csv_sha,
                                    "coverage_exact": validation["coverage_exact"]}

        provenance = json.loads((workspace / "weather_provenance.json").read_text(encoding="utf-8"))
        modes = {mode: sum(record.get("mode") == mode for record in provenance) for mode in ["http", "cache"]}
        require("weather_fifty_documented_requests", len(provenance) == 50 and sum(modes.values()) == 50,
                "Expected fifty weather provenance entries (HTTP or cache)")
        report["weather_provenance"] = {"entries": len(provenance), "modes": modes,
            "note": "Read the stored extraction provenance; audit makes no HTTP requests."}

        artifacts = ROOT / "artifacts"
        model_metadata = json.loads((artifacts / "metadata.json").read_text(encoding="utf-8"))
        model_sha = hashlib.sha256((artifacts / "model.joblib").read_bytes()).hexdigest()
        require("model_matches_latest_run", model_metadata.get("source_run_ids") == [run_id],
                "Model was not trained from the latest ETL batch")
        require("model_hash_matches_metadata", model_metadata.get("model_sha256") == model_sha,
                "Serialized model hash differs from metadata")
        require("model_source_rows_match", model_metadata.get("source_rows") == len(frame),
                "Model metadata source count differs")
        require("model_feature_contract_matches", model_metadata.get("feature_columns") == FEATURE_COLUMNS,
                "Training and streaming feature contracts differ")
        report["model"] = {"name": model_metadata["model_name"], "version": model_metadata["model_version"],
                           "sha256": model_sha, "source_rows": model_metadata["source_rows"],
                           "split": model_metadata["split"], "metrics": model_metadata["metrics"]}

        before = warehouse_fingerprint(database_url)
        environment = os.environ.copy()
        environment["INDIA_TEST_RUN_ID"] = run_id
        # Exactly the original quality gate test: no invented assertions or skips.
        completed = subprocess.run(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_quality_gate.py", "-v"],
            cwd=ROOT, env=environment, capture_output=True, text=True, timeout=240,
        )
        log = completed.stdout + "\n" + completed.stderr
        # Keep verbose GX output private in runtime; summarize factual checks below.
        log_path = ROOT / "runtime" / "quality_gate_audit.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(log, encoding="utf-8")
        after = warehouse_fingerprint(database_url)
        report["quality_failure_test"] = {
            "original_test": "test_quality_gate.QualityGate.test_negative_precipitation_and_changed_file_are_blocked",
            "return_code": completed.returncode,
            "ran_one_test": bool(re.search(r"Ran 1 test\b", log)),
            "skipped": bool(re.search(r"\bskipped\b|\bSkipTest\b", log, re.IGNORECASE)),
            "warehouse_before": before, "warehouse_after": after,
            "scope": "Negative precipitation is rejected by GX; missing approval and changed CSV are blocked before loading.",
        }
        require("original_quality_gate_test_passed", completed.returncode == 0
                and report["quality_failure_test"]["ran_one_test"]
                and not report["quality_failure_test"]["skipped"], "Original quality gate test failed or skipped")
        require("warehouse_unchanged_after_rejections", before == after, "Warehouse changed during quality failure checks")
        report["quality_failure_test"]["load_guard_blocked_invalid_or_changed_batch"] = True
        report["quality_failure_test"]["warehouse_unchanged"] = True

        features = build_features(frame)
        demonstration = features.loc[features.target_date.ge("2023-01-01")].iloc[0].to_dict()
        event = validate_event(event_from_row(demonstration, "documented-example"))
        require("example_event_has_no_target", "target_aqi" not in event
                and "target_aqi" not in event["features"] and "aqi_bucket" not in event["features"],
                "Target leakage in documented event")
        save_json(ROOT / "docs" / "event_example.json", event)
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
        # Broker host:port is useful configuration evidence; credentials never belong here.
        safe_bootstrap = bootstrap if re.fullmatch(r"[A-Za-z0-9_.:\[\],-]+", bootstrap) else "configured-via-environment"
        report["stream_contract"] = {"topic": os.environ.get("KAFKA_TOPIC", TOPIC),
            "bootstrap_servers": safe_bootstrap, "event_schema": event["schema_version"],
            "example": "docs/event_example.json", "feature_count": len(event["features"]),
            "note": "Contract example only; actual Kafka delivery is audited separately."}
        report["success"] = True
    except Exception as exc:
        # Never serialize environment values or connection strings in portfolio evidence.
        report["error"] = f"{type(exc).__name__}: audit did not complete; inspect the failing check or local runtime log"
        raise
    finally:
        report["finished_at_utc"] = now()
        save_json(output / "audit.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag-id", default="etl_india_final")
    args = parser.parse_args()
    report = audit(args.dag_id)
    print(json.dumps({"success": report["success"], "checks": len(report["checks"]),
                      "run_id": report["airflow_run"]["run_id"]}, indent=2))


if __name__ == "__main__":
    main()
