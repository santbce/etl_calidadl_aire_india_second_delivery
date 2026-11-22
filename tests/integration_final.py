"""Run the final Kafka/ML integration check against real local services.

Start the isolated project PostgreSQL/Kafka services and train the model first.
This script creates and removes only a randomly named india.test.* Kafka topic.
It never writes to the analytical warehouse. Run directly, not through pytest.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid

import joblib
import numpy as np
import pandas as pd
import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from final_ml import FEATURE_COLUMNS, build_features
from final_stream import event_from_row, read_warehouse, validate_event


def fingerprint(database_url):
    """Hash all enriched fact rows, including dimensional values and run IDs."""
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute("""SELECT count(*), md5(string_agg(row_to_json(v)::text, E'\\n'
                           ORDER BY measurement_id))
                           FROM india_dw.air_quality_enriched v""")
            rows, digest = cur.fetchone()
    return {"rows": int(rows), "md5": digest}


def check(condition, explanation):
    if not condition:
        raise AssertionError(explanation)


def run_integration(database_url, bootstrap="localhost:19092", artifacts="artifacts",
                    output="evidencias/final/integration.json"):
    artifact_path = (ROOT / artifacts).resolve()
    metadata = json.loads((artifact_path / "metadata.json").read_text(encoding="utf-8"))
    model = joblib.load(artifact_path / "model.joblib")
    token = uuid.uuid4().hex
    topic, group = f"india.test.{token}", f"india-test-{token}"
    runtime = ROOT / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    store = runtime / f"integration_{token}.sqlite"
    log_path = runtime / f"integration_{token}.log"
    command = [sys.executable, "src/final_stream.py", "consume", "--bootstrap", bootstrap,
               "--topic", topic, "--group", group, "--artifacts", str(artifact_path),
               "--store", str(store), "--max-messages", "43", "--idle-timeout", "30"]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + environment.get("PYTHONPATH", "")
    environment["PYTHONUNBUFFERED"] = "1"
    # The consumer is deliberately not given the warehouse connection string.
    environment.pop("INDIA_DATABASE_URL", None)
    result = {"success": False, "status": False, "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "topic": topic, "group": group, "expected": {"predicted": 20, "duplicate": 20, "rejected": 3},
              "commands": ["python tests/integration_final.py", "python src/final_stream.py consume --topic <isolated-test-topic> --group <isolated-test-group> --max-messages 43 --idle-timeout 30"],
              "warehouse_write_operations": 0, "consumer_warehouse_credentials_present": False,
              "consumer_log": str(log_path.relative_to(ROOT)), "prediction_store": str(store.relative_to(ROOT))}
    admin = AdminClient({"bootstrap.servers": bootstrap})
    child = None
    topic_created = False
    failure = None
    try:
        before = fingerprint(database_url)
        result["warehouse_before"] = before
        check(before["rows"] == 18265, "The warehouse must contain the 18,265 validated source observations")
        features = build_features(read_warehouse(database_url))
        sample = features.loc[(features.target_date >= "2023-01-01") &
                              (features.target_date <= "2023-12-31")].head(20)
        check(len(sample) == 20, "Not enough out-of-sample rows in the 2023 replay interval")
        events = [validate_event(event_from_row(row, token)) for row in sample.to_dict("records")]
        for event in events:
            check(set(event["features"]) == set(FEATURE_COLUMNS), "Unexpected inference feature")
            check("target_aqi" not in event and "target_aqi" not in event["features"], "Target leakage in event")
            check(event["target_date"] > str(metadata["fit_through"])[:10], "Integration replay must be outside the fitted period")
        expected_predictions = model.predict(pd.DataFrame([e["features"] for e in events], columns=FEATURE_COLUMNS))
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(30)
        topic_created = True
        with log_path.open("w", encoding="utf-8") as logfile:
            child = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=logfile,
                                     stderr=subprocess.STDOUT, text=True)
            producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                 "acks": "all", "delivery.timeout.ms": 30000,
                                 "client.id": f"india-integration-{token}"})
            deliveries = []
            errors = []

            def delivered(error, message):
                if error:
                    errors.append(str(error))
                else:
                    deliveries.append({"partition": message.partition(), "offset": message.offset()})

            raw_events = [json.dumps(e, allow_nan=False).encode("utf-8") for e in events]
            # Same event IDs and bytes are resent: durable de-duplication must
            # avoid both a second prediction and a second result row.
            payloads = raw_events + raw_events + [b"{invalid-json", b'{"schema_version":"unsupported"}', None]
            for i, payload in enumerate(payloads):
                producer.produce(topic, partition=0, key=str(i % 5), value=payload, on_delivery=delivered)
                producer.poll(0)
            pending = producer.flush(35)
            check(pending == 0 and not errors, f"Kafka delivery failed: pending={pending}, errors={errors}")
            check(len(deliveries) == 43, "Not every test message was acknowledged by Kafka")
            check(sorted(d["offset"] for d in deliveries) == list(range(43)), "Unexpected offsets in isolated single-partition topic")
            try:
                return_code = child.wait(timeout=60)
            except subprocess.TimeoutExpired as exc:
                child.kill()
                child.wait(timeout=10)
                raise AssertionError("Consumer did not finish the 43-message integration run within 60 seconds") from exc
        check(return_code == 0, f"Consumer exited with code {return_code}; see {log_path.relative_to(ROOT)}")
        log_lines = log_path.read_text(encoding="utf-8").splitlines()
        summaries = []
        for line in log_lines:
            try:
                record = json.loads(line)
                if isinstance(record, dict) and all(k in record for k in ["predicted", "duplicate", "rejected"]):
                    summaries.append(record)
            except json.JSONDecodeError:
                pass
        check(bool(summaries), "Consumer did not emit its final processing summary")
        counts = {k: summaries[-1][k] for k in ["predicted", "duplicate", "rejected"]}
        check(counts == result["expected"], f"Unexpected consumer counts: {counts}")
        result["counts"] = counts
        with sqlite3.connect(store) as conn:
            predictions = conn.execute("SELECT event_id,predicted_aqi,model_version,topic,partition_id,kafka_offset,event_json FROM predictions ORDER BY kafka_offset").fetchall()
            rejected = conn.execute("SELECT topic,partition_id,kafka_offset,reason FROM rejected_events ORDER BY kafka_offset").fetchall()
        check(len(predictions) == 20 and len(rejected) == 3, "Durable result counts differ from processing summary")
        check(len({r[0] for r in predictions}) == 20, "Duplicate prediction IDs in store")
        check([r[0] for r in predictions] == [e["event_id"] for e in events], "Stored event order differs from producer order")
        check([r[5] for r in predictions] == list(range(20)), "Duplicate events overwrote original prediction offsets")
        check([r[2] for r in rejected] == [40, 41, 42], "Invalid messages were not quarantined at their Kafka offsets")
        check(all(r[2] == metadata["model_version"] and r[3] == topic and r[4] == 0 for r in predictions), "Prediction provenance mismatch")
        check(all(r[0] == topic and r[1] == 0 and r[3] for r in rejected), "Rejection provenance mismatch")
        for row, event in zip(predictions, events):
            check(json.loads(row[6]) == event, "Stored event does not match the produced event")
        np.testing.assert_allclose(np.array([r[1] for r in predictions]), np.array(expected_predictions), rtol=1e-12, atol=1e-12)
        probe = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
        try:
            committed = probe.committed([TopicPartition(topic, 0)], timeout=15)[0].offset
        finally:
            probe.close()
        check(committed == 43, f"Consumer offset must be committed after every durable outcome, got {committed}")
        result.update({"acknowledged_messages": 43, "committed_offset": committed,
                       "unique_predictions": len(predictions), "prediction_matches_serialized_model": True,
                       "target_excluded_from_features": True, "replay_is_out_of_sample": True,
                       "duplicate_did_not_overwrite_first_offset": True,
                       "invalid_offsets": [40, 41, 42], "model_version": metadata["model_version"]})
        result["warehouse_after"] = fingerprint(database_url)
        check(result["warehouse_after"] == result["warehouse_before"], "Streaming modified the analytical source dataset")
        result["warehouse_unchanged"] = True
        result["success"] = True
        result["status"] = True
    except Exception as exc:
        failure = exc
        # Do not serialize connection strings, environment or exception details
        # that may contain credentials. Detailed consumer output stays local.
        result["failure_type"] = type(exc).__name__
        result["failure"] = "Integration assertion or infrastructure operation failed; inspect the local consumer log and terminal output."
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait(timeout=10)
        if topic_created:
            try:
                # This name is generated by the script, never supplied by user
                # input, so cleanup cannot target the demonstration topic.
                check(topic == f"india.test.{token}", "Unsafe integration topic cleanup target")
                admin.delete_topics([topic], operation_timeout=15)[topic].result(30)
                result["test_topic_deleted"] = True
            except Exception:
                result["test_topic_deleted"] = False
        result["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        output_path = ROOT / output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    if failure is not None:
        raise failure
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("INDIA_DATABASE_URL"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--artifacts", default="artifacts")
    parser.add_argument("--output", default="evidencias/final/integration.json")
    args = parser.parse_args()
    if not args.database_url:
        parser.error("Set INDIA_DATABASE_URL or provide --database-url")
    result = run_integration(args.database_url, args.bootstrap, args.artifacts, args.output)
    print(json.dumps({"success": result["success"], "counts": result["counts"],
                      "warehouse_unchanged": result["warehouse_unchanged"],
                      "evidence": args.output}, indent=2))


if __name__ == "__main__":
    main()
