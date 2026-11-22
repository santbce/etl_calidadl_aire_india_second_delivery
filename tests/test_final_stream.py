"""Event contracts and durable Kafka inference, without a live broker."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from final_ml import FEATURE_COLUMNS
from final_stream import SCHEMA, TOPIC, event_from_row, open_store, process_message, validate_event


def feature_row():
    # 2023-01-02 is Monday; every measurement belongs to 2023-01-01 or earlier.
    return {"origin_date": "2023-01-01", "target_date": "2023-01-02",
            "run_id": "successful-etl-run", "city_id": 1, "measurement_id": 1001,
            "city_name": "Delhi", "month": 1, "day_of_week": 0,
            "aqi_lag1": 250., "aqi_mean7": 230., "pm25_lag1": 40.,
            "pm10_lag1": 60., "no2_lag1": 20., "temperature_lag1": 18.,
            "precipitation_lag1": 0., "wind_lag1": 3.,
            "target_aqi": 999.}  # A producer must never copy this label.


class CountingModel:
    def __init__(self, result=240., error=None):
        self.calls = 0
        self.result = result
        self.error = error
        self.last_input = None

    def predict(self, frame):
        self.calls += 1
        self.last_input = frame.copy()
        if self.error:
            raise self.error
        return [self.result]


class EventContractTests(unittest.TestCase):
    def test_producer_excludes_target_and_preserves_provenance(self):
        event = event_from_row(feature_row(), "demo-one")
        self.assertEqual(event["schema_version"], SCHEMA)
        self.assertEqual(set(event["features"]), set(FEATURE_COLUMNS))
        self.assertNotIn("target_aqi", json.dumps(event))
        self.assertEqual(event["source_measurement_id"], 1001)
        self.assertEqual(event["source_run_id"], "successful-etl-run")
        self.assertTrue(event["simulation"])
        self.assertIsNotNone(datetime.fromisoformat(event["published_at"]).tzinfo)
        self.assertEqual(validate_event(event), event)

    def test_replay_event_identifier_is_stable_but_new_replay_is_distinct(self):
        first = event_from_row(feature_row(), "demo-one")
        repeated = event_from_row(feature_row(), "demo-one")
        other_replay = event_from_row(feature_row(), "demo-two")
        self.assertEqual(first["event_id"], repeated["event_id"])
        self.assertNotEqual(first["event_id"], other_replay["event_id"])

    def test_target_or_unknown_feature_is_rejected(self):
        for field in ["target_aqi", "aqi_bucket", "future_temperature"]:
            with self.subTest(field=field):
                event = event_from_row(feature_row(), "demo")
                event["features"][field] = 400.
                with self.assertRaises(ValueError):
                    validate_event(event)
        event = event_from_row(feature_row(), "demo")
        event["target_aqi"] = 400.
        with self.assertRaises(ValueError):
            validate_event(event)

    def test_nonfinite_negative_and_boolean_numeric_values_are_rejected(self):
        for value in [float("nan"), float("inf"), -1., True, "250"]:
            with self.subTest(value=repr(value)):
                event = event_from_row(feature_row(), "demo")
                event["features"]["aqi_lag1"] = value
                with self.assertRaises(ValueError):
                    validate_event(event)
        # Below-zero temperatures are meaningful, unlike negative AQI/rain/wind.
        event = event_from_row(feature_row(), "demo")
        event["features"]["temperature_lag1"] = -5.
        self.assertEqual(validate_event(event)["features"]["temperature_lag1"], -5.)

    def test_date_horizon_calendar_and_timezone_must_be_consistent(self):
        changes = [
            ("target_date", "2023-01-03"),
            ("published_at", "2026-11-22T10:00:00"),
            ("origin_date", "2014-12-31"),
        ]
        for key, value in changes:
            with self.subTest(key=key):
                event = event_from_row(feature_row(), "demo")
                event[key] = value
                with self.assertRaises(ValueError):
                    validate_event(event)
        for key, value in [("month", 2), ("day_of_week", 3)]:
            event = event_from_row(feature_row(), "demo")
            event["features"][key] = value
            with self.assertRaises(ValueError):
                validate_event(event)


class DurableInferenceTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.con = open_store(Path(self.folder.name) / "predictions.sqlite")
        self.model = CountingModel()
        self.metadata = {"model_version": "unit-test-model"}
        self.event = event_from_row(feature_row(), "unit-replay")

    def tearDown(self):
        self.con.close()
        self.folder.cleanup()

    def process(self, value, offset=0, model=None):
        raw = json.dumps(value).encode("utf-8") if isinstance(value, dict) else value
        return process_message(raw, model or self.model, self.metadata, self.con, TOPIC, 0, offset)

    def count(self, table):
        return self.con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    def test_valid_message_is_predicted_and_stored_before_return(self):
        self.assertEqual(self.process(self.event), "predicted")
        self.assertEqual(self.model.calls, 1)
        self.assertEqual(list(self.model.last_input.columns), FEATURE_COLUMNS)
        self.assertNotIn("target_aqi", self.model.last_input)
        stored = self.con.execute("SELECT event_id, predicted_aqi, model_version, topic, kafka_offset FROM predictions").fetchone()
        self.assertEqual(stored, (self.event["event_id"], 240., "unit-test-model", TOPIC, 0))
        self.assertEqual(self.count("rejected_events"), 0)

    def test_duplicate_event_does_not_predict_twice(self):
        self.assertEqual(self.process(self.event), "predicted")
        self.assertEqual(self.process(self.event, offset=1), "duplicate")
        self.assertEqual(self.model.calls, 1)
        self.assertEqual(self.count("predictions"), 1)

    def test_bad_json_and_injected_target_are_durably_rejected(self):
        self.assertEqual(self.process(b"{not valid JSON", offset=10), "rejected")
        leaked = deepcopy(self.event)
        leaked["features"]["target_aqi"] = 999.
        self.assertEqual(self.process(leaked, offset=11), "rejected")
        self.assertEqual(self.count("rejected_events"), 2)
        self.assertEqual(self.count("predictions"), 0)
        self.assertEqual(self.model.calls, 0)
        # Reprocessing the same invalid offset is idempotent too.
        self.assertEqual(self.process(b"{not valid JSON", offset=10), "rejected")
        self.assertEqual(self.count("rejected_events"), 2)

    def test_model_failure_propagates_without_storing_or_rejecting_event(self):
        model = CountingModel(error=RuntimeError("model unavailable"))
        with self.assertRaisesRegex(RuntimeError, "model unavailable"):
            self.process(self.event, model=model)
        self.assertEqual(self.count("predictions"), 0)
        self.assertEqual(self.count("rejected_events"), 0)
        self.assertEqual(self.process(self.event), "predicted")

    def test_nonfinite_prediction_does_not_advance_durable_state(self):
        with self.assertRaisesRegex(RuntimeError, "non-finite"):
            self.process(self.event, model=CountingModel(result=float("nan")))
        self.assertEqual(self.count("predictions"), 0)
        self.assertEqual(self.count("rejected_events"), 0)

    def test_non_string_event_id_is_rejected_instead_of_crashing_consumer(self):
        event = deepcopy(self.event)
        event["event_id"] = 123
        self.assertEqual(self.process(event, offset=12), "rejected")
        self.assertEqual(self.count("rejected_events"), 1)
        self.assertEqual(self.model.calls, 0)

    def test_kafka_tombstone_is_rejected_instead_of_crashing_consumer(self):
        self.assertEqual(self.process(None, offset=13), "rejected")
        self.assertEqual(self.count("rejected_events"), 1)
        self.assertEqual(self.model.calls, 0)

    def test_corrupt_utf8_is_quarantined_with_readable_replacement(self):
        self.assertEqual(self.process(b"\xff\xfe", offset=14), "rejected")
        payload = self.con.execute("SELECT payload FROM rejected_events").fetchone()[0]
        self.assertIn("\ufffd", payload)
        self.assertEqual(self.model.calls, 0)


if __name__ == "__main__":
    unittest.main()
