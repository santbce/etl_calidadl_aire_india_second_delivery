"""Analytical report rejects unapproved or incomplete database snapshots."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from final_report import _model_section, summarize, validate_snapshot


def fixture():
    rows = []
    for city_id, city in [(1, "Delhi"), (2, "Mumbai")]:
        for i, date in enumerate(pd.date_range("2024-01-01", periods=4)):
            rows.append({"measurement_id": city_id * 10 + i, "city_id": city_id, "city_name": city,
                         "date": date, "aqi": 200 + i * 10 + city_id, "aqi_bucket": "Poor",
                         "run_id": "approved", "temperature_2m_mean": 20 + i,
                         "precipitation_sum": i, "wind_speed_10m_max": 2 + i})
    return pd.DataFrame(rows), [{"run_id": "approved", "rows": 8, "quality_result": {"success": True}}]


class ReportQualityTests(unittest.TestCase):
    def test_no_approved_metadata_is_rejected(self):
        df, records = fixture()
        records[0]["quality_result"]["success"] = False
        with self.assertRaisesRegex(ValueError, "approval"):
            validate_snapshot(df, records)

    def test_lost_row_is_rejected_even_if_other_values_are_valid(self):
        df, records = fixture()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            validate_snapshot(df.iloc[:-1], records)

    def test_daily_gap_is_rejected_even_with_matching_metadata_count(self):
        df, records = fixture()
        df = df.drop(index=1)
        records[0]["rows"] = len(df)
        with self.assertRaisesRegex(ValueError, "daily city coverage"):
            validate_snapshot(df, records)

    def test_duplicate_key_is_rejected(self):
        df, records = fixture()
        df.loc[1, "date"] = df.loc[0, "date"]
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            validate_snapshot(df, records)

    def test_summary_preserves_every_measurement_and_category_count(self):
        df, records = fixture()
        summary, cities, monthly, correlations = summarize(validate_snapshot(df, records))
        self.assertEqual(summary["rows"], 8)
        self.assertEqual(summary["category_counts"]["Poor"], 8)
        self.assertEqual(summary["very_poor_or_severe_share"], 0)
        self.assertEqual(cities.observations.sum(), 8)
        self.assertEqual(len(monthly), 2)
        self.assertEqual(correlations.shape, (2, 3))

    def test_artifact_metadata_is_escaped_and_baselines_are_visible(self):
        with tempfile.TemporaryDirectory() as folder:
            Path(folder, "metadata.json").write_text(json.dumps({
                "selected_model": "<script>unsafe</script>", "model_version": "one",
                "test_metrics": {"model": {"mae": 10}, "mean_baseline": {"mae": 9}},
                "limitations": ["Baseline has lower error"]
            }), encoding="utf-8")
            html = _model_section(Path(folder))
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertIn("mean_baseline", html)
        self.assertIn("Baseline has lower error", html)


if __name__ == "__main__":
    unittest.main()
