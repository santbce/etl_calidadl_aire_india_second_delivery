"""Temporal leakage and feature-history checks, independent of external services."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from final_ml import FEATURE_COLUMNS, build_features, temporal_split, training_labels


def source_frame(days=25, start="2021-12-15"):
    frame = pd.DataFrame({"date": pd.date_range(start, periods=days)})
    frame["measurement_id"] = np.arange(days) + 1
    frame["city_id"] = 1
    frame["city_name"] = "Delhi"
    frame["run_id"] = "unit_test"
    frame["aqi"] = np.arange(days, dtype=float) + 100
    for name in ["pm25", "pm10", "no2", "temperature_2m_mean", "precipitation_sum", "wind_speed_10m_max"]:
        frame[name] = np.arange(days, dtype=float) + 1
    return frame


class FeatureHistoryTests(unittest.TestCase):
    def test_future_values_do_not_change_earlier_features(self):
        frame = source_frame()
        earlier = build_features(frame)
        changed = frame.copy()
        changed.loc[changed.date.gt("2021-12-28"), ["aqi", "pm25", "temperature_2m_mean"]] = 9999
        after = build_features(changed)
        columns = ["origin_date"] + FEATURE_COLUMNS
        pd.testing.assert_frame_equal(
            earlier.loc[earlier.origin_date.le("2021-12-28"), columns],
            after.loc[after.origin_date.le("2021-12-28"), columns],
        )

    def test_next_day_label_is_not_an_event_feature(self):
        frame = source_frame()
        events = build_features(frame)
        labeled = training_labels(frame)
        row = labeled.iloc[0]
        self.assertNotIn("target_aqi", events.columns)
        self.assertNotIn("aqi_bucket", events.columns)
        self.assertEqual(row.aqi_lag1, 106)
        self.assertEqual(row.aqi_mean7, 103)
        self.assertEqual(row.target_aqi, 107)
        self.assertEqual(row.target_date, "2021-12-22")
        self.assertEqual(len(labeled), len(events) - 1)

    def test_gap_excludes_unsafe_seven_day_windows(self):
        frame = source_frame(days=30, start="2022-01-01")
        frame = frame.loc[frame.date.ne("2022-01-10")]
        features = build_features(frame)
        origins = set(features.origin_date)
        self.assertIn("2022-01-09", origins)
        for day in pd.date_range("2022-01-11", "2022-01-16"):
            self.assertNotIn(day.strftime("%Y-%m-%d"), origins)
        self.assertIn("2022-01-17", origins)
        labeled = training_labels(frame)
        self.assertNotIn("2022-01-10", set(labeled.target_date))

    def test_city_windows_cannot_mix_measurements(self):
        first = source_frame(days=10)
        second = first.copy()
        second["city_id"] = 2
        second["city_name"] = "Mumbai"
        second["measurement_id"] += 1000
        second["aqi"] += 500
        features = build_features(pd.concat([second, first]).sample(frac=1, random_state=1))
        self.assertEqual(features.loc[features.city_id.eq(1)].iloc[0].aqi_mean7, 103)
        self.assertEqual(features.loc[features.city_id.eq(2)].iloc[0].aqi_mean7, 603)

    def test_missing_and_duplicate_sources_are_rejected(self):
        frame = source_frame()
        bad = frame.copy()
        bad.loc[0, "aqi"] = np.nan
        with self.assertRaisesRegex(ValueError, "Missing"):
            build_features(bad)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            build_features(pd.concat([frame, frame.iloc[[0]]]))

    def test_split_groups_all_cities_by_target_date(self):
        dates = ["2021-12-31", "2022-01-01", "2022-12-31", "2023-01-01", "2024-12-31"]
        frame = pd.DataFrame({"target_date": dates * 2, "city_id": [1] * 5 + [2] * 5})
        train, validation, test = temporal_split(frame)
        self.assertEqual(set(train.target_date), {"2021-12-31"})
        self.assertEqual(set(validation.target_date), {"2022-01-01", "2022-12-31"})
        self.assertEqual(set(test.target_date), {"2023-01-01", "2024-12-31"})
        self.assertLess(train.target_date.max(), validation.target_date.min())
        self.assertLess(validation.target_date.max(), test.target_date.min())


if __name__ == "__main__":
    unittest.main()
