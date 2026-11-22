"""Next-day AQI regression from the validated India dimensional warehouse.

Feature engineering is shared by offline training and the Kafka producer. Every
numeric observation in an event belongs to origin_date or an earlier date;
target_date is the following local calendar day in India.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

CITIES = ["Bangalore", "Chennai", "Delhi", "Kolkata", "Mumbai"]
NUMERIC_FEATURES = [
    "month", "day_of_week", "aqi_lag1", "aqi_mean7", "pm25_lag1",
    "pm10_lag1", "no2_lag1", "temperature_lag1", "precipitation_lag1", "wind_lag1",
]
FEATURE_COLUMNS = ["city_name"] + NUMERIC_FEATURES
SOURCE_MEASURES = [
    "aqi", "pm25", "pm10", "no2", "temperature_2m_mean",
    "precipitation_sum", "wind_speed_10m_max",
]
REQUIRED_COLUMNS = ["measurement_id", "city_id", "city_name", "date", "run_id"] + SOURCE_MEASURES
WAREHOUSE_SQL = """SELECT measurement_id, city_id, city_name, date, run_id,
    aqi, pm25, pm10, no2, temperature_2m_mean, precipitation_sum, wind_speed_10m_max
    FROM india_dw.air_quality_enriched v
    WHERE EXISTS (SELECT 1 FROM india_dw.etl_runs r WHERE r.run_id=v.run_id
                  AND (r.quality_result->>'success')::boolean IS TRUE)
    ORDER BY city_id, date"""


def _source_frame(df: pd.DataFrame) -> pd.DataFrame:
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing warehouse columns: {sorted(missing)}")
    if df.empty:
        raise ValueError("The validated warehouse is empty")
    source = df[REQUIRED_COLUMNS].copy()
    if source.isna().any().any():
        raise ValueError("Missing source values: repair the ETL before training or publishing")
    source["date"] = pd.to_datetime(source["date"], errors="raise")
    if source["date"].dt.tz is not None or not source["date"].eq(source["date"].dt.normalize()).all():
        raise ValueError("Source dates must be local calendar days without time or timezone")
    if source.duplicated(["city_id", "date"]).any() or source["measurement_id"].duplicated().any():
        raise ValueError("Duplicate source measurement or city/day")
    if not source["city_name"].isin(CITIES).all():
        raise ValueError("Unknown city; the model supports the five documented cities")
    if (source.groupby("city_id")["city_name"].nunique() != 1).any():
        raise ValueError("A city identifier maps to multiple names")
    if (source.groupby("city_name")["city_id"].nunique() != 1).any():
        raise ValueError("A city name maps to multiple identifiers")
    for name in SOURCE_MEASURES:
        source[name] = pd.to_numeric(source[name], errors="raise")
        if not np.isfinite(source[name].to_numpy(dtype=float)).all():
            raise ValueError(f"Non-finite source values in {name}")
    if (source[[c for c in SOURCE_MEASURES if c != "temperature_2m_mean"]] < 0).any().any():
        raise ValueError("Negative AQI, pollutant, rain or wind value")
    return source.sort_values(["city_id", "date"]).reset_index(drop=True)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build inference rows, excluding incomplete seven-day histories and gaps.

    The row at origin day t contains the AQI at t (aqi_lag1 relative to the
    forecast day t+1) and a trailing mean from t-6 through t. No shift toward a
    future observation, centered rolling window or target category is used.
    """
    source = _source_frame(df)
    pieces = []
    for _, city in source.groupby("city_id", sort=True):
        city = city.copy()
        seven_consecutive = city["date"].sub(city["date"].shift(6)).dt.days.eq(6)
        city["origin_date"] = city["date"].dt.strftime("%Y-%m-%d")
        tomorrow = city["date"] + pd.Timedelta(days=1)
        city["target_date"] = tomorrow.dt.strftime("%Y-%m-%d")
        city["month"] = tomorrow.dt.month.astype(int)
        city["day_of_week"] = tomorrow.dt.dayofweek.astype(int)
        city["aqi_mean7"] = city["aqi"].rolling(window=7, min_periods=7).mean()
        mapping = {
            "aqi": "aqi_lag1", "pm25": "pm25_lag1", "pm10": "pm10_lag1",
            "no2": "no2_lag1", "temperature_2m_mean": "temperature_lag1",
            "precipitation_sum": "precipitation_lag1", "wind_speed_10m_max": "wind_lag1",
        }
        for original, feature in mapping.items():
            city[feature] = city[original]
        pieces.append(city.loc[seven_consecutive, [
            "measurement_id", "city_id", "run_id", "origin_date", "target_date",
        ] + FEATURE_COLUMNS])
    return pd.concat(pieces, ignore_index=True).sort_values(
        ["target_date", "city_id"]
    ).reset_index(drop=True)


def training_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Attach observed next-day AQI for supervised learning, never for events."""
    source = _source_frame(df)
    labels = source[["city_id", "date", "aqi"]].copy()
    labels["target_date"] = labels["date"].dt.strftime("%Y-%m-%d")
    labels = labels.rename(columns={"aqi": "target_aqi"}).drop(columns="date")
    return build_features(source).merge(
        labels, on=["city_id", "target_date"], how="inner", validate="one_to_one"
    ).sort_values(["target_date", "city_id"]).reset_index(drop=True)


def temporal_split(labeled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fixed, chronological target-day partitions; all cities share cutoffs."""
    dates = pd.to_datetime(labeled["target_date"], errors="raise")
    train_frame = labeled.loc[dates.between("2015-01-01", "2021-12-31")].copy()
    validation = labeled.loc[dates.between("2022-01-01", "2022-12-31")].copy()
    test = labeled.loc[dates.between("2023-01-01", "2024-12-31")].copy()
    if any(part.empty for part in [train_frame, validation, test]):
        raise ValueError("Training, validation and test periods must all contain observations")
    return train_frame, validation, test


def read_warehouse(database_url: str) -> pd.DataFrame:
    import psycopg

    if not database_url:
        raise ValueError("Set INDIA_DATABASE_URL or pass --database-url")
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(WAREHOUSE_SQL)
            return pd.DataFrame(cursor.fetchall(), columns=[c.name for c in cursor.description])


def _scores(actual, predicted) -> dict:
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
        "r2": float(r2_score(actual, predicted)),
    }


def _pipeline(estimator):
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    # The ETL and event contract reject missing data. No imputation masks errors.
    preprocessor = ColumnTransformer([
        ("numeric", StandardScaler(), NUMERIC_FEATURES),
        ("city", OneHotEncoder(handle_unknown="error", sparse_output=False), ["city_name"]),
    ], remainder="drop")
    return Pipeline([("preprocessor", preprocessor), ("regressor", estimator)])


def _period(part: pd.DataFrame) -> dict:
    return {"rows": int(len(part)), "start": str(part.target_date.min()), "end": str(part.target_date.max())}


def train(database_url: str, outdir: str | Path) -> dict:
    """Select on 2022 only, refit through 2022, then evaluate untouched 2023-24."""
    import joblib
    from sklearn.base import clone
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import Ridge

    frame = read_warehouse(database_url)
    labeled = training_labels(frame)
    training, validation, test = temporal_split(labeled)
    if set(training.city_name.unique()) != set(CITIES):
        raise ValueError("The training warehouse must include all five documented cities")
    candidates = {
        "ridge": _pipeline(Ridge(alpha=10.0)),
        "random_forest": _pipeline(RandomForestRegressor(
            n_estimators=120, max_depth=8, min_samples_leaf=20, random_state=42, n_jobs=-1
        )),
    }
    validation_scores = {}
    for name, candidate in candidates.items():
        candidate.fit(training[FEATURE_COLUMNS], training.target_aqi)
        validation_scores[name] = _scores(
            validation.target_aqi, candidate.predict(validation[FEATURE_COLUMNS])
        )
    validation_scores["mean_baseline"] = _scores(
        validation.target_aqi, np.full(len(validation), training.target_aqi.mean())
    )
    validation_scores["persistence_baseline"] = _scores(validation.target_aqi, validation.aqi_lag1)
    selected_name = min(candidates, key=lambda name: validation_scores[name]["mae"])
    selected = clone(candidates[selected_name])
    development = pd.concat([training, validation], ignore_index=True)
    selected.fit(development[FEATURE_COLUMNS], development.target_aqi)
    predictions = selected.predict(test[FEATURE_COLUMNS])
    reference_mean = float(development.target_aqi.mean())
    test_scores = {
        "selected_model": _scores(test.target_aqi, predictions),
        "mean_baseline": _scores(test.target_aqi, np.full(len(test), reference_mean)),
        "persistence_baseline": _scores(test.target_aqi, test.aqi_lag1),
    }
    output = Path(outdir)
    output.mkdir(parents=True, exist_ok=True)
    temporary_model = output / "model.pending.joblib"
    joblib.dump(selected, temporary_model)
    model_path = output / "model.joblib"
    temporary_model.replace(model_path)
    model_sha = hashlib.sha256(model_path.read_bytes()).hexdigest()
    metadata = {
        "model_version": model_sha[:16],
        "model_sha256": model_sha,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_name": selected_name,
        "target": "AQI at the next local calendar day in India",
        "horizon_days": 1,
        "feature_columns": FEATURE_COLUMNS,
        "numeric_features": NUMERIC_FEATURES,
        "cities": CITIES,
        "source": "PostgreSQL india_dw.air_quality_enriched (validated dimensional warehouse)",
        "source_rows": int(len(frame)),
        "labeled_rows": int(len(labeled)),
        "dataset_sha256": hashlib.sha256(labeled.to_csv(index=False).encode("utf-8")).hexdigest(),
        "source_run_ids": sorted(frame.run_id.astype(str).unique().tolist()),
        "split": {"train": _period(training), "validation": _period(validation), "test": _period(test)},
        "fit_through": str(development.target_date.max()),
        "selection_rule": "Lowest validation MAE among Ridge and RandomForest; test never selects the model",
        "selection_validation_mae": validation_scores[selected_name]["mae"],
        "metrics": {"validation": validation_scores, "test": test_scores},
        "baseline_mean_aqi": reference_mean,
        "beats_mean_baseline_on_test": test_scores["selected_model"]["mae"] < test_scores["mean_baseline"]["mae"],
        "beats_persistence_on_test": test_scores["selected_model"]["mae"] < test_scores["persistence_baseline"]["mae"],
        "by_city_test": {
            city: _scores(test.loc[test.city_name.eq(city), "target_aqi"], predictions[test.city_name.eq(city)])
            for city in CITIES
        },
        "preprocessing": {
            "numeric": "StandardScaler fitted on the development partition only",
            "categorical": "OneHotEncoder fitted on city_name; unknown cities are rejected",
            "missing": "Missing/non-finite source values are rejected; no hidden imputation",
            "history": "First six dates and incomplete consecutive seven-day windows are excluded",
        },
        "limitations": [
            "Academic next-day prediction experiment, not a validated public warning system.",
            "The original Kaggle source does not establish measurement units or station provenance for every pollutant.",
            "ERA5 weather is historical reanalysis; only weather from the origin day enters a forecast event.",
            "Daily timestamps are assumed to represent local days in India, consistent with the earlier ETL.",
            "Weak associations and weak temporal persistence can limit accuracy; baseline comparisons are reported even when superior.",
            "The stream replays historical validated observations; event creation time is distinct from the historical observation date.",
            "The fitted model uses target dates through 2022 only; demo forecasts for 2023-24 are chronologically out of training.",
        ],
        "versions": {name: importlib.metadata.version(name) for name in ["scikit-learn", "pandas", "numpy", "joblib"]},
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    result = test[["city_id", "city_name", "origin_date", "target_date", "target_aqi", "aqi_lag1"]].copy()
    result["predicted_aqi"] = predictions
    result["mean_baseline"] = reference_mean
    result["error"] = result.predicted_aqi - result.target_aqi
    result.to_csv(output / "test_predictions.csv", index=False)
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("INDIA_DATABASE_URL"))
    parser.add_argument("--output", default="artifacts")
    args = parser.parse_args()
    metadata = train(args.database_url, args.output)
    print(json.dumps({"model_name": metadata["model_name"], "model_version": metadata["model_version"],
                      "test": metadata["metrics"]["test"]}, indent=2))


if __name__ == "__main__":
    main()
