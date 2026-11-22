"""Generate the point-in-time analytical report from the validated warehouse.

This application reads PostgreSQL once. Kafka predictions are displayed by the
separate consumer/dashboard application, never by this static report.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from html import escape
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd


WEATHER = ["temperature_2m_mean", "precipitation_sum", "wind_speed_10m_max"]
WEATHER_LABELS = ["Mean temperature (°C)", "Daily precipitation (mm)", "Maximum wind (m/s)"]
BUCKETS = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
REQUIRED_COLUMNS = ["measurement_id", "city_id", "city_name", "date", "aqi", "aqi_bucket", "run_id"] + WEATHER


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def validate_snapshot(frame: pd.DataFrame, run_records: list[dict]) -> pd.DataFrame:
    """Reject unapproved, incomplete or structurally inconsistent snapshots."""
    missing = set(REQUIRED_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Warehouse is missing required columns: {sorted(missing)}")
    if frame.empty:
        raise ValueError("The warehouse contains no measurements")
    df = frame.copy()
    if df[REQUIRED_COLUMNS].isna().any().any():
        raise ValueError("The report cannot use null warehouse values")
    df["date"] = pd.to_datetime(df["date"], errors="raise")
    if df.duplicated(["city_id", "date"]).any() or df["measurement_id"].duplicated().any():
        raise ValueError("Duplicate measurement identifiers or city/date keys")
    if not set(df.aqi_bucket).issubset(BUCKETS):
        raise ValueError("Unexpected AQI category in warehouse")
    numeric = df[["aqi"] + WEATHER].apply(pd.to_numeric, errors="raise")
    if not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise ValueError("Non-finite measurement in warehouse")
    if (numeric[["aqi", "precipitation_sum", "wind_speed_10m_max"]] < 0).any().any():
        raise ValueError("Negative AQI, precipitation or wind in warehouse")
    records = {r["run_id"]: r for r in run_records}
    for run_id, count in df.groupby("run_id").size().items():
        record = records.get(run_id)
        quality = record.get("quality_result") if record else None
        if isinstance(quality, str):
            quality = json.loads(quality)
        if not isinstance(quality, dict) or quality.get("success") is not True:
            raise ValueError(f"Run {run_id} has no successful data-quality approval")
        if record.get("rows") != int(count):
            raise ValueError(f"Run {run_id} is incomplete: {count} rows, expected {record.get('rows')}")
    # All five historical city series must cover the same daily interval.
    city_dates = df.groupby("city_id")["date"].agg(["min", "max", "count"])
    first, last = df.date.min(), df.date.max()
    expected_days = len(pd.date_range(first, last, freq="D"))
    if not (city_dates["min"].eq(first) & city_dates["max"].eq(last) & city_dates["count"].eq(expected_days)).all():
        raise ValueError("The warehouse snapshot has incomplete daily city coverage")
    return df.sort_values(["date", "city_id"]).reset_index(drop=True)


def read_validated_warehouse(database_url: str) -> tuple[pd.DataFrame, list[dict]]:
    import psycopg

    with psycopg.connect(database_url) as con:
        with con.cursor() as cur:
            # Snapshot isolation keeps approval metadata and fact rows coherent
            # if an Airflow load commits while this report is being generated.
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute("SELECT * FROM india_dw.air_quality_enriched ORDER BY date, city_id")
            frame = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
            cur.execute("SELECT run_id, loaded_at, rows, quality_result FROM india_dw.etl_runs WHERE run_id IN (SELECT DISTINCT run_id FROM india_dw.fact_air_quality)")
            fields = [c.name for c in cur.description]
            records = [dict(zip(fields, row)) for row in cur.fetchall()]
    return validate_snapshot(frame, records), records


def summarize(frame: pd.DataFrame) -> tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cities = frame.groupby("city_name", as_index=False).agg(
        observations=("aqi", "size"), mean_aqi=("aqi", "mean"), median_aqi=("aqi", "median")
    ).sort_values("mean_aqi", ascending=False)
    monthly = frame.assign(month=frame.date.dt.to_period("M").dt.to_timestamp()).groupby(
        ["city_name", "month"], as_index=False
    )["aqi"].mean()
    correlations = pd.DataFrame({
        city: group[["aqi"] + WEATHER].corr().loc["aqi", WEATHER]
        for city, group in frame.groupby("city_name")
    }).T
    correlations.index.name = "city_name"
    categories = frame.aqi_bucket.value_counts().reindex(BUCKETS, fill_value=0)
    finite_corr = correlations.stack().dropna()
    max_abs_corr = float(finite_corr.abs().max()) if len(finite_corr) else None
    high_month = monthly.loc[monthly.aqi.idxmax()]
    low_month = monthly.loc[monthly.aqi.idxmin()]
    summary = {
        "rows": len(frame), "cities": len(cities),
        "period_start": frame.date.min().date().isoformat(),
        "period_end": frame.date.max().date().isoformat(),
        "run_ids": sorted(frame.run_id.unique().tolist()),
        "aqi_mean": float(frame.aqi.mean()), "aqi_std": float(frame.aqi.std()),
        "city_statistics": cities.to_dict("records"),
        "category_counts": categories.to_dict(),
        "very_poor_or_severe_share": float(frame.aqi_bucket.isin(["Very Poor", "Severe"]).mean()),
        "aqi_weather_correlations_by_city": correlations.to_dict(orient="index"),
        "maximum_absolute_weather_correlation": max_abs_corr,
        "highest_city_month": {"city": high_month.city_name, "month": high_month.month.strftime("%Y-%m"), "mean_aqi": float(high_month.aqi)},
        "lowest_city_month": {"city": low_month.city_name, "month": low_month.month.strftime("%Y-%m"), "mean_aqi": float(low_month.aqi)},
        "source": "PostgreSQL india_dw.air_quality_enriched, approved ETL runs only",
    }
    return _json_safe(summary), cities, monthly, correlations


def _model_section(artifacts_dir: Path) -> str:
    metadata_file = artifacts_dir / "metadata.json"
    if not metadata_file.exists():
        return "<p>Model evaluation is not included in this report snapshot. Train the model and regenerate the report to include its measured results.</p>"
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    name = metadata.get("selected_model", metadata.get("model_name", "Trained regression model"))
    version = metadata.get("model_version", "not recorded")
    metrics = metadata.get("test_metrics", metadata.get("metrics", {}))
    if isinstance(metrics, dict) and isinstance(metrics.get("test"), dict):
        metrics = metrics["test"]
    rows = []
    if isinstance(metrics, dict):
        # Render both {model: {metric: value}} and a flat metrics mapping.
        if any(isinstance(v, dict) for v in metrics.values()):
            for label, values in metrics.items():
                if isinstance(values, dict):
                    rows.append({"Model / reference": label, **values})
        else:
            rows.append({"Model / reference": str(name), **metrics})
    table = pd.DataFrame(rows).to_html(index=False, escape=True, float_format=lambda x: f"{x:.4f}") if rows else "<p>No test metrics were recorded.</p>"
    limitations = metadata.get("limitations", [])
    if isinstance(limitations, str):
        limitations = [limitations]
    notes = "".join(f"<li>{escape(str(item))}</li>" for item in limitations)
    baseline_finding = ""
    if metadata.get('beats_mean_baseline_on_test') is False:
        baseline_finding = ("<p class='finding'>The fitted model does not improve test MAE over the constant development-mean baseline. "
            "This experiment demonstrates the inference architecture, but does not establish useful next-day forecasting skill.</p>")
    return (
        f"<p>Selected model: <strong>{escape(str(name))}</strong>. Artifact version: <code>{escape(str(version))}</code>.</p>"
        "<p>The target is next-day AQI for the same city. Features use only information available through the preceding day. "
        "Training uses 2015-2021, model selection uses 2022, and the final model is fitted on 2015-2022. The held-out test period is 2023-2024.</p>"
        + table + baseline_finding + "<p>MAE and RMSE are measured in AQI points; smaller values indicate lower error. R² compares performance with the test-set mean and may be negative. "
        "Mean and persistence references must be considered before claiming predictive value.</p>"
        + (f"<ul>{notes}</ul>" if notes else "")
        + "<p>This section describes offline evaluation. Incoming Kafka predictions belong to the separate live dashboard.</p>"
    )


def generate_report(database_url: str, output_dir, artifacts_dir="artifacts") -> dict:
    """Query approved analytical data and write an HTML report and evidence files."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    frame, records = read_validated_warehouse(database_url)
    summary, cities, monthly, correlations = summarize(frame)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    summary["generated_at_utc"] = generated_at
    summary["etl_runs"] = [{"run_id": r["run_id"], "loaded_at": str(r["loaded_at"]), "rows": r["rows"]} for r in records]
    sns.set_theme(style="whitegrid", context="notebook", palette="colorblind")

    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.barh(cities.city_name.iloc[::-1], cities.mean_aqi.iloc[::-1], color="#147d92")
    ax.set(xlabel="Mean AQI (original dataset scale)", ylabel="", title="Average AQI by city")
    for i, value in enumerate(cities.mean_aqi.iloc[::-1]):
        ax.text(value + 1, i, f"{value:.2f}", va="center", fontsize=10)
    ax.set_xlim(0, cities.mean_aqi.max() * 1.15)
    fig.tight_layout(); fig.savefig(output / "01_city_aqi.png", dpi=160); plt.close(fig)

    fig, ax = plt.subplots(figsize=(11, 5.2))
    sns.lineplot(data=monthly, x="month", y="aqi", hue="city_name", linewidth=1.4, ax=ax)
    ax.set(xlabel="Measurement month", ylabel="Monthly mean AQI", title="Monthly AQI across the historical period")
    ax.legend(title="City", ncol=5, loc="upper center", bbox_to_anchor=(.5, -0.17), frameon=False)
    fig.tight_layout(); fig.savefig(output / "02_monthly_aqi.png", dpi=160); plt.close(fig)

    shares = pd.crosstab(frame.city_name, frame.aqi_bucket, normalize="index").reindex(columns=BUCKETS, fill_value=0) * 100
    fig, ax = plt.subplots(figsize=(11, 5.2))
    shares.plot.barh(stacked=True, color=["#339966", "#80b66b", "#e4c75a", "#d9954c", "#ba5c58", "#783b65"], ax=ax)
    ax.set(xlabel="Share of city observations (%)", ylabel="", title="AQI categories retained from the original source", xlim=(0, 100))
    ax.legend(title="Original category", ncol=3, loc="upper center", bbox_to_anchor=(.5, -.17), frameon=False)
    fig.tight_layout(); fig.savefig(output / "03_category_shares.png", dpi=160); plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4.6))
    sns.heatmap(correlations.rename(columns=dict(zip(WEATHER, WEATHER_LABELS))), annot=True, fmt=".3f", cmap="RdBu_r", center=0, vmin=-1, vmax=1, ax=ax, cbar_kws={"label": "Pearson r with AQI"})
    ax.set(xlabel="Historical weather variable", ylabel="", title="Within-city weather association with AQI")
    fig.tight_layout(); fig.savefig(output / "04_weather_correlations.png", dpi=160); plt.close(fig)

    highest, lowest = cities.iloc[0], cities.iloc[-1]
    spread = float(highest.mean_aqi - lowest.mean_aqi)
    max_corr = summary["maximum_absolute_weather_correlation"]
    correlation_text = (
        f"The largest absolute within-city Pearson correlation is {max_corr:.3f}. "
        + ("These weak linear associations do not support a strong weather-only explanation or forecast of AQI." if max_corr < .2 else "This association needs predictive evaluation before it can support a forecast.")
        if max_corr is not None else "The correlation could not be estimated because the inputs have no variation."
    )
    monthly.to_csv(output / "monthly_aqi.csv", index=False)
    correlations.to_csv(output / "weather_correlations.csv")
    (output / "findings.json").write_text(json.dumps(_json_safe(summary), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    city_table = cities.rename(columns={"city_name": "City", "observations": "Observations", "mean_aqi": "Mean AQI", "median_aqi": "Median AQI"}).to_html(index=False, float_format=lambda x: f"{x:.2f}")
    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>India air quality | Analytical report</title>
<style>
body{{font:17px/1.65 system-ui,-apple-system,Segoe UI,sans-serif;color:#23323b;background:#f4f6f7;margin:0}}
main{{max-width:1080px;margin:auto;padding:42px 28px 70px;background:white}}
header{{border-top:6px solid #147d92;padding-top:24px;margin-bottom:35px}}h1{{font-size:2.3rem;line-height:1.15;margin:.3em 0}}
h2{{font-size:1.45rem;line-height:1.3;margin-top:2.4em;border-bottom:1px solid #d9e2e6;padding-bottom:10px}}
.eyebrow{{font-size:.8rem;font-weight:700;letter-spacing:.12em;color:#147d92}}.meta{{font-size:.86rem;color:#586b76}}
img{{display:block;max-width:100%;height:auto;margin:24px auto}}table{{border-collapse:collapse;width:100%;margin:24px 0;font-size:.91rem}}
th,td{{text-align:left;padding:10px 12px;border-bottom:1px solid #d9e2e6}}th{{background:#edf4f5}}code{{overflow-wrap:anywhere}}
.finding{{border-left:4px solid #147d92;padding:10px 20px;background:#f0f6f7}}footer{{border-top:1px solid #d9e2e6;margin-top:45px;padding-top:18px}}
@media print{{body,main{{background:white}}main{{padding:0}}h2{{break-after:avoid}}img,table{{break-inside:avoid}}}}
</style></head><body><main>
<header><div class="eyebrow">ETL FINAL DELIVERY · SAMUEL FLORES &amp; SANTIAGO BECERRA</div>
<h1>Air quality and weather in India</h1>
<p>A static analytical view of {summary['rows']:,} validated city-day measurements across {summary['cities']} cities.</p>
<p class="meta">Period: {summary['period_start']} to {summary['period_end']} · Generated: {escape(generated_at)} UTC<br>
Source: PostgreSQL <code>india_dw.air_quality_enriched</code> · Grain: one city on one measurement day.</p></header>
<p>This report supports historical comparison and review of the enriched data. It is a point-in-time output generated with Python after data-quality approval. The operational prediction dashboard is a separate application driven by the Kafka consumer.</p>
<h2>1. Which cities have the highest average AQI?</h2>
<p class="finding">{escape(str(highest.city_name))} has the highest mean AQI ({highest.mean_aqi:.2f}); {escape(str(lowest.city_name))} has the lowest ({lowest.mean_aqi:.2f}). The difference is {spread:.2f} AQI points. The overall mean is {summary['aqi_mean']:.2f}.</p>
<img src="01_city_aqi.png" alt="Horizontal bar chart comparing mean AQI across cities">
{city_table}
<p>These averages describe this dataset and period. They do not establish a ranking of current conditions or the exposure of individual residents.</p>
<h2>2. How does AQI change across months?</h2>
<p class="finding">The highest city-month average is {summary['highest_city_month']['mean_aqi']:.2f} in {escape(summary['highest_city_month']['city'])} ({summary['highest_city_month']['month']}); the lowest is {summary['lowest_city_month']['mean_aqi']:.2f} in {escape(summary['lowest_city_month']['city'])} ({summary['lowest_city_month']['month']}). These are historical monthly averages, not individual daily extremes.</p>
<img src="02_monthly_aqi.png" alt="Historical monthly average AQI, with one line per city">
<p>The monthly series preserves variation hidden by a single city average. The exported <code>monthly_aqi.csv</code> makes every plotted point reproducible. Aggregation uses actual observations by calendar month and city; no observations are invented or interpolated.</p>
<h2>3. How frequent are the original AQI categories?</h2>
<p class="finding">Very Poor and Severe together account for {summary['very_poor_or_severe_share']:.1%} of observations. These labels are retained from the original dataset and are not recalculated with a different country's AQI thresholds.</p>
<img src="03_category_shares.png" alt="Stacked bars showing the proportion of original AQI categories in each city">
<h2>4. Is historical weather associated with AQI?</h2>
<p class="finding">{escape(correlation_text)}</p>
<img src="04_weather_correlations.png" alt="Heatmap of Pearson correlation between AQI and three weather variables, calculated within each city">
<p>Correlations are calculated separately within each city to avoid interpreting differences between locations as a daily relationship. Association does not demonstrate causation; low Pearson correlation does not exclude every nonlinear relationship.</p>
<h2>5. How well does the next-day prediction model perform?</h2>
{_model_section(Path(artifacts_dir))}
<h2>6. Data quality, provenance and limits</h2>
<p>Air measurements originate from the first delivery's Kaggle dataset. Wikipedia coordinates identify the five cities and link them to historical ERA5 weather obtained through Open-Meteo. ERA5 represents reanalysis grid cells, not a colocated pollution-monitoring station.</p>
<p>Airflow joins the sources, executes Great Expectations, and blocks the analytical load when critical checks fail. This report verifies successful approval metadata, reconciles each active run's row count, rejects duplicate or missing keys and incomplete daily coverage, and reads the database in one consistent transaction.</p>
<p>The source does not document every pollutant unit. Original dates are interpreted as local days in India; that assumption needs source confirmation before claiming exact temporal alignment. The historical source and weak associations limit the operational interpretation of model predictions.</p>
<h2>7. Analytical and operational value</h2>
<p>The dimensional model enables reproducible city and time comparisons, quality auditing and offline model evaluation. The Kafka path demonstrates a separate operational process: validated historical records are replayed as events, the consumer applies one fitted model, and the live dashboard updates with its predictions. Historical replay is explicitly a simulation.</p>
<footer class="meta">Regenerate from the repository root: <code>python src/final_report.py --output reports/final --artifacts artifacts</code> with <code>INDIA_DATABASE_URL</code> configured. Re-run after training to include the latest offline model metrics. This HTML and its four image files must remain together.</footer>
</main></body></html>"""
    (output / "report.html").write_text(html, encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("INDIA_DATABASE_URL"))
    parser.add_argument("--output", default="reports/final")
    parser.add_argument("--artifacts", default="artifacts")
    args = parser.parse_args()
    if not args.database_url:
        parser.error("Set INDIA_DATABASE_URL or provide --database-url")
    summary = generate_report(args.database_url, args.output, args.artifacts)
    print(json.dumps({"rows": summary["rows"], "report": str(Path(args.output) / "report.html")}, indent=2))


if __name__ == "__main__":
    main()
