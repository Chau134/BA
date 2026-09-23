"""
mapreduce_analysis.py
==============================================================
MapReduce-style Feature Engineering for Big Data BI Project.

Input:
    output/market_data_final.csv

Output:
    output/market_analysis_results.csv
    output/market_analysis_results_weekly.csv
    output/market_analysis_results_monthly_volatility.csv

Main idea:
    - MAP: create daily returns and lag features
    - REDUCE: compute moving averages, volatility, rolling correlations
    - ML target: target_market_risk

ML task:
    Classify current market risk state:
        1 = HIGH RISK
        0 = NORMAL

This is not future price prediction.
It classifies the current market condition based on return, volatility,
correlation and alert-level features generated through MapReduce-style processing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


INPUT_PATH = os.getenv("INPUT_PATH", "output/market_data_final.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/market_analysis_results.csv")

ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "10"))

# Current risk target rule config
STOCK_RETURN_RISK_PCT = float(os.getenv("STOCK_RETURN_RISK_PCT", "2.0"))
COMMODITY_RETURN_RISK_PCT = float(os.getenv("COMMODITY_RETURN_RISK_PCT", "2.0"))
VOLATILITY_QUANTILE = float(os.getenv("VOLATILITY_QUANTILE", "0.75"))


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, pd.NA)
    return numerator / denominator


def load_data(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    df = pd.read_csv(path)

    if "date" not in df.columns:
        raise ValueError("Input CSV must contain 'date' column.")

    if "close" in df.columns and "stock_close" not in df.columns:
        df = df.rename(columns={"close": "stock_close"})

    required = ["date", "symbol", "stock_close", "oil_price"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. "
            f"Current columns: {df.columns.tolist()}"
        )

    if "gold_price" not in df.columns:
        df["gold_price"] = 0.0

    keep_cols = [
        "date",
        "symbol",
        "stock_close",
        "oil_price",
        "gold_price",
    ]

    df = df[keep_cols].copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.strip()

    for col in ["stock_close", "oil_price", "gold_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["date", "symbol", "stock_close", "oil_price"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    print("=" * 80)
    print("[LOAD] Market data")
    print(f"Rows: {len(df)}")
    print(f"Symbols: {sorted(df['symbol'].unique().tolist())}")
    print(f"Date range: {df['date'].min().date()} -> {df['date'].max().date()}")
    print("=" * 80)

    return df


def map_daily_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    MAP phase:
    Convert each daily row into basic time-series features.
    """

    frames = []

    for symbol, g in df.groupby("symbol", sort=True):
        g = g.sort_values("date").copy()

        g["stock_return_pct"] = g["stock_close"].pct_change() * 100
        g["oil_return_pct"] = g["oil_price"].pct_change() * 100

        if g["gold_price"].abs().sum() > 0:
            g["gold_return_pct"] = g["gold_price"].pct_change() * 100
        else:
            g["gold_return_pct"] = 0.0

        g["stock_return_lag_1"] = g["stock_return_pct"].shift(1)
        g["stock_return_lag_3"] = g["stock_return_pct"].shift(3)
        g["stock_return_lag_5"] = g["stock_return_pct"].shift(5)

        g["oil_return_lag_1"] = g["oil_return_pct"].shift(1)
        g["gold_return_lag_1"] = g["gold_return_pct"].shift(1)

        g["week"] = g["date"].dt.to_period("W").astype(str)
        g["month"] = g["date"].dt.to_period("M").astype(str)
        g["year"] = g["date"].dt.year

        frames.append(g)

    mapped = pd.concat(frames, ignore_index=True)

    print("[MAP] Daily features created")
    print(f"Rows: {len(mapped)}")

    return mapped


def reduce_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    REDUCE phase:
    Group by symbol and compute rolling statistical features.

    Also creates current market risk label:
        target_market_risk = 1 -> HIGH RISK
        target_market_risk = 0 -> NORMAL
    """

    win = ROLLING_WINDOW
    frames = []

    for symbol, g in df.groupby("symbol", sort=True):
        g = g.sort_values("date").copy()

        g["stock_ma"] = g["stock_close"].rolling(win).mean()
        g["oil_ma"] = g["oil_price"].rolling(win).mean()

        if g["gold_price"].abs().sum() > 0:
            g["gold_ma"] = g["gold_price"].rolling(win).mean()
        else:
            g["gold_ma"] = 0.0

        g["stock_volatility"] = g["stock_return_pct"].rolling(win).std()
        g["oil_volatility"] = g["oil_return_pct"].rolling(win).std()
        g["gold_volatility"] = g["gold_return_pct"].rolling(win).std()

        g["rolling_corr"] = (
            g["stock_close"]
            .rolling(win)
            .corr(g["oil_price"])
        )

        if g["gold_price"].abs().sum() > 0:
            g["stock_gold_corr"] = (
                g["stock_close"]
                .rolling(win)
                .corr(g["gold_price"])
            )
        else:
            g["stock_gold_corr"] = 0.0

        g["stock_ma_ratio"] = safe_divide(g["stock_close"], g["stock_ma"])
        g["oil_ma_ratio"] = safe_divide(g["oil_price"], g["oil_ma"])

        if g["gold_price"].abs().sum() > 0:
            g["gold_ma_ratio"] = safe_divide(g["gold_price"], g["gold_ma"])
        else:
            g["gold_ma_ratio"] = 0.0

        frames.append(g)

    reduced = pd.concat(frames, ignore_index=True)

    numeric_cols = [
        "stock_return_pct",
        "oil_return_pct",
        "gold_return_pct",
        "stock_return_lag_1",
        "stock_return_lag_3",
        "stock_return_lag_5",
        "oil_return_lag_1",
        "gold_return_lag_1",
        "stock_ma",
        "oil_ma",
        "gold_ma",
        "stock_volatility",
        "oil_volatility",
        "gold_volatility",
        "rolling_corr",
        "stock_gold_corr",
        "stock_ma_ratio",
        "oil_ma_ratio",
        "gold_ma_ratio",
    ]

    for col in numeric_cols:
        if col in reduced.columns:
            reduced[col] = pd.to_numeric(reduced[col], errors="coerce").round(6)

    reduced["alert_level"] = "LOW"

    medium_condition = (
        (reduced["oil_return_pct"].abs() >= 1.5)
        | (reduced["gold_return_pct"].abs() >= 1.5)
        | (reduced["rolling_corr"].abs() >= 0.4)
        | (reduced["stock_gold_corr"].abs() >= 0.4)
    )

    high_condition = (
        (
            (reduced["oil_return_pct"].abs() >= 3.0)
            | (reduced["gold_return_pct"].abs() >= 3.0)
        )
        & (
            (reduced["rolling_corr"].abs() >= 0.6)
            | (reduced["stock_gold_corr"].abs() >= 0.6)
        )
    )

    reduced.loc[medium_condition, "alert_level"] = "MEDIUM"
    reduced.loc[high_condition, "alert_level"] = "HIGH"

    stock_volatility_threshold = reduced["stock_volatility"].quantile(VOLATILITY_QUANTILE)
    oil_volatility_threshold = reduced["oil_volatility"].quantile(VOLATILITY_QUANTILE)
    gold_volatility_threshold = reduced["gold_volatility"].quantile(VOLATILITY_QUANTILE)

    # ==========================================================
    # ML target: current market risk classification
    # 1 = HIGH RISK
    # 0 = NORMAL
    #
    # This label is based on current market state, not future data.
    # ==========================================================

    reduced["target_market_risk"] = (
        (reduced["stock_return_pct"].abs() >= STOCK_RETURN_RISK_PCT)
        | (reduced["oil_return_pct"].abs() >= COMMODITY_RETURN_RISK_PCT)
        | (reduced["gold_return_pct"].abs() >= COMMODITY_RETURN_RISK_PCT)
        | (reduced["stock_volatility"] >= stock_volatility_threshold)
        | (reduced["oil_volatility"] >= oil_volatility_threshold)
        | (reduced["gold_volatility"] >= gold_volatility_threshold)
        | (reduced["alert_level"] == "HIGH")
    ).astype("float")

    reduced["business_signal"] = reduced.apply(build_business_signal, axis=1)

    print("[REDUCE] Rolling features created")
    print(f"Rows: {len(reduced)}")
    print("Alert distribution:")
    print(reduced["alert_level"].value_counts().to_string())

    print("\nCurrent market risk target distribution:")
    print(reduced["target_market_risk"].value_counts(dropna=False).to_string())
    print(reduced["target_market_risk"].value_counts(normalize=True, dropna=True).to_string())

    print("\nRisk rule thresholds:")
    print(f"stock_return_abs >= {STOCK_RETURN_RISK_PCT}%")
    print(f"commodity_return_abs >= {COMMODITY_RETURN_RISK_PCT}%")
    print(f"stock_volatility >= {stock_volatility_threshold:.6f}")
    print(f"oil_volatility >= {oil_volatility_threshold:.6f}")
    print(f"gold_volatility >= {gold_volatility_threshold:.6f}")

    return reduced


def build_business_signal(row: pd.Series) -> str:
    alert = row.get("alert_level", "LOW")
    market_risk = row.get("target_market_risk", 0)
    stock_return = row.get("stock_return_pct", 0)
    oil_corr = row.get("rolling_corr", 0)
    gold_corr = row.get("stock_gold_corr", 0)

    market_risk = 0 if pd.isna(market_risk) else market_risk
    stock_return = 0 if pd.isna(stock_return) else stock_return
    oil_corr = 0 if pd.isna(oil_corr) else oil_corr
    gold_corr = 0 if pd.isna(gold_corr) else gold_corr

    if market_risk == 1:
        return "Current market state is classified as HIGH RISK. Review exposure and monitor volatility."

    if alert == "HIGH" and stock_return < 0 and (oil_corr > 0 or gold_corr > 0):
        return "High commodity correlation with negative stock movement. Review short-term risk."

    if abs(oil_corr) < 0.3 and abs(gold_corr) < 0.3:
        return "Weak commodity relationship. Avoid decisions based only on commodities."

    if oil_corr <= -0.5 or gold_corr <= -0.5:
        return "Stock is diverging from commodity movement. Check stock-specific factors."

    return "Market condition is currently normal. Continue monitoring trend and volatility."


def reduce_weekly_summary(df: pd.DataFrame) -> pd.DataFrame:
    weekly = (
        df.groupby(["symbol", "week"])
        .agg(
            stock_avg=("stock_close", "mean"),
            stock_max=("stock_close", "max"),
            stock_min=("stock_close", "min"),
            oil_avg=("oil_price", "mean"),
            oil_max=("oil_price", "max"),
            oil_min=("oil_price", "min"),
            gold_avg=("gold_price", "mean"),
            gold_max=("gold_price", "max"),
            gold_min=("gold_price", "min"),
            count=("date", "count"),
        )
        .reset_index()
        .rename(columns={"week": "period"})
    )

    weekly.insert(0, "period_type", "weekly")

    return weekly


def reduce_monthly_volatility(df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        df.groupby(["symbol", "month"])
        .agg(
            stock_volatility=("stock_return_pct", "std"),
            oil_volatility=("oil_return_pct", "std"),
            gold_volatility=("gold_return_pct", "std"),
            stock_avg=("stock_close", "mean"),
            oil_avg=("oil_price", "mean"),
            gold_avg=("gold_price", "mean"),
            count=("date", "count"),
        )
        .reset_index()
        .rename(columns={"month": "period"})
    )

    monthly.insert(0, "period_type", "monthly_volatility")

    return monthly


def save_results(
    daily_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    output_path: str,
) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    daily_df.to_csv(output_path, index=False)

    weekly_path = output_path.replace(".csv", "_weekly.csv")
    monthly_path = output_path.replace(".csv", "_monthly_volatility.csv")

    weekly_df.to_csv(weekly_path, index=False)
    monthly_df.to_csv(monthly_path, index=False)

    print("=" * 80)
    print("[OUTPUT]")
    print(f"Daily ML feature dataset: {output_path}")
    print(f"Weekly summary: {weekly_path}")
    print(f"Monthly volatility: {monthly_path}")
    print("=" * 80)


def main() -> None:
    print("=" * 80)
    print("MAPREDUCE-STYLE FEATURE ENGINEERING")
    print("Current Market Risk Classification Target")
    print("=" * 80)
    print(f"Input: {INPUT_PATH}")
    print(f"Rolling window: {ROLLING_WINDOW}")
    print(f"Stock return risk threshold: {STOCK_RETURN_RISK_PCT}%")
    print(f"Commodity return risk threshold: {COMMODITY_RETURN_RISK_PCT}%")
    print(f"Volatility quantile threshold: {VOLATILITY_QUANTILE}")
    print("=" * 80)

    raw_df = load_data(INPUT_PATH)
    mapped_df = map_daily_features(raw_df)
    features_df = reduce_rolling_features(mapped_df)

    weekly_df = reduce_weekly_summary(mapped_df)
    monthly_df = reduce_monthly_volatility(mapped_df)

    save_results(features_df, weekly_df, monthly_df, OUTPUT_PATH)

    valid_ml_rows = features_df.dropna(subset=["target_market_risk"])
    print(f"Rows usable for ML: {len(valid_ml_rows)}")

    latest_rows = (
        features_df.dropna(subset=["rolling_corr"])
        .sort_values(["symbol", "date"])
        .groupby("symbol")
        .tail(1)
    )

    print("\nLatest rows by symbol:")
    for _, row in latest_rows.iterrows():
        print(
            f"{row['symbol']} | {row['date'].date()} | "
            f"Stock={row['stock_close']:.2f} | "
            f"Oil={row['oil_price']:.2f} | "
            f"Gold={row['gold_price']:.2f} | "
            f"MarketRisk={row.get('target_market_risk')} | "
            f"Alert={row['alert_level']}"
        )


if __name__ == "__main__":
    main()