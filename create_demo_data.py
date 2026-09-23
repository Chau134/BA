"""
create_demo_data.py
============================================================
Create a small demo CSV for Kafka streaming presentation.

Input:
    output/market_data_final.csv

Output:
    output/market_data_demo.csv

Default behavior:
    - Take latest rows for AAPL, MSFT, TSLA
    - 120 rows per symbol
    - Sort by date, then symbol
    - Producer will stream rows in chronological order

Usage:
    python create_demo_data.py

Custom examples:
    DEMO_SYMBOLS="AAPL,MSFT,TSLA" python create_demo_data.py
    DEMO_SYMBOLS="TSLA" DEMO_ROWS_PER_SYMBOL=150 python create_demo_data.py
    DEMO_START_DATE="2024-01-01" python create_demo_data.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


INPUT_CSV = os.getenv("DEMO_INPUT_CSV", "output/market_data_final.csv")
OUTPUT_CSV = os.getenv("DEMO_OUTPUT_CSV", "output/market_data_demo.csv")

DEMO_SYMBOLS = os.getenv("DEMO_SYMBOLS", "AAPL,MSFT,TSLA")
DEMO_START_DATE = os.getenv("DEMO_START_DATE", "2024-01-01")
DEMO_ROWS_PER_SYMBOL = int(os.getenv("DEMO_ROWS_PER_SYMBOL", "120"))


def parse_symbols(value: str) -> list[str]:
    return [symbol.strip() for symbol in value.split(",") if symbol.strip()]


def main() -> None:
    input_path = Path(INPUT_CSV)
    output_path = Path(OUTPUT_CSV)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}. "
            f"Run python etl_pipeline.py first."
        )

    symbols = parse_symbols(DEMO_SYMBOLS)

    if not symbols:
        raise ValueError("DEMO_SYMBOLS is empty.")

    df = pd.read_csv(input_path)

    required_cols = ["date", "symbol", "stock_close", "oil_price", "gold_price"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(
            f"Missing required columns: {missing_cols}. "
            f"Current columns: {df.columns.tolist()}"
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "symbol"])

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df = df[df["symbol"].isin(symbols)].copy()

    if DEMO_START_DATE:
        df = df[df["date"] >= pd.to_datetime(DEMO_START_DATE)].copy()

    if df.empty:
        raise ValueError(
            "Demo dataframe is empty. "
            "Check DEMO_SYMBOLS or DEMO_START_DATE."
        )

    df = (
        df.sort_values(["symbol", "date"])
        .groupby("symbol", group_keys=False)
        .tail(DEMO_ROWS_PER_SYMBOL)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print("=" * 80)
    print("Demo data created")
    print("=" * 80)
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Symbols requested: {symbols}")
    print(f"Rows per symbol: {DEMO_ROWS_PER_SYMBOL}")
    print(f"Start date filter: {DEMO_START_DATE}")
    print("-" * 80)
    print(f"Total rows: {len(df)}")
    print("Rows by symbol:")
    print(df["symbol"].value_counts().to_string())
    print("-" * 80)
    print(f"Date range: {df['date'].min()} -> {df['date'].max()}")
    print("-" * 80)
    print("Head:")
    print(df[["date", "symbol", "stock_close", "oil_price", "gold_price"]].head(10).to_string(index=False))
    print("-" * 80)
    print("Tail:")
    print(df[["date", "symbol", "stock_close", "oil_price", "gold_price"]].tail(10).to_string(index=False))
    print("=" * 80)


if __name__ == "__main__":
    main()