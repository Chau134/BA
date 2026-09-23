import json
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"

STOCK_CSV_PATH = DATA_DIR / "stock_data.csv"
OIL_RAW_PATH = DATA_DIR / "oil_price_raw.json"
GOLD_RAW_PATH = DATA_DIR / "gold_price_raw.json"

MERGED_DAILY_PATH = OUTPUT_DIR / "market_data_final.csv"


def load_stock_csv(path: Path) -> pd.DataFrame:
    """
    Load stock/index data in long format.

    Expected input columns:
    - date
    - symbol
    - open
    - high
    - low
    - close
    - volume
    """

    if not path.exists():
        raise FileNotFoundError(f"Missing stock CSV file: {path}")

    df = pd.read_csv(path)

    if "date" in df.columns:
        date_col = "date"
    elif "timestamp" in df.columns:
        date_col = "timestamp"
    else:
        raise ValueError("Stock CSV must contain either 'date' or 'timestamp' column.")

    if "symbol" not in df.columns:
        raise ValueError("Stock CSV must contain 'symbol' column for multi-symbol pipeline.")

    numeric_columns = ["open", "high", "low", "close", "volume"]
    missing_columns = [column for column in numeric_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(f"Stock CSV missing columns: {missing_columns}")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=[date_col, "symbol", "close"]).copy()

    output_columns = [date_col, "symbol"] + numeric_columns

    return (
        df[output_columns]
        .rename(columns={date_col: "date"})
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )


def load_json_raw(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing JSON file: {path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def transform_oil(oil_raw: dict) -> pd.DataFrame:
    """
    Transform oil JSON data.

    Supports:
    - old schema: value
    - new schema: open, high, low, close, volume
    """

    rows = oil_raw.get("data", [])

    if not rows:
        raise ValueError("Oil raw data missing 'data' array.")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Old schema support
    if "value" in df.columns:
        cleaned_value = df["value"].astype(str).str.strip()
        cleaned_value = cleaned_value.replace({
            ".": None,
            "": None,
            "nan": None,
            "None": None,
        })

        df["oil_price"] = pd.to_numeric(cleaned_value, errors="coerce")
        df = df.dropna(subset=["date", "oil_price"]).copy()

        return df[["date", "oil_price"]]

    # New yfinance OHLCV schema
    column_map = {
        "open": "oil_open",
        "high": "oil_high",
        "low": "oil_low",
        "close": "oil_close",
        "volume": "oil_volume",
    }

    missing = [column for column in column_map if column not in df.columns]

    if missing:
        raise ValueError(f"Oil raw data missing columns: {missing}")

    for source_column, target_column in column_map.items():
        df[target_column] = pd.to_numeric(df[source_column], errors="coerce")

    df["oil_price"] = df["oil_close"]

    df = df.dropna(subset=["date", "oil_price"]).copy()

    return df[
        [
            "date",
            "oil_open",
            "oil_high",
            "oil_low",
            "oil_price",
            "oil_volume",
        ]
    ]


def transform_gold(gold_raw: dict) -> pd.DataFrame:
    """
    Transform gold JSON data from yfinance OHLCV schema.
    """

    rows = gold_raw.get("data", [])

    if not rows:
        raise ValueError("Gold raw data missing 'data' array.")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    column_map = {
        "open": "gold_open",
        "high": "gold_high",
        "low": "gold_low",
        "close": "gold_close",
        "volume": "gold_volume",
    }

    missing = [column for column in column_map if column not in df.columns]

    if missing:
        raise ValueError(f"Gold raw data missing columns: {missing}")

    for source_column, target_column in column_map.items():
        df[target_column] = pd.to_numeric(df[source_column], errors="coerce")

    df["gold_price"] = df["gold_close"]

    df = df.dropna(subset=["date", "gold_price"]).copy()

    return df[
        [
            "date",
            "gold_open",
            "gold_high",
            "gold_low",
            "gold_price",
            "gold_volume",
        ]
    ]


def build_daily_join(
    stock_df: pd.DataFrame,
    oil_df: pd.DataFrame,
    gold_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join stock, oil and gold data by date.

    Main output schema used by downstream pipeline:
    - date
    - symbol
    - stock_close
    - oil_price
    - gold_price
    """

    stock_df = stock_df.rename(columns={"close": "stock_close"}).copy()

    merged = stock_df.merge(oil_df, on="date", how="inner")
    merged = merged.merge(gold_df, on="date", how="inner")

    keep_columns = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "stock_close",
        "volume",
        "oil_open",
        "oil_high",
        "oil_low",
        "oil_price",
        "oil_volume",
        "gold_open",
        "gold_high",
        "gold_low",
        "gold_price",
        "gold_volume",
    ]

    existing_columns = [column for column in keep_columns if column in merged.columns]
    merged = merged[existing_columns]

    merged = merged.sort_values(["date", "symbol"]).reset_index(drop=True)

    return merged


def run_etl() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    stock_df = load_stock_csv(STOCK_CSV_PATH)

    oil_raw = load_json_raw(OIL_RAW_PATH)
    gold_raw = load_json_raw(GOLD_RAW_PATH)

    oil_df = transform_oil(oil_raw)
    gold_df = transform_gold(gold_raw)

    merged_daily = build_daily_join(stock_df, oil_df, gold_df)
    merged_daily["date"] = merged_daily["date"].dt.strftime("%Y-%m-%d")

    merged_daily.to_csv(MERGED_DAILY_PATH, index=False)

    print("ETL completed.")
    print(f"- Daily inner-join output: {MERGED_DAILY_PATH}")
    print(f"- Rows: {len(merged_daily)}")
    print(f"- Symbols: {merged_daily['symbol'].nunique()}")
    print(f"- Columns: {merged_daily.columns.tolist()}")


if __name__ == "__main__":
    run_etl()