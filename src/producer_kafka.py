from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


# Windows PowerShell may expose a legacy cp1252 console. Force UTF-8 so status
# messages cannot stop the producer after a Kafka record has been sent.
for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "market_data")

INPUT_CSV = Path(os.getenv("MARKET_CSV_PATH", "output/market_data_final.csv"))

SLEEP_SECONDS = float(os.getenv("STREAM_SLEEP_SECONDS", "0.5"))

SYMBOL_FILTER = os.getenv("SYMBOL_FILTER", "")
START_DATE = os.getenv("START_DATE", "")
END_DATE = os.getenv("END_DATE", "")
MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "100"))

REQUIRED_COLUMNS = [
    "date",
    "symbol",
    "stock_close",
    "oil_price",
    "gold_price",
]


producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def load_market_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path)

    # Normalize column names between versions: some CSVs have `close` instead of `stock_close`.
    if "stock_close" not in df.columns and "close" in df.columns:
        df = df.rename(columns={"close": "stock_close"})

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(
            f"CSV missing required columns: {missing}. "
            f"Current columns: {df.columns.tolist()}"
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["symbol"] = df["symbol"].astype(str).str.strip()

    numeric_columns = [
        "stock_close",
        "oil_price",
        "gold_price",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=REQUIRED_COLUMNS).reset_index(drop=True)

    if SYMBOL_FILTER:
        symbols = [s.strip() for s in SYMBOL_FILTER.split(",") if s.strip()]
        df = df[df["symbol"].isin(symbols)]

    if START_DATE:
        df = df[pd.to_datetime(df["date"]) >= pd.to_datetime(START_DATE)]

    if END_DATE:
        df = df[pd.to_datetime(df["date"]) <= pd.to_datetime(END_DATE)]

    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)

    if MAX_ROWS > 0:
        df = df.head(MAX_ROWS)

    return df


def build_message(row: pd.Series) -> dict:
    return {
        "date": row["date"],
        "symbol": row["symbol"],
        "stock_close": float(row["stock_close"]),
        "oil_price": float(row["oil_price"]),
        "gold_price": float(row["gold_price"]),
    }


def stream_data() -> None:
    df = load_market_data(INPUT_CSV)

    print("=" * 80)
    print("Kafka Producer started")
    print(f"- Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"- Topic: {TOPIC}")
    print(f"- Input CSV: {INPUT_CSV}")
    print(f"- Rows: {len(df)}")
    print(f"- Symbols: {df['symbol'].nunique()}")
    print(f"- Sleep seconds: {SLEEP_SECONDS}")
    print("=" * 80)

    for idx, row in df.iterrows():
        message = build_message(row)

        producer.send(TOPIC, value=message)

        if (idx + 1) % FLUSH_EVERY == 0:
            producer.flush()

        print(
            f"Đã gửi: {message['date']} | {message['symbol']} | "
            f"Stock: {message['stock_close']:.2f} | "
            f"Oil: {message['oil_price']:.2f} | "
            f"Gold: {message['gold_price']:.2f}"
        )

        time.sleep(SLEEP_SECONDS)

    producer.flush()


if __name__ == "__main__":
    try:
        stream_data()
    except KeyboardInterrupt:
        print("Dừng Producer.")
    finally:
        producer.close()
