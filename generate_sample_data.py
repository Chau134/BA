import json
import os
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import yfinance as yf


DATA_DIR = "data"
YEARS_BACK = int(os.getenv("YEARS_BACK", "50"))

STOCK_SYMBOLS = [
    symbol.strip()
    for symbol in os.getenv(
        "STOCK_SYMBOLS",
        "^DJI,^GSPC,^IXIC,^RUT,AAPL,MSFT,TSLA,XOM,JPM,WMT",
    ).split(",")
    if symbol.strip()
]

OIL_SYMBOL = os.getenv("OIL_SYMBOL", "BZ=F")
GOLD_SYMBOL = os.getenv("GOLD_SYMBOL", "GC=F")


os.makedirs(DATA_DIR, exist_ok=True)


def normalize_yfinance_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df


def get_source_1_stock_csv(symbols: Optional[List[str]] = None) -> bool:
    """
    Tải dữ liệu stock/index cho nhiều mã và lưu dạng long format.
    """

    symbols = symbols or STOCK_SYMBOLS

    print(f"--- Đang tải dữ liệu Stock cho các mã: {', '.join(symbols)} ---")

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * YEARS_BACK)

        print(f"Khoảng thời gian yêu cầu: {start_date.date()} đến {end_date.date()}")

        expected_columns = [
            "date",
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
            "Dividends",
            "Stock Splits",
        ]

        all_frames = []

        for symbol in symbols:
            print(f"  -> Đang tải mã: {symbol}")

            df = yf.download(
                symbol,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                actions=True,
                progress=False,
            )

            if df.empty:
                print(f"  -> Bỏ qua {symbol}: không có dữ liệu")
                continue

            df = normalize_yfinance_columns(df)

            df = df.reset_index().rename(columns={"Date": "date"})

            for column in expected_columns:
                if column not in df.columns:
                    df[column] = 0 if column in {"Dividends", "Stock Splits"} else pd.NA

            df = df[expected_columns].copy()

            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime(
                "%Y-%m-%d"
            )

            df = df.rename(
                columns={
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Adj Close": "adj_close",
                    "Volume": "volume",
                    "Dividends": "dividends",
                    "Stock Splits": "stock_splits",
                }
            )

            df["symbol"] = symbol

            df = df[
                [
                    "date",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "adj_close",
                    "volume",
                    "dividends",
                    "stock_splits",
                ]
            ]

            all_frames.append(df)

        if not all_frames:
            print("Lỗi: Không tải được dữ liệu stock cho bất kỳ mã nào")
            return False

        final_df = pd.concat(all_frames, ignore_index=True)

        output_path = os.path.join(DATA_DIR, "stock_data.csv")
        final_df.to_csv(output_path, index=False)

        print(f"✓ Thành công! Đã lưu {len(final_df)} dòng dữ liệu vào: {output_path}")

        return True

    except Exception as exc:
        print(f"✗ Lỗi khi tải dữ liệu Stock: {exc}")
        return False


def download_commodity_json(symbol: str, output_filename: str, description: str) -> bool:
    """
    Tải dữ liệu commodity từ Yahoo Finance và lưu JSON.
    """

    print(f"--- Đang tải dữ liệu {description} cho mã: {symbol} ---")

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * YEARS_BACK)

        print(f"  Khoảng thời gian yêu cầu: {start_date.date()} đến {end_date.date()}")

        df = yf.download(
            symbol,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            actions=False,
            progress=False,
        )

        if df.empty:
            print(f"Lỗi: Không tải được dữ liệu {description} cho {symbol}")
            return False

        df = normalize_yfinance_columns(df)

        df = df.reset_index().rename(columns={"Date": "date"})

        expected_columns = [
            "date",
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
        ]

        for column in expected_columns:
            if column not in df.columns:
                df[column] = pd.NA

        df = df[expected_columns].copy()

        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )

        df = df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )

        raw_data = {
            "data": df.to_dict("records"),
            "meta_data": {
                "source": "Yahoo Finance",
                "symbol": symbol,
                "description": f"{description} futures historical OHLCV",
                "interval": "daily",
                "updated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        }

        output_path = os.path.join(DATA_DIR, output_filename)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, indent=4, ensure_ascii=False)

        print(f"✓ Thành công! Đã lưu {len(df)} dòng dữ liệu vào: {output_path}")

        return True

    except Exception as exc:
        print(f"✗ Lỗi khi tải dữ liệu {description}: {exc}")
        return False


def get_source_2_oil_json() -> bool:
    return download_commodity_json(
        symbol=OIL_SYMBOL,
        output_filename="oil_price_raw.json",
        description="Oil",
    )


def get_gold_data() -> bool:
    return download_commodity_json(
        symbol=GOLD_SYMBOL,
        output_filename="gold_price_raw.json",
        description="Gold",
    )


def main() -> None:
    print("=" * 80)
    print(f"LẤY DỮ LIỆU STOCK, OIL & GOLD - Tối đa {YEARS_BACK} năm")
    print("Yahoo Finance sẽ trả dữ liệu theo khoảng thời gian khả dụng.")
    print("=" * 80)

    stock_data = get_source_1_stock_csv(STOCK_SYMBOLS)
    oil_data = get_source_2_oil_json()
    gold_data = get_gold_data()

    print("\n" + "=" * 80)

    if stock_data and oil_data and gold_data:
        print("✓ HOÀN THÀNH: Đã lấy dữ liệu Stock, Oil và Gold thành công!")
        print("  Các file được lưu trong thư mục data/")
        print("  Bước tiếp theo:")
        print("  python etl_pipeline.py")
    else:
        print("✗ CÓ LỖI: Không lấy được đủ dữ liệu.")

    print("=" * 80)


if __name__ == "__main__":
    main()