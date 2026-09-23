import pandas as pd


INPUT_PATH = "output/market_data_final.csv"


def main() -> None:
    df = pd.read_csv(INPUT_PATH)

    print("Dữ liệu ghép - Stock + Oil + Gold")
    print("=" * 70)

    print(f"Tổng dòng: {len(df)}")
    print(f"Khoảng thời gian: {df['date'].min()} đến {df['date'].max()}")

    print("\nCác cột:")
    print(list(df.columns))

    if "symbol" in df.columns:
        print(f"\nDanh sách symbol: {sorted(df['symbol'].dropna().unique().tolist())}")
        print("\nSố dòng theo symbol:")
        print(df.groupby("symbol").size().sort_values(ascending=False))

    print("\n5 dòng đầu tiên:")
    print(df.head())

    print("\n5 dòng cuối cùng:")
    print(df.tail())

    summary_columns = [
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

    existing_summary_columns = [
        column for column in summary_columns if column in df.columns
    ]

    print("\nThống kê cơ bản:")
    print(df[existing_summary_columns].describe())

    if "symbol" in df.columns and "stock_close" in df.columns:
        print("\nStock close trung bình theo symbol:")
        print(df.groupby("symbol")["stock_close"].mean().sort_values(ascending=False))


if __name__ == "__main__":
    main()