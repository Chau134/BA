from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


METRICS_PATH = os.getenv("METRICS_PATH", "output/parquet/market_metrics")
ANALYSIS_CSV = os.getenv("ANALYSIS_CSV", "output/market_analysis_results.csv")
DASHBOARD_MODE = os.getenv("DASHBOARD_MODE", "streaming")
PREDICTIONS_PATH = os.getenv("PREDICTIONS_PATH", "output/parquet/market_predictions")
MODEL_PATH = os.getenv("MODEL_PATH", "models/stock_direction_rf")
MODEL_META_PATH = os.getenv("MODEL_META_PATH", "models/stock_direction_rf_meta.json")

REFRESH_EVERY = os.getenv("STREAMLIT_REFRESH_EVERY", "5s")
TAIL_ROWS = int(os.getenv("TAIL_ROWS", "300"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "180"))
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "AAPL")

OIL_LABEL = os.getenv("OIL_LABEL", "Crude Oil")
GOLD_LABEL = os.getenv("GOLD_LABEL", "Gold")


def load_model_metadata() -> dict:
    path = Path(MODEL_META_PATH)

    if not path.exists():
        return {
            "feature_columns": [
                "stock_close",
                "oil_price",
                "gold_price",
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
            ],
            "positive_class": "HIGH RISK",
            "negative_class": "NORMAL",
        }

    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


MODEL_META = load_model_metadata()
ML_FEATURE_COLUMNS = MODEL_META.get("feature_columns", [])
ML_POSITIVE_CLASS = MODEL_META.get("positive_class", "HIGH RISK")
ML_NEGATIVE_CLASS = MODEL_META.get("negative_class", "NORMAL")


st.set_page_config(
    page_title="Real-time BI Dashboard",
    page_icon="📈",
    layout="wide",
)

def filter_recent_data(df: pd.DataFrame, lookback_days: int, tail_rows: int) -> pd.DataFrame:
    """
    Chỉ giữ dữ liệu gần nhất để dashboard không vẽ toàn bộ lịch sử nhiều năm.
    """

    if df.empty:
        return df

    date_col = get_date_col(df)

    df = df.dropna(subset=[date_col]).copy()
    df = df.sort_values(date_col).reset_index(drop=True)

    latest_date = df[date_col].max()

    if lookback_days > 0:
        start_date = latest_date - pd.Timedelta(days=lookback_days)
        df = df[df[date_col] >= start_date].copy()

    if tail_rows > 0:
        df = df.tail(tail_rows).copy()

    return df.sort_values(date_col).reset_index(drop=True)

def normalize_metrics_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "market_date" in df.columns:
        df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for column in ["kafka_timestamp", "processing_time"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    numeric_columns = [
        "stock_close",
        "oil_price",
        "gold_price",
        "stock_ma",
        "oil_ma",
        "gold_ma",
        "stock_return_pct",
        "oil_return_pct",
        "gold_return_pct",
        "stock_volatility",
        "oil_volatility",
        "gold_volatility",
        "rolling_corr",
        "stock_gold_corr",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    # prediction numeric columns
    for column in ["prediction", "probability_high_risk"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


# def load_metrics(path: str) -> pd.DataFrame:
#     """
#     Load dashboard data.

#     Priority:
#     1. Spark streaming parquet metrics
#     2. Offline MapReduce CSV output
#     """

#     parquet_path = Path(path)

#     if parquet_path.exists():
#         try:
#             df = pd.read_parquet(parquet_path)

#             if not df.empty:
#                 df = normalize_metrics_df(df)

#                 date_col = "market_date" if "market_date" in df.columns else "date"

#                 if "symbol" in df.columns:
#                     return df.sort_values(["symbol", date_col]).reset_index(drop=True)

#                 return (
#                     df.sort_values(date_col)
#                     .drop_duplicates(subset=[date_col], keep="last")
#                     .tail(TAIL_ROWS)
#                     .reset_index(drop=True)
#                 )

#         except Exception as exc:
#             st.sidebar.warning(f"Không đọc được parquet metrics: {exc}")

#     csv_path = Path(ANALYSIS_CSV)

#     if csv_path.exists():
#         try:
#             df = pd.read_csv(csv_path)

#             if not df.empty:
#                 df = normalize_metrics_df(df)

#                 date_col = "date" if "date" in df.columns else "market_date"

#                 if "symbol" in df.columns:
#                     return df.sort_values(["symbol", date_col]).reset_index(drop=True)

#                 return df.sort_values(date_col).tail(TAIL_ROWS).reset_index(drop=True)

#         except Exception as exc:
#             st.sidebar.warning(f"Không đọc được analysis CSV: {exc}")

#     return pd.DataFrame()

def load_metrics(path: str) -> pd.DataFrame:
    """
    Load dashboard data.

    DASHBOARD_MODE:
    - streaming: chỉ đọc Spark Parquet metrics
    - offline: chỉ đọc MapReduce CSV
    - auto: ưu tiên Parquet, fallback CSV
    """

    parquet_path = Path(path)
    csv_path = Path(ANALYSIS_CSV)

    if DASHBOARD_MODE in {"streaming", "auto"}:
        if parquet_path.exists():
            try:
                df = pd.read_parquet(parquet_path)

                if not df.empty:
                    df = normalize_metrics_df(df)
                    date_col = "market_date" if "market_date" in df.columns else "date"

                    if "symbol" in df.columns:
                        return df.sort_values(["symbol", date_col]).reset_index(drop=True)

                    return (
                        df.sort_values(date_col)
                        .drop_duplicates(subset=[date_col], keep="last")
                        .tail(TAIL_ROWS)
                        .reset_index(drop=True)
                    )

            except Exception as exc:
                st.sidebar.warning(f"Spark đang ghi Parquet, chờ batch tiếp theo...")

                # Quan trọng:
                # Nếu đang demo streaming thì KHÔNG fallback sang CSV cũ.
                if DASHBOARD_MODE == "streaming":
                    return pd.DataFrame()

        if DASHBOARD_MODE == "streaming":
            return pd.DataFrame()

    if DASHBOARD_MODE in {"offline", "auto"}:
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)

                if not df.empty:
                    df = normalize_metrics_df(df)
                    date_col = "date" if "date" in df.columns else "market_date"

                    if "symbol" in df.columns:
                        return df.sort_values(["symbol", date_col]).reset_index(drop=True)

                    return df.sort_values(date_col).tail(TAIL_ROWS).reset_index(drop=True)

            except Exception as exc:
                st.sidebar.warning(f"Không đọc được analysis CSV: {exc}")

    return pd.DataFrame()


def load_predictions(path: str) -> pd.DataFrame:
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        return pd.DataFrame()

    try:
        df = pd.read_parquet(p)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    if "market_date" in df.columns:
        df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce")

    for col in ["kafka_timestamp", "processing_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "probability_high_risk" not in df.columns and "probability_up" in df.columns:
        df["probability_high_risk"] = df["probability_up"]

    if "prediction_label" in df.columns:
        df["prediction_label"] = df["prediction_label"].astype(str)

    numeric_cols = [
        "stock_close",
        "oil_price",
        "gold_price",
        "stock_return_pct",
        "oil_return_pct",
        "gold_return_pct",
        "stock_ma",
        "oil_ma",
        "gold_ma",
        "stock_volatility",
        "oil_volatility",
        "gold_volatility",
        "rolling_corr",
        "stock_gold_corr",
        "prediction",
        "probability_high_risk",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["market_date", "symbol"]) if not df.empty else df
    if not df.empty:
        df = df.sort_values(["symbol", "market_date"]).reset_index(drop=True)

    return df


def load_eval() -> pd.DataFrame:
    path = Path(os.getenv("ML_EVAL_PATH", "output/ml_evaluation.csv"))

    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def load_feature_importance() -> pd.DataFrame:
    path = Path(os.getenv("ML_FEATURE_IMPORTANCE_PATH", "output/ml_feature_importance.csv"))

    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def prepare_ml_feature_row(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame()

    date_col = get_date_col(history_df)
    df = history_df.copy().sort_values(date_col).reset_index(drop=True)

    for column in ["stock_close", "oil_price", "gold_price"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "stock_return_pct" not in df.columns and "stock_close" in df.columns:
        df["stock_return_pct"] = df["stock_close"].pct_change() * 100
    if "oil_return_pct" not in df.columns and "oil_price" in df.columns:
        df["oil_return_pct"] = df["oil_price"].pct_change() * 100
    if "gold_return_pct" not in df.columns and "gold_price" in df.columns:
        df["gold_return_pct"] = df["gold_price"].pct_change() * 100

    if "stock_return_pct" in df.columns:
        df["stock_return_lag_1"] = df["stock_return_pct"].shift(1)
        df["stock_return_lag_3"] = df["stock_return_pct"].shift(3)
        df["stock_return_lag_5"] = df["stock_return_pct"].shift(5)

    if "oil_return_pct" in df.columns:
        df["oil_return_lag_1"] = df["oil_return_pct"].shift(1)

    if "gold_return_pct" in df.columns:
        df["gold_return_lag_1"] = df["gold_return_pct"].shift(1)

    window = max(int(os.getenv("ROLLING_WINDOW", "10")), 2)

    if "stock_close" in df.columns:
        df["stock_ma"] = df["stock_close"].rolling(window, min_periods=1).mean()
        df["stock_volatility"] = df["stock_close"].rolling(window, min_periods=2).std()
    if "oil_price" in df.columns:
        df["oil_ma"] = df["oil_price"].rolling(window, min_periods=1).mean()
        df["oil_volatility"] = df["oil_price"].rolling(window, min_periods=2).std()
    if "gold_price" in df.columns:
        df["gold_ma"] = df["gold_price"].rolling(window, min_periods=1).mean()
        df["gold_volatility"] = df["gold_price"].rolling(window, min_periods=2).std()

    if {"stock_close", "oil_price"}.issubset(df.columns):
        df["rolling_corr"] = df["stock_close"].rolling(window, min_periods=2).corr(df["oil_price"])
    if {"stock_close", "gold_price"}.issubset(df.columns):
        df["stock_gold_corr"] = df["stock_close"].rolling(window, min_periods=2).corr(df["gold_price"])

    if "stock_ma" in df.columns:
        df["stock_ma_ratio"] = df["stock_close"] / df["stock_ma"].replace(0, pd.NA)
    if "oil_ma" in df.columns:
        df["oil_ma_ratio"] = df["oil_price"] / df["oil_ma"].replace(0, pd.NA)
    if "gold_ma" in df.columns:
        df["gold_ma_ratio"] = df["gold_price"] / df["gold_ma"].replace(0, pd.NA)

    feature_row = df.tail(1).copy()

    if not ML_FEATURE_COLUMNS:
        return pd.DataFrame()

    for column in ML_FEATURE_COLUMNS:
        if column not in feature_row.columns:
            feature_row[column] = 0.0
        feature_row[column] = pd.to_numeric(feature_row[column], errors="coerce").fillna(0.0)

    return feature_row[ML_FEATURE_COLUMNS]


@st.cache_resource(show_spinner=False)
def get_ml_model():
    from pyspark.ml import PipelineModel

    if not Path(MODEL_PATH).exists():
        return None

    return PipelineModel.load(MODEL_PATH)


@st.cache_resource(show_spinner=False)
def get_ml_spark_session():
    import pyspark
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("Streamlit-ML-Scoring")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def predict_latest_verdict(history_df: pd.DataFrame):
    features_df = prepare_ml_feature_row(history_df)

    if features_df.empty:
        return None

    model = get_ml_model()
    if model is None:
        return None

    spark = get_ml_spark_session()
    spark_df = spark.createDataFrame(features_df)
    prediction_row = model.transform(spark_df).select("prediction", "probability").head()

    if prediction_row is None:
        return None

    probability_high_risk = float(prediction_row["probability"][1])
    prediction = float(prediction_row["prediction"])
    label = ML_POSITIVE_CLASS if prediction == 1.0 else ML_NEGATIVE_CLASS

    return {
        "prediction": prediction,
        "prediction_label": label,
        "probability_high_risk": probability_high_risk,
        "source": "live model",
    }


def get_date_col(df: pd.DataFrame) -> str:
    if "date" in df.columns:
        return "date"

    if "market_date" in df.columns:
        return "market_date"

    raise ValueError("DataFrame must contain 'date' or 'market_date' column.")


def metric_delta_text(df: pd.DataFrame, column: str) -> str:
    if len(df) < 2 or column not in df.columns:
        return "N/A"

    current = df.iloc[-1][column]
    previous = df.iloc[-2][column]

    if pd.isna(current) or pd.isna(previous):
        return "N/A"

    return f"{current - previous:+.2f}"


def build_price_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    date_col = get_date_col(df)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df["stock_close"],
            mode="lines+markers",
            name=f"{symbol} Close",
        ),
        secondary_y=False,
    )

    if "stock_ma" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["stock_ma"],
                mode="lines",
                name=f"{symbol} Moving Avg",
                line={"dash": "dash"},
            ),
            secondary_y=False,
        )

    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df["oil_price"],
            mode="lines+markers",
            name=OIL_LABEL,
        ),
        secondary_y=True,
    )

    if "oil_ma" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["oil_ma"],
                mode="lines",
                name=f"{OIL_LABEL} Moving Avg",
                line={"dash": "dot"},
            ),
            secondary_y=True,
        )

    if "gold_price" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["gold_price"],
                mode="lines+markers",
                name=GOLD_LABEL,
            ),
            secondary_y=True,
        )

    if "gold_ma" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["gold_ma"],
                mode="lines",
                name=f"{GOLD_LABEL} Moving Avg",
                line={"dash": "longdash"},
            ),
            secondary_y=True,
        )

    fig.update_layout(
        title=f"{symbol} vs {OIL_LABEL} & {GOLD_LABEL}",
        hovermode="x unified",
        legend_title="Series",
        xaxis_title="Date",
    )

    fig.update_yaxes(title_text=f"{symbol} Close", secondary_y=False)
    fig.update_yaxes(title_text="Commodity Price", secondary_y=True)

    return fig


def build_corr_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    date_col = get_date_col(df)

    fig = go.Figure()

    if "rolling_corr" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["rolling_corr"],
                mode="lines+markers",
                name=f"{symbol} vs {OIL_LABEL}",
            )
        )

    if "stock_gold_corr" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["stock_gold_corr"],
                mode="lines+markers",
                name=f"{symbol} vs {GOLD_LABEL}",
            )
        )

    fig.update_layout(
        title=f"Rolling Correlation: {symbol} vs Commodities",
        xaxis_title="Date",
        yaxis_title="Correlation",
        yaxis_range=[-1, 1],
        hovermode="x unified",
    )

    return fig


def build_return_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    date_col = get_date_col(df)

    fig = go.Figure()

    if "stock_return_pct" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["stock_return_pct"],
                mode="lines",
                name=f"{symbol} Return %",
            )
        )

    if "oil_return_pct" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["oil_return_pct"],
                mode="lines",
                name=f"{OIL_LABEL} Return %",
            )
        )

    if "gold_return_pct" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["gold_return_pct"],
                mode="lines",
                name=f"{GOLD_LABEL} Return %",
            )
        )

    fig.update_layout(
        title="Daily Return Comparison",
        xaxis_title="Date",
        yaxis_title="Return %",
        hovermode="x unified",
    )

    return fig


def build_probability_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    if "probability_high_risk" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["market_date"],
                y=df["probability_high_risk"],
                mode="lines+markers",
                name="Probability HIGH RISK",
            )
        )

    fig.add_hline(y=0.5, line_dash="dash")

    fig.update_layout(
        title="ML Probability: Current HIGH RISK",
        xaxis_title="Date",
        yaxis_title="Probability",
        yaxis_range=[0, 1],
        hovermode="x unified",
    )

    return fig


def prediction_badge(label: str, prob_high_risk: float) -> None:
    if label == "HIGH RISK":
        st.error(f"Nhận định ML: HIGH RISK | Xác suất rủi ro: {prob_high_risk:.2%}")
    else:
        st.success(f"Nhận định ML: NORMAL | Xác suất rủi ro: {prob_high_risk:.2%}")


def build_alert_messages(latest: pd.Series, symbol: str) -> list[str]:
    messages: list[str] = []

    alert_level = latest.get("alert_level", "LOW")
    stock_oil_corr = latest.get("rolling_corr")
    stock_gold_corr = latest.get("stock_gold_corr")
    oil_return = latest.get("oil_return_pct")
    gold_return = latest.get("gold_return_pct")
    stock_return = latest.get("stock_return_pct")
    signal = latest.get("business_signal", "")

    if alert_level == "HIGH":
        messages.append(
            f"🔴 Cảnh báo cao: commodity biến động mạnh và có liên hệ đáng kể với {symbol}."
        )
    elif alert_level == "MEDIUM":
        messages.append(
            f"🟠 Cảnh báo trung bình: mối liên hệ giữa {symbol} và commodity đang tăng."
        )
    else:
        messages.append(
            f"🟢 Cảnh báo thấp: chưa có tín hiệu rủi ro mạnh từ oil hoặc gold."
        )

    if pd.notna(stock_oil_corr):
        if stock_oil_corr >= 0.6:
            messages.append(f"{symbol} đang di chuyển cùng chiều khá mạnh với {OIL_LABEL}.")
        elif stock_oil_corr <= -0.5:
            messages.append(f"{symbol} đang nghịch pha đáng kể với {OIL_LABEL}.")

    if pd.notna(stock_gold_corr):
        if stock_gold_corr >= 0.6:
            messages.append(f"{symbol} đang di chuyển cùng chiều khá mạnh với {GOLD_LABEL}.")
        elif stock_gold_corr <= -0.5:
            messages.append(f"{symbol} đang nghịch pha đáng kể với {GOLD_LABEL}.")

    if pd.notna(oil_return) and abs(oil_return) >= 3:
        messages.append(f"{OIL_LABEL} biến động mạnh trong quan sát gần nhất.")

    if pd.notna(gold_return) and abs(gold_return) >= 3:
        messages.append(f"{GOLD_LABEL} biến động mạnh trong quan sát gần nhất.")

    if pd.notna(stock_return) and stock_return <= -2:
        messages.append(f"{symbol} đang giảm đáng kể. Có thể phát tín hiệu cảnh báo sớm.")

    if isinstance(signal, str) and signal.strip():
        messages.append(f"Gợi ý hệ thống: {signal}")

    return messages


st.title("📊 Real-time Big Data BI: Stock vs Oil & Gold")
st.caption("Nguồn dữ liệu: Kafka → Spark Streaming hoặc Offline MapReduce Analysis")

df_all = load_metrics(METRICS_PATH)

if "symbol" in df_all.columns and not df_all.empty:
    available_symbols = sorted(df_all["symbol"].dropna().unique().tolist())
else:
    available_symbols = [DEFAULT_SYMBOL]


with st.sidebar:
    st.header("Thiết lập")

    st.write(f"**Metrics path:** `{METRICS_PATH}`")
    st.write(f"**Analysis CSV:** `{ANALYSIS_CSV}`")
    st.write(f"**Auto refresh:** `{REFRESH_EVERY}`")

    st.divider()

    st.subheader("Chọn mã cổ phiếu")

    if "selected_symbol" not in st.session_state:
        if DEFAULT_SYMBOL in available_symbols:
            st.session_state.selected_symbol = DEFAULT_SYMBOL
        else:
            st.session_state.selected_symbol = available_symbols[0]

    if st.session_state.selected_symbol not in available_symbols:
        st.session_state.selected_symbol = available_symbols[0]

    selected_symbol = st.selectbox(
        "Mã cần phân tích:",
        available_symbols,
        index=available_symbols.index(st.session_state.selected_symbol),
    )

    st.session_state.selected_symbol = selected_symbol

    st.divider()

    st.write("**Schema chính:**")
    st.code("date, symbol, stock_close, oil_price, gold_price")

    st.divider()

    st.subheader("Khoảng thời gian hiển thị")

    st.session_state.lookback_days = st.number_input(
        "Số ngày gần nhất:",
        min_value=7,
        max_value=3650,
        value=LOOKBACK_DAYS,
        step=30,
    )

    st.session_state.tail_rows = st.number_input(
        "Số dòng tối đa:",
        min_value=50,
        max_value=5000,
        value=TAIL_ROWS,
        step=50,
    )

@st.fragment(run_every=REFRESH_EVERY)
def live_dashboard() -> None:
    df = load_metrics(METRICS_PATH)

    if df.empty:
        st.warning(
            "Chưa có dữ liệu. Hãy chạy ETL + MapReduce analysis hoặc Spark processor + Kafka producer."
        )
        return

    date_col = get_date_col(df)
    selected_symbol = st.session_state.get("selected_symbol", DEFAULT_SYMBOL)

    if "symbol" in df.columns:
        df_filtered = df[df["symbol"] == selected_symbol].copy()
    else:
        df_filtered = df.copy()

    if df_filtered.empty:
        st.warning(f"Không có dữ liệu cho {selected_symbol}.")
        return

    df_filtered = df_filtered.sort_values(date_col).reset_index(drop=True)

    lookback_days = int(st.session_state.get("lookback_days", LOOKBACK_DAYS))
    tail_rows = int(st.session_state.get("tail_rows", TAIL_ROWS))

    df_visible = filter_recent_data(
        df_filtered,
        lookback_days=lookback_days,
        tail_rows=tail_rows,
    )

    if df_visible.empty:
        st.warning(f"Không có dữ liệu gần đây cho {selected_symbol}.")
        return

    latest = df_visible.iloc[-1]

    visible_start = df_visible[date_col].min()
    visible_end = df_visible[date_col].max()

    st.caption(
        f"Đang hiển thị {len(df_visible)} dòng gần nhất "
        f"từ {visible_start.date()} đến {visible_end.date()}."
    )

    k1, k2, k3, k4, k5 = st.columns(5)

    k1.metric(
        f"{selected_symbol} Close",
        f"{latest['stock_close']:.2f}",
        metric_delta_text(df_filtered, "stock_close"),
    )

    k2.metric(
        OIL_LABEL,
        f"{latest['oil_price']:.2f}",
        metric_delta_text(df_filtered, "oil_price"),
    )

    if "gold_price" in latest.index and pd.notna(latest.get("gold_price")):
        k3.metric(
            GOLD_LABEL,
            f"{latest['gold_price']:.2f}",
            metric_delta_text(df_filtered, "gold_price"),
        )
    else:
        k3.metric(GOLD_LABEL, "N/A")

    stock_oil_corr = latest.get("rolling_corr")
    k4.metric(
        "Stock-Oil Corr",
        "N/A" if pd.isna(stock_oil_corr) else f"{stock_oil_corr:.4f}",
    )

    k5.metric("Alert Level", str(latest.get("alert_level", "LOW")))

    preds = load_predictions(PREDICTIONS_PATH)
    preds_sym = pd.DataFrame()
    if not preds.empty:
        preds_sym = preds[preds["symbol"] == selected_symbol].copy()

    ml_verdict = None
    if not preds_sym.empty:
        pl = preds_sym.sort_values("market_date").iloc[-1]
        ml_verdict = {
            "prediction_label": pl.get("prediction_label") if "prediction_label" in pl.index else None,
            "probability_high_risk": pl.get("probability_high_risk") if "probability_high_risk" in pl.index else None,
            "source": "predictions parquet",
        }

    if ml_verdict is None:
        ml_verdict = predict_latest_verdict(df_filtered)

    if ml_verdict and ml_verdict.get("prediction_label"):
        ml_prob = ml_verdict.get("probability_high_risk")
        ml_source = ml_verdict.get("source", "model")
        st.metric(
            "ML Prediction",
            str(ml_verdict["prediction_label"]),
            f"p={ml_prob:.2f} · {ml_source}" if pd.notna(ml_prob) else ml_source,
        )

    tab_live, tab_model, tab_table = st.tabs(
        ["📡 Live Risk Classification", "🧠 Model Evaluation", "📋 Latest Data"]
    )

    with tab_live:
        left, right = st.columns([2, 1])

        with left:
            st.plotly_chart(
                build_price_chart(df_visible, selected_symbol),
                config={"responsive": True},
            )

        with right:
            if not preds_sym.empty:
                prob_df = preds_sym.sort_values("market_date").tail(TAIL_ROWS).copy()
                st.plotly_chart(
                    build_probability_chart(prob_df),
                    config={"responsive": True},
                )
            else:
                fallback_prob = float(ml_verdict.get("probability_high_risk", 0.0)) if ml_verdict else 0.0
                fallback_prob_df = pd.DataFrame(
                    {
                        "market_date": [latest[date_col]],
                        "probability_high_risk": [fallback_prob],
                    }
                )
                st.plotly_chart(
                    build_probability_chart(fallback_prob_df),
                    config={"responsive": True},
                )

        st.plotly_chart(
            build_corr_chart(df_visible, selected_symbol),
            config={"responsive": True},
        )

        st.plotly_chart(
            build_return_chart(df_visible, selected_symbol),
            config={"responsive": True},
        )

        st.subheader("Decision Support")

        for message in build_alert_messages(latest, selected_symbol):
            if latest.get("alert_level") == "HIGH":
                st.error(message)
            elif latest.get("alert_level") == "MEDIUM":
                st.warning(message)
            else:
                st.info(message)

        if ml_verdict and ml_verdict.get("prediction_label"):
            ml_label = ml_verdict["prediction_label"]
            ml_prob = ml_verdict.get("probability_high_risk")
            ml_source = ml_verdict.get("source", "model")
            ml_msg = f"ML prediction: {ml_label}"
            if ml_prob is not None and pd.notna(ml_prob):
                ml_msg += f" (prob={ml_prob:.2f})"
            ml_msg += f" · source: {ml_source}"

            prediction_badge(str(ml_label), float(ml_prob or 0.0))
            st.info(ml_msg)
        else:
            st.info("Nhận định ML: chưa tính được dự báo, hãy kiểm tra model hoặc dữ liệu đầu vào.")

    with tab_model:
        eval_df = load_eval()
        fi_df = load_feature_importance()

        if eval_df.empty:
            st.warning("Chưa có file evaluation. Hãy chạy: python src/spark_ml_train.py")
        else:
            row = eval_df.iloc[0]

            m1, m2, m3, m4, m5 = st.columns(5)

            m1.metric("AUC", f"{row.get('auc', 0):.4f}")
            m2.metric("Accuracy", f"{row.get('accuracy', 0):.4f}")
            m3.metric("Baseline", f"{row.get('baseline_accuracy', 0):.4f}")
            m4.metric("F1", f"{row.get('f1', 0):.4f}")
            m5.metric("Improve", f"{row.get('improvement_over_baseline', 0):+.4f}")

            st.dataframe(eval_df, width="stretch")

        if not fi_df.empty:
            fig = go.Figure()
            top = fi_df.sort_values("importance", ascending=False).head(15)

            fig.add_trace(
                go.Bar(
                    x=top["importance"],
                    y=top["feature"],
                    orientation="h",
                    name="Feature Importance",
                )
            )

            fig.update_layout(
                title="Top Feature Importance",
                xaxis_title="Importance",
                yaxis_title="Feature",
                yaxis={"categoryorder": "total ascending"},
            )

            st.plotly_chart(fig, config={"responsive": True})

    with tab_table:
        display_cols = [
            "market_date",
            "symbol",
            "stock_close",
            "oil_price",
            "gold_price",
            "stock_return_pct",
            "oil_return_pct",
            "gold_return_pct",
            "rolling_corr",
            "stock_gold_corr",
            "prediction",
            "prediction_label",
            "probability_high_risk",
            "alert_level",
            "business_signal",
            "processing_time",
        ]

        df_table = df_visible.copy()
        if not preds_sym.empty:
            merge_cols = [col for col in ["prediction", "prediction_label", "probability_high_risk", "processing_time"] if col in preds_sym.columns]
            if merge_cols:
                pred_join = preds_sym[["market_date", "symbol", *merge_cols]].drop_duplicates(["market_date", "symbol"], keep="last")
                df_table = df_table.merge(pred_join, on=["market_date", "symbol"], how="left")

        existing_cols = [col for col in display_cols if col in df_table.columns]

        st.dataframe(
            df_table[existing_cols].sort_values("market_date", ascending=False),
            width="stretch",
        )


live_dashboard()