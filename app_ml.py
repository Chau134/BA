# from __future__ import annotations

# import os
# from pathlib import Path

# import pandas as pd
# import plotly.graph_objects as go
# import streamlit as st


# PREDICTIONS_PATH = os.getenv("PREDICTIONS_PATH", "output/parquet/market_predictions")
# EVAL_PATH = os.getenv("ML_EVAL_PATH", "output/ml_evaluation.csv")
# FEATURE_IMPORTANCE_PATH = os.getenv(
#     "ML_FEATURE_IMPORTANCE_PATH",
#     "output/ml_feature_importance.csv",
# )

# REFRESH_EVERY = os.getenv("STREAMLIT_REFRESH_EVERY", "1s")
# DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "TSLA")
# LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "90"))
# TAIL_ROWS = int(os.getenv("TAIL_ROWS", "120"))


# st.set_page_config(
#     page_title="Spark ML Current Market Risk Dashboard",
#     page_icon="🚨",
#     layout="wide",
# )


# st.markdown(
#     """
#     <style>
#     .block-container {
#         padding-top: 1.4rem;
#         padding-bottom: 2rem;
#     }

#     div[data-testid="stMetric"] {
#         background: #0f172a;
#         border: 1px solid #334155;
#         padding: 1rem;
#         border-radius: 1rem;
#         box-shadow: 0 8px 20px rgba(15, 23, 42, 0.18);
#     }

#     div[data-testid="stMetric"] label {
#         color: #cbd5e1 !important;
#     }
#     </style>
#     """,
#     unsafe_allow_html=True,
# )


# def load_predictions() -> pd.DataFrame:
#     path = Path(PREDICTIONS_PATH)

#     if not path.exists():
#         return pd.DataFrame()

#     try:
#         df = pd.read_parquet(path)
#     except Exception:
#         return pd.DataFrame()

#     if df.empty:
#         return df

#     if "market_date" in df.columns:
#         df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce")

#     for col in ["kafka_timestamp", "processing_time"]:
#         if col in df.columns:
#             df[col] = pd.to_datetime(df[col], errors="coerce")

#     if "probability_high_risk" not in df.columns and "probability_up" in df.columns:
#         df["probability_high_risk"] = df["probability_up"]

#     if "prediction_label" in df.columns:
#         df["prediction_label"] = (
#             df["prediction_label"]
#             .astype(str)
#             .replace({"UP": "HIGH RISK", "DOWN": "NORMAL"})
#         )

#     numeric_cols = [
#         "stock_close",
#         "oil_price",
#         "gold_price",
#         "stock_return_pct",
#         "oil_return_pct",
#         "gold_return_pct",
#         "stock_ma",
#         "oil_ma",
#         "gold_ma",
#         "stock_volatility",
#         "oil_volatility",
#         "gold_volatility",
#         "rolling_corr",
#         "stock_gold_corr",
#         "prediction",
#         "probability_high_risk",
#     ]

#     for col in numeric_cols:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")

#     df = df.dropna(subset=["market_date", "symbol"])
#     df = df.sort_values(["symbol", "market_date"]).reset_index(drop=True)

#     return df


# def filter_recent(df: pd.DataFrame) -> pd.DataFrame:
#     if df.empty:
#         return df

#     latest_date = df["market_date"].max()
#     start_date = latest_date - pd.Timedelta(days=LOOKBACK_DAYS)

#     df = df[df["market_date"] >= start_date].copy()

#     if TAIL_ROWS > 0:
#         df = df.tail(TAIL_ROWS)

#     return df.sort_values("market_date").reset_index(drop=True)


# def load_eval() -> pd.DataFrame:
#     path = Path(EVAL_PATH)

#     if not path.exists():
#         return pd.DataFrame()

#     try:
#         return pd.read_csv(path)
#     except Exception:
#         return pd.DataFrame()


# def load_feature_importance() -> pd.DataFrame:
#     path = Path(FEATURE_IMPORTANCE_PATH)

#     if not path.exists():
#         return pd.DataFrame()

#     try:
#         return pd.read_csv(path)
#     except Exception:
#         return pd.DataFrame()


# def build_price_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
#     fig = go.Figure()

#     fig.add_trace(
#         go.Scatter(
#             x=df["market_date"],
#             y=df["stock_close"],
#             mode="lines+markers",
#             name=f"{symbol} Close",
#         )
#     )

#     if "stock_ma" in df.columns:
#         fig.add_trace(
#             go.Scatter(
#                 x=df["market_date"],
#                 y=df["stock_ma"],
#                 mode="lines",
#                 name="Stock MA",
#             )
#         )

#     fig.update_layout(
#         title=f"{symbol} Price Streaming",
#         xaxis_title="Date",
#         yaxis_title="Stock Price",
#         hovermode="x unified",
#     )

#     return fig


# def build_probability_chart(df: pd.DataFrame) -> go.Figure:
#     fig = go.Figure()

#     fig.add_trace(
#         go.Scatter(
#             x=df["market_date"],
#             y=df["probability_high_risk"],
#             mode="lines+markers",
#             name="Probability HIGH RISK",
#         )
#     )

#     fig.add_hline(y=0.5, line_dash="dash")

#     fig.update_layout(
#         title="ML Probability: Current HIGH RISK",
#         xaxis_title="Date",
#         yaxis_title="Probability",
#         yaxis_range=[0, 1],
#         hovermode="x unified",
#     )

#     return fig


# def build_corr_chart(df: pd.DataFrame) -> go.Figure:
#     fig = go.Figure()

#     if "rolling_corr" in df.columns:
#         fig.add_trace(
#             go.Scatter(
#                 x=df["market_date"],
#                 y=df["rolling_corr"],
#                 mode="lines+markers",
#                 name="Stock-Oil Corr",
#             )
#         )

#     if "stock_gold_corr" in df.columns:
#         fig.add_trace(
#             go.Scatter(
#                 x=df["market_date"],
#                 y=df["stock_gold_corr"],
#                 mode="lines+markers",
#                 name="Stock-Gold Corr",
#             )
#         )

#     fig.update_layout(
#         title="Rolling Correlation",
#         xaxis_title="Date",
#         yaxis_title="Correlation",
#         yaxis_range=[-1, 1],
#         hovermode="x unified",
#     )

#     return fig


# def build_return_chart(df: pd.DataFrame) -> go.Figure:
#     fig = go.Figure()

#     if "stock_return_pct" in df.columns:
#         fig.add_trace(
#             go.Scatter(
#                 x=df["market_date"],
#                 y=df["stock_return_pct"],
#                 mode="lines",
#                 name="Stock Return %",
#             )
#         )

#     if "oil_return_pct" in df.columns:
#         fig.add_trace(
#             go.Scatter(
#                 x=df["market_date"],
#                 y=df["oil_return_pct"],
#                 mode="lines",
#                 name="Oil Return %",
#             )
#         )

#     if "gold_return_pct" in df.columns:
#         fig.add_trace(
#             go.Scatter(
#                 x=df["market_date"],
#                 y=df["gold_return_pct"],
#                 mode="lines",
#                 name="Gold Return %",
#             )
#         )

#     fig.update_layout(
#         title="Daily Return Comparison",
#         xaxis_title="Date",
#         yaxis_title="Return %",
#         hovermode="x unified",
#     )

#     return fig


# def prediction_badge(label: str, prob_high_risk: float) -> None:
#     if label == "HIGH RISK":
#         st.error(f"🚨 Current Market State: HIGH RISK | Risk Probability: {prob_high_risk:.2%}")
#     else:
#         st.success(f"✅ Current Market State: NORMAL | Risk Probability: {prob_high_risk:.2%}")


# st.title("🚨 Spark ML Current Market Risk Dashboard")
# st.caption("Kafka → Spark Structured Streaming → Spark MLlib → Current Market Risk Classification")

# with st.sidebar:
#     st.header("Settings")
#     st.write(f"**Predictions path:** `{PREDICTIONS_PATH}`")
#     st.write(f"**Evaluation path:** `{EVAL_PATH}`")
#     st.write(f"**Refresh:** `{REFRESH_EVERY}`")
#     st.write(f"**Lookback days:** `{LOOKBACK_DAYS}`")
#     st.write(f"**Tail rows:** `{TAIL_ROWS}`")

#     st.divider()
#     st.subheader("Symbol")

#     selected_symbol = st.text_input(
#         "Symbol cần theo dõi",
#         value=DEFAULT_SYMBOL,
#     ).strip()

#     if not selected_symbol:
#         selected_symbol = DEFAULT_SYMBOL

#     st.session_state.selected_symbol = selected_symbol


# @st.fragment(run_every=REFRESH_EVERY)
# def live_app() -> None:
#     df = load_predictions()

#     if df.empty:
#         st.warning(
#             "Chưa có prediction. Hãy chạy Spark streaming predictor và Kafka producer."
#         )
#         return

#     symbols = sorted(df["symbol"].dropna().unique().tolist())
#     selected_symbol = st.session_state.get("selected_symbol", DEFAULT_SYMBOL)

#     df_symbol = df[df["symbol"] == selected_symbol].copy()

#     if df_symbol.empty:
#         st.warning(
#             f"Chưa có dữ liệu streaming cho symbol `{selected_symbol}`. "
#             f"Symbols hiện có trong prediction: {symbols}"
#         )
#         return

#     df_visible = filter_recent(df_symbol)

#     if df_visible.empty:
#         st.warning(f"No recent data for {selected_symbol}")
#         return

#     latest = df_visible.iloc[-1]

#     st.caption(
#         f"Showing {len(df_visible)} rows from "
#         f"{df_visible['market_date'].min().date()} to "
#         f"{df_visible['market_date'].max().date()}"
#     )

#     k1, k2, k3, k4, k5 = st.columns(5)

#     k1.metric(
#         f"{selected_symbol} Close",
#         f"{latest['stock_close']:.2f}",
#     )

#     k2.metric(
#         "Oil",
#         f"{latest['oil_price']:.2f}",
#     )

#     if "gold_price" in latest.index:
#         k3.metric(
#             "Gold",
#             f"{latest['gold_price']:.2f}",
#         )
#     else:
#         k3.metric("Gold", "N/A")

#     k4.metric(
#         "Risk Prob.",
#         f"{latest['probability_high_risk']:.2%}",
#     )

#     k5.metric(
#         "Alert",
#         str(latest.get("alert_level", "LOW")),
#     )

#     prediction_badge(
#         str(latest.get("prediction_label", "NORMAL")),
#         float(latest.get("probability_high_risk", 0.0)),
#     )

#     tab_live, tab_model, tab_table = st.tabs(
#         ["📡 Live Risk Classification", "🧠 Model Evaluation", "📋 Latest Data"]
#     )

#     with tab_live:
#         left, right = st.columns([2, 1])

#         with left:
#             st.plotly_chart(
#                 build_price_chart(df_visible, selected_symbol),
#                 width="stretch",
#             )

#         with right:
#             st.plotly_chart(
#                 build_probability_chart(df_visible),
#                 width="stretch",
#             )

#         st.plotly_chart(
#             build_corr_chart(df_visible),
#             width="stretch",
#         )

#         st.plotly_chart(
#             build_return_chart(df_visible),
#             width="stretch",
#         )

#         signal = latest.get("business_signal", "")

#         if isinstance(signal, str) and signal.strip():
#             st.info(f"Business signal: {signal}")

#     with tab_model:
#         eval_df = load_eval()
#         fi_df = load_feature_importance()

#         if eval_df.empty:
#             st.warning("Chưa có file evaluation. Hãy chạy: python src/spark_ml_train.py")
#         else:
#             row = eval_df.iloc[0]

#             m1, m2, m3, m4, m5 = st.columns(5)

#             m1.metric("AUC", f"{row.get('auc', 0):.4f}")
#             m2.metric("Accuracy", f"{row.get('accuracy', 0):.4f}")
#             m3.metric("Baseline", f"{row.get('baseline_accuracy', 0):.4f}")
#             m4.metric("F1", f"{row.get('f1', 0):.4f}")
#             m5.metric("Improve", f"{row.get('improvement_over_baseline', 0):+.4f}")

#             st.dataframe(eval_df, width="stretch")

#         if not fi_df.empty:
#             fig = go.Figure()
#             top = fi_df.sort_values("importance", ascending=False).head(15)

#             fig.add_trace(
#                 go.Bar(
#                     x=top["importance"],
#                     y=top["feature"],
#                     orientation="h",
#                     name="Feature Importance",
#                 )
#             )

#             fig.update_layout(
#                 title="Top Feature Importance",
#                 xaxis_title="Importance",
#                 yaxis_title="Feature",
#                 yaxis={"categoryorder": "total ascending"},
#             )

#             st.plotly_chart(fig, width="stretch")

#     with tab_table:
#         display_cols = [
#             "market_date",
#             "symbol",
#             "stock_close",
#             "oil_price",
#             "gold_price",
#             "stock_return_pct",
#             "oil_return_pct",
#             "gold_return_pct",
#             "rolling_corr",
#             "stock_gold_corr",
#             "prediction_label",
#             "probability_high_risk",
#             "alert_level",
#             "business_signal",
#             "processing_time",
#         ]

#         existing_cols = [col for col in display_cols if col in df_visible.columns]

#         st.dataframe(
#             df_visible[existing_cols].sort_values("market_date", ascending=False),
#             width="stretch",
#         )


# live_app()
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


PREDICTIONS_PATH = os.getenv("PREDICTIONS_PATH", "output/parquet/market_predictions")
EVAL_PATH = os.getenv("ML_EVAL_PATH", "output/ml_evaluation.csv")
FEATURE_IMPORTANCE_PATH = os.getenv(
    "ML_FEATURE_IMPORTANCE_PATH",
    "output/ml_feature_importance.csv",
)

REFRESH_EVERY = os.getenv("STREAMLIT_REFRESH_EVERY", "1s")
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "TSLA")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "90"))
TAIL_ROWS = int(os.getenv("TAIL_ROWS", "120"))


st.set_page_config(
    page_title="Spark ML Current Market Risk Dashboard",
    page_icon="🚨",
    layout="wide",
)


st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
    }

    div[data-testid="stMetric"] {
        background: #0f172a;
        border: 1px solid #334155;
        padding: 1rem;
        border-radius: 1rem;
        box-shadow: 0 8px 20px rgba(15, 23, 42, 0.18);
    }

    div[data-testid="stMetric"] label {
        color: #cbd5e1 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_predictions() -> pd.DataFrame:
    """
    Load prediction parquet generated by Spark Streaming.

    Expected path:
        output/parquet/market_predictions
    """

    path = Path(PREDICTIONS_PATH)

    if not path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_parquet(path)
    except Exception:
        # Spark may be overwriting parquet while Streamlit reads it.
        return pd.DataFrame()

    if df.empty:
        return df

    if "market_date" in df.columns:
        df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce")

    for col in ["kafka_timestamp", "processing_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Compatibility with older prediction output
    if "probability_high_risk" not in df.columns and "probability_up" in df.columns:
        df["probability_high_risk"] = df["probability_up"]

    if "prediction_label" in df.columns:
        df["prediction_label"] = (
            df["prediction_label"]
            .astype(str)
            .replace({"UP": "HIGH RISK", "DOWN": "NORMAL"})
        )

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

    df = df.dropna(subset=["market_date", "symbol"])
    df["symbol"] = df["symbol"].astype(str).str.strip()

    df = df.sort_values(["symbol", "market_date"]).reset_index(drop=True)

    return df


def load_available_symbols() -> list[str]:
    """
    Load available symbols from prediction parquet.

    This function is called outside @st.fragment so that the sidebar selectbox
    does not violate Streamlit fragment widget rules.
    """

    df = load_predictions()

    if df.empty or "symbol" not in df.columns:
        return []

    return sorted(df["symbol"].dropna().astype(str).unique().tolist())


def filter_recent(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    latest_date = df["market_date"].max()
    start_date = latest_date - pd.Timedelta(days=LOOKBACK_DAYS)

    df = df[df["market_date"] >= start_date].copy()

    if TAIL_ROWS > 0:
        df = df.tail(TAIL_ROWS)

    return df.sort_values("market_date").reset_index(drop=True)


def load_eval() -> pd.DataFrame:
    path = Path(EVAL_PATH)

    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def load_feature_importance() -> pd.DataFrame:
    path = Path(FEATURE_IMPORTANCE_PATH)

    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def build_price_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["market_date"],
            y=df["stock_close"],
            mode="lines+markers",
            name=f"{symbol} Close",
        )
    )

    if "stock_ma" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["market_date"],
                y=df["stock_ma"],
                mode="lines",
                name="Stock MA",
            )
        )

    fig.update_layout(
        title=f"{symbol} Price Streaming",
        xaxis_title="Date",
        yaxis_title="Stock Price",
        hovermode="x unified",
    )

    return fig


def build_probability_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

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


def build_corr_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    if "rolling_corr" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["market_date"],
                y=df["rolling_corr"],
                mode="lines+markers",
                name="Stock-Oil Corr",
            )
        )

    if "stock_gold_corr" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["market_date"],
                y=df["stock_gold_corr"],
                mode="lines+markers",
                name="Stock-Gold Corr",
            )
        )

    fig.update_layout(
        title="Rolling Correlation",
        xaxis_title="Date",
        yaxis_title="Correlation",
        yaxis_range=[-1, 1],
        hovermode="x unified",
    )

    return fig


def build_return_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    if "stock_return_pct" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["market_date"],
                y=df["stock_return_pct"],
                mode="lines",
                name="Stock Return %",
            )
        )

    if "oil_return_pct" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["market_date"],
                y=df["oil_return_pct"],
                mode="lines",
                name="Oil Return %",
            )
        )

    if "gold_return_pct" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["market_date"],
                y=df["gold_return_pct"],
                mode="lines",
                name="Gold Return %",
            )
        )

    fig.update_layout(
        title="Daily Return Comparison",
        xaxis_title="Date",
        yaxis_title="Return %",
        hovermode="x unified",
    )

    return fig


def prediction_badge(label: str, prob_high_risk: float) -> None:
    if label == "HIGH RISK":
        st.error(
            f"🚨 Current Market State: HIGH RISK | "
            f"Risk Probability: {prob_high_risk:.2%}"
        )
    else:
        st.success(
            f"✅ Current Market State: NORMAL | "
            f"Risk Probability: {prob_high_risk:.2%}"
        )


st.title("🚨 Spark ML Current Market Risk Dashboard")
st.caption(
    "Kafka → Spark Structured Streaming → Spark MLlib → "
    "Current Market Risk Classification"
)


# ==========================================================
# Sidebar
# IMPORTANT:
# Do not put sidebar widgets inside @st.fragment.
# ==========================================================

with st.sidebar:
    st.header("Settings")
    st.write(f"**Predictions path:** `{PREDICTIONS_PATH}`")
    st.write(f"**Evaluation path:** `{EVAL_PATH}`")
    st.write(f"**Refresh:** `{REFRESH_EVERY}`")
    st.write(f"**Lookback days:** `{LOOKBACK_DAYS}`")
    st.write(f"**Tail rows:** `{TAIL_ROWS}`")

    st.divider()
    st.subheader("Symbol")

    available_symbols = load_available_symbols()

    if not available_symbols:
        available_symbols = [DEFAULT_SYMBOL]
        st.info(
            "Chưa có prediction data. Dropdown sẽ cập nhật sau khi Spark ghi dữ liệu."
        )

    current_symbol = st.session_state.get("selected_symbol", DEFAULT_SYMBOL)

    if current_symbol in available_symbols:
        default_index = available_symbols.index(current_symbol)
    elif DEFAULT_SYMBOL in available_symbols:
        default_index = available_symbols.index(DEFAULT_SYMBOL)
    else:
        default_index = 0

    selected_symbol = st.selectbox(
        "Chọn symbol cần theo dõi",
        options=available_symbols,
        index=default_index,
        key="selected_symbol_dropdown",
    )

    st.session_state.selected_symbol = selected_symbol

    if st.button("Làm mới danh sách symbol"):
        st.rerun()


@st.fragment(run_every=REFRESH_EVERY)
def live_app() -> None:
    df = load_predictions()

    if df.empty:
        st.warning(
            "Chưa có prediction. Hãy chạy Spark streaming predictor và Kafka producer."
        )
        return

    symbols = sorted(df["symbol"].dropna().unique().tolist())
    selected_symbol = st.session_state.get("selected_symbol", DEFAULT_SYMBOL)

    df_symbol = df[df["symbol"] == selected_symbol].copy()

    if df_symbol.empty:
        st.warning(
            f"Chưa có dữ liệu streaming cho symbol `{selected_symbol}`. "
            f"Symbols hiện có trong prediction: {symbols}"
        )
        return

    df_visible = filter_recent(df_symbol)

    if df_visible.empty:
        st.warning(f"No recent data for {selected_symbol}")
        return

    latest = df_visible.iloc[-1]

    st.caption(
        f"Showing {len(df_visible)} rows from "
        f"{df_visible['market_date'].min().date()} to "
        f"{df_visible['market_date'].max().date()}"
    )

    k1, k2, k3, k4, k5 = st.columns(5)

    k1.metric(
        f"{selected_symbol} Close",
        f"{latest['stock_close']:.2f}",
    )

    k2.metric(
        "Oil",
        f"{latest['oil_price']:.2f}",
    )

    if "gold_price" in latest.index:
        k3.metric(
            "Gold",
            f"{latest['gold_price']:.2f}",
        )
    else:
        k3.metric("Gold", "N/A")

    k4.metric(
        "Risk Prob.",
        f"{latest['probability_high_risk']:.2%}",
    )

    k5.metric(
        "Alert",
        str(latest.get("alert_level", "LOW")),
    )

    prediction_badge(
        str(latest.get("prediction_label", "NORMAL")),
        float(latest.get("probability_high_risk", 0.0)),
    )

    tab_live, tab_model, tab_table = st.tabs(
        ["📡 Live Risk Classification", "🧠 Model Evaluation", "📋 Latest Data"]
    )

    with tab_live:
        left, right = st.columns([2, 1])

        with left:
            st.plotly_chart(
                build_price_chart(df_visible, selected_symbol),
                width="stretch",
            )

        with right:
            st.plotly_chart(
                build_probability_chart(df_visible),
                width="stretch",
            )

        st.plotly_chart(
            build_corr_chart(df_visible),
            width="stretch",
        )

        st.plotly_chart(
            build_return_chart(df_visible),
            width="stretch",
        )

        signal = latest.get("business_signal", "")

        if isinstance(signal, str) and signal.strip():
            st.info(f"Business signal: {signal}")

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

            st.plotly_chart(fig, width="stretch")

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
            "prediction_label",
            "probability_high_risk",
            "alert_level",
            "business_signal",
            "processing_time",
        ]

        existing_cols = [col for col in display_cols if col in df_visible.columns]

        st.dataframe(
            df_visible[existing_cols].sort_values("market_date", ascending=False),
            width="stretch",
        )


live_app()