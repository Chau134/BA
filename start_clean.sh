#!/bin/bash

set -e

# =========================
# Environment
# =========================

export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# =========================
# Presentation streaming config
# =========================

DEMO_SYMBOLS=${DEMO_SYMBOLS:-"AAPL"}
DEMO_START_DATE=${DEMO_START_DATE:-"2024-01-01"}
DEMO_MAX_ROWS=${DEMO_MAX_ROWS:-120}

STREAM_SLEEP_SECONDS=${STREAM_SLEEP_SECONDS:-1}
STREAMLIT_REFRESH_EVERY=${STREAMLIT_REFRESH_EVERY:-"1s"}
SPARK_TRIGGER_INTERVAL=${SPARK_TRIGGER_INTERVAL:-"2 seconds"}

LOOKBACK_DAYS=${LOOKBACK_DAYS:-90}
TAIL_ROWS=${TAIL_ROWS:-120}

KAFKA_TOPIC=${KAFKA_TOPIC:-"market_data"}
KAFKA_STARTING_OFFSETS=${KAFKA_STARTING_OFFSETS:-"latest"}

FULL_CSV="output/market_data_final.csv"
DEMO_CSV="output/market_data_demo.csv"

mkdir -p output
mkdir -p kafka-runtime-logs

echo "============================================================"
echo "Big Data BI Streaming Demo"
echo "============================================================"
echo "Python:"
which python
python --version
echo ""
echo "PYSPARK_PYTHON=$PYSPARK_PYTHON"
echo "PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON"
echo "JAVA_HOME=$JAVA_HOME"
echo "Kafka starting offsets: $KAFKA_STARTING_OFFSETS"
echo "============================================================"

# =========================
# Check project root
# =========================

if [ ! -f "etl_pipeline.py" ]; then
  echo "Error: Please run this script from the project root directory."
  exit 1
fi

if [ ! -f "kraft.properties" ]; then
  echo "Error: Missing kraft.properties."
  exit 1
fi

# =========================
# Stop old local app jobs
# =========================

echo "Stopping old Spark / Streamlit / Producer processes if any..."
pkill -f "streamlit run app.py" || true
pkill -f "src/spark_processor.py" || true
pkill -f "src/producer_kafka.py" || true

sleep 2

# =========================
# Start Kafka if needed
# =========================

echo "Checking Kafka on localhost:9092..."

if lsof -i :9092 > /dev/null; then
  echo "Kafka is already running on localhost:9092."
  KAFKA_PID=""
else
  echo "Kafka is not running. Starting Kafka..."

  KAFKA_LOG_DIR=$(grep "^log.dirs=" kraft.properties | cut -d= -f2-)

  if [ -z "$KAFKA_LOG_DIR" ]; then
    KAFKA_LOG_DIR="./kafka-logs"
  fi

  mkdir -p "$KAFKA_LOG_DIR"

  if [ ! -f "$KAFKA_LOG_DIR/meta.properties" ]; then
    echo "Formatting Kafka storage at $KAFKA_LOG_DIR..."
    kafka-storage.sh format \
      -t $(kafka-storage.sh random-uuid) \
      -c kraft.properties
  else
    echo "Kafka storage already formatted."
  fi

  kafka-server-start.sh kraft.properties > kafka-runtime-logs/kafka.log 2>&1 &
  KAFKA_PID=$!

  echo "Kafka PID: $KAFKA_PID"
  echo "Waiting for Kafka to start..."
  sleep 8

  if ! lsof -i :9092 > /dev/null; then
    echo "Error: Kafka failed to start. Check kafka-runtime-logs/kafka.log"
    exit 1
  fi

  echo "Kafka started successfully."
fi

# =========================
# Clean old streaming outputs
# =========================

echo "Cleaning old Spark streaming outputs..."
rm -rf output/parquet/bronze_market_data
rm -rf output/parquet/market_metrics
rm -rf checkpoints/spark_market_metrics

# =========================
# Run ETL
# =========================

echo "Running ETL..."
python etl_pipeline.py

if [ ! -f "$FULL_CSV" ]; then
  echo "Error: ETL did not create $FULL_CSV"
  exit 1
fi

# =========================
# Create demo CSV from latest dates
# =========================

echo "Creating demo CSV from latest records..."
echo "Symbols: $DEMO_SYMBOLS"
echo "Start date: $DEMO_START_DATE"
echo "Max rows: $DEMO_MAX_ROWS"
echo "Producer speed: $STREAM_SLEEP_SECONDS second(s)/row"

python - <<PY
import pandas as pd

full_csv = "$FULL_CSV"
demo_csv = "$DEMO_CSV"

symbols = [s.strip() for s in "$DEMO_SYMBOLS".split(",") if s.strip()]
start_date = "$DEMO_START_DATE"
max_rows = int("$DEMO_MAX_ROWS")

df = pd.read_csv(full_csv)

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])

if symbols:
    df = df[df["symbol"].isin(symbols)]

if start_date:
    df = df[df["date"] >= pd.to_datetime(start_date)]

df = df.sort_values(["date", "symbol"]).reset_index(drop=True)

# Lấy đoạn mới nhất để demo,
# nhưng producer vẫn stream từ đầu đoạn đó đến cuối đoạn đó.
if max_rows > 0:
    df = df.tail(max_rows)

df = df.sort_values(["date", "symbol"]).reset_index(drop=True)

df["date"] = df["date"].dt.strftime("%Y-%m-%d")
df.to_csv(demo_csv, index=False)

print(f"Demo CSV saved to: {demo_csv}")
print(f"Rows: {len(df)}")
print(f"Symbols: {sorted(df['symbol'].unique().tolist()) if len(df) else []}")

if len(df) > 0:
    print(f"Date range: {df['date'].min()} -> {df['date'].max()}")

if len(df) == 0:
    raise SystemExit("Demo CSV is empty. Change DEMO_SYMBOLS or DEMO_START_DATE.")
PY

# =========================
# Start Spark
# =========================

echo "Starting Spark processor..."
echo "Spark will read Kafka topic from: $KAFKA_STARTING_OFFSETS"

KAFKA_TOPIC="$KAFKA_TOPIC" \
KAFKA_STARTING_OFFSETS="$KAFKA_STARTING_OFFSETS" \
SPARK_TRIGGER_INTERVAL="$SPARK_TRIGGER_INTERVAL" \
python src/spark_processor.py &
SPARK_PID=$!

echo "Spark PID: $SPARK_PID"
sleep 10

# =========================
# Start Streamlit
# =========================

echo "Starting Streamlit dashboard in streaming-only mode..."

DASHBOARD_MODE="streaming" \
STREAMLIT_REFRESH_EVERY="$STREAMLIT_REFRESH_EVERY" \
LOOKBACK_DAYS="$LOOKBACK_DAYS" \
TAIL_ROWS="$TAIL_ROWS" \
streamlit run app.py &
STREAMLIT_PID=$!

echo "Streamlit PID: $STREAMLIT_PID"
sleep 8

# =========================
# Start Producer
# =========================

echo "Starting Kafka producer..."
echo "Producer will stream demo CSV only."

KAFKA_TOPIC="$KAFKA_TOPIC" \
MARKET_CSV_PATH="$DEMO_CSV" \
STREAM_SLEEP_SECONDS="$STREAM_SLEEP_SECONDS" \
python src/producer_kafka.py

echo "============================================================"
echo "Producer finished."
echo "============================================================"
echo "Kafka PID: ${KAFKA_PID:-already running}"
echo "Spark PID: $SPARK_PID"
echo "Streamlit PID: $STREAMLIT_PID"
echo ""
echo "Dashboard:"
echo "http://localhost:8501"
echo ""
echo "To stop Spark and Streamlit:"
echo "kill $SPARK_PID $STREAMLIT_PID"
if [ -n "$KAFKA_PID" ]; then
  echo ""
  echo "To stop Kafka started by this script:"
  echo "kill $KAFKA_PID"
fi
echo "============================================================"