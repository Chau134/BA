from __future__ import annotations

import json
import os
from pathlib import Path

import pyspark
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


APP_NAME = os.getenv("SPARK_APP_NAME", "Current-Market-Risk-Streaming-Predictor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_data")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "2 seconds")
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "10"))

BRONZE_PATH = os.getenv("BRONZE_PATH", "output/parquet/bronze_market_data")
PREDICTIONS_PATH = os.getenv("PREDICTIONS_PATH", "output/parquet/market_predictions")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoints/spark_market_predictions")

MODEL_PATH = os.getenv("MODEL_PATH", "models/stock_direction_rf")
MODEL_META_PATH = os.getenv("MODEL_META_PATH", "models/stock_direction_rf_meta.json")

STREAM_SYMBOL_FILTER = os.getenv("STREAM_SYMBOL_FILTER", "").strip()

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_KAFKA_PACKAGE = os.getenv("SPARK_KAFKA_PACKAGE", "")
SPARK_WINDOWS_FS_PACKAGE = os.getenv(
    "SPARK_WINDOWS_FS_PACKAGE",
    "com.globalmentor:hadoop-bare-naked-local-fs:0.1.0",
)


INPUT_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("stock_close", DoubleType(), True),
        StructField("oil_price", DoubleType(), True),
        StructField("gold_price", DoubleType(), True),
    ]
)


DEFAULT_FEATURE_COLUMNS = [
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
]


def load_model_metadata() -> dict:
    path = Path(MODEL_META_PATH)

    if not path.exists():
        return {
            "feature_columns": DEFAULT_FEATURE_COLUMNS,
            "positive_class": "HIGH RISK",
            "negative_class": "NORMAL",
            "prediction_task": "current_market_risk_classification",
        }

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


MODEL_META = load_model_metadata()
FEATURE_COLUMNS = MODEL_META.get("feature_columns", DEFAULT_FEATURE_COLUMNS)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )

    kafka_package = SPARK_KAFKA_PACKAGE.strip()

    if not kafka_package:
        spark_version = pyspark.__version__.split("+")[0]
        kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"

    packages = [kafka_package]
    if os.name == "nt":
        packages.append(SPARK_WINDOWS_FS_PACKAGE)
        builder = builder.config(
                "spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.checkpointing."
                "FileSystemBasedCheckpointFileManager",
            )

    spark = builder.config("spark.jars.packages", ",".join(packages)).getOrCreate()
    if os.name == "nt":
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set(
            "fs.file.impl",
            "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
        )
        hadoop_conf.set("fs.file.impl.disable.cache", "true")
        hadoop_conf.set("fs.file.checksum.verify", "false")
    spark.sparkContext.setLogLevel("WARN")

    return spark


def ensure_paths() -> None:
    for path in [BRONZE_PATH, PREDICTIONS_PATH, CHECKPOINT_PATH]:
        Path(path).mkdir(parents=True, exist_ok=True)

    if not Path(MODEL_PATH).exists():
        raise FileNotFoundError(
            f"Missing model: {MODEL_PATH}. "
            f"Run: python src/spark_ml_train.py"
        )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), INPUT_SCHEMA).alias("data"),
        )
        .select("kafka_timestamp", "data.*")
        .withColumn("market_date", F.to_date("date", "yyyy-MM-dd"))
        .withColumn("symbol", F.trim(F.col("symbol")))
        .withColumn("stock_close", F.col("stock_close").cast("double"))
        .withColumn("oil_price", F.col("oil_price").cast("double"))
        .withColumn(
            "gold_price",
            F.coalesce(F.col("gold_price").cast("double"), F.lit(0.0)),
        )
        .drop("date")
        .filter(
            F.col("market_date").isNotNull()
            & F.col("symbol").isNotNull()
            & (F.length(F.col("symbol")) > 0)
            & F.col("stock_close").isNotNull()
            & F.col("oil_price").isNotNull()
        )
        .select(
            "market_date",
            "symbol",
            "stock_close",
            "oil_price",
            "gold_price",
            "kafka_timestamp",
        )
    )

    if STREAM_SYMBOL_FILTER:
        symbols = [s.strip() for s in STREAM_SYMBOL_FILTER.split(",") if s.strip()]
        parsed = parsed.filter(F.col("symbol").isin(*symbols))
        print(f"Streaming symbol filter: {symbols}")

    return parsed


def safe_ratio(numerator_col: str, denominator_col: str):
    return F.when(
        F.col(denominator_col).isNull() | (F.col(denominator_col) == 0),
        F.lit(0.0),
    ).otherwise(F.col(numerator_col) / F.col(denominator_col))


def compute_features(history_df: DataFrame) -> DataFrame:
    ordered = history_df.dropDuplicates(["symbol", "market_date"]).orderBy(
        "symbol",
        "market_date",
    )

    lag_window = Window.partitionBy("symbol").orderBy("market_date")

    rolling_window = (
        Window.partitionBy("symbol")
        .orderBy("market_date")
        .rowsBetween(-(ROLLING_WINDOW - 1), 0)
    )

    df = (
        ordered.withColumn("stock_prev", F.lag("stock_close").over(lag_window))
        .withColumn("oil_prev", F.lag("oil_price").over(lag_window))
        .withColumn("gold_prev", F.lag("gold_price").over(lag_window))
        .withColumn(
            "stock_return_pct",
            F.when(F.col("stock_prev").isNull(), None).otherwise(
                ((F.col("stock_close") - F.col("stock_prev")) / F.col("stock_prev")) * 100
            ),
        )
        .withColumn(
            "oil_return_pct",
            F.when(F.col("oil_prev").isNull(), None).otherwise(
                ((F.col("oil_price") - F.col("oil_prev")) / F.col("oil_prev")) * 100
            ),
        )
        .withColumn(
            "gold_return_pct",
            F.when(
                F.col("gold_prev").isNull() | (F.col("gold_prev") == 0),
                F.lit(0.0),
            ).otherwise(
                ((F.col("gold_price") - F.col("gold_prev")) / F.col("gold_prev")) * 100
            ),
        )
        .withColumn("stock_return_lag_1", F.lag("stock_return_pct", 1).over(lag_window))
        .withColumn("stock_return_lag_3", F.lag("stock_return_pct", 3).over(lag_window))
        .withColumn("stock_return_lag_5", F.lag("stock_return_pct", 5).over(lag_window))
        .withColumn("oil_return_lag_1", F.lag("oil_return_pct", 1).over(lag_window))
        .withColumn("gold_return_lag_1", F.lag("gold_return_pct", 1).over(lag_window))
        .withColumn("stock_ma", F.avg("stock_close").over(rolling_window))
        .withColumn("oil_ma", F.avg("oil_price").over(rolling_window))
        .withColumn("gold_ma", F.avg("gold_price").over(rolling_window))
        .withColumn("stock_volatility", F.stddev_samp("stock_return_pct").over(rolling_window))
        .withColumn("oil_volatility", F.stddev_samp("oil_return_pct").over(rolling_window))
        .withColumn("gold_volatility", F.stddev_samp("gold_return_pct").over(rolling_window))
        .withColumn("rolling_corr", F.corr("stock_close", "oil_price").over(rolling_window))
        .withColumn("stock_gold_corr", F.corr("stock_close", "gold_price").over(rolling_window))
        .withColumn("stock_ma_ratio", safe_ratio("stock_close", "stock_ma"))
        .withColumn("oil_ma_ratio", safe_ratio("oil_price", "oil_ma"))
        .withColumn("gold_ma_ratio", safe_ratio("gold_price", "gold_ma"))
    )

    for col in FEATURE_COLUMNS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(0.0))

        df = df.withColumn(
            col,
            F.when(F.isnan(F.col(col)) | F.col(col).isNull(), F.lit(0.0))
            .otherwise(F.col(col).cast("double")),
        )

    df = (
        df.withColumn(
            "alert_level",
            F.when(
                (
                    (F.abs(F.coalesce(F.col("oil_return_pct"), F.lit(0.0))) >= 3)
                    | (F.abs(F.coalesce(F.col("gold_return_pct"), F.lit(0.0))) >= 3)
                )
                & (
                    (F.abs(F.coalesce(F.col("rolling_corr"), F.lit(0.0))) >= 0.6)
                    | (F.abs(F.coalesce(F.col("stock_gold_corr"), F.lit(0.0))) >= 0.6)
                ),
                F.lit("HIGH"),
            )
            .when(
                (F.abs(F.coalesce(F.col("oil_return_pct"), F.lit(0.0))) >= 1.5)
                | (F.abs(F.coalesce(F.col("gold_return_pct"), F.lit(0.0))) >= 1.5)
                | (F.abs(F.coalesce(F.col("rolling_corr"), F.lit(0.0))) >= 0.4)
                | (F.abs(F.coalesce(F.col("stock_gold_corr"), F.lit(0.0))) >= 0.4),
                F.lit("MEDIUM"),
            )
            .otherwise(F.lit("LOW")),
        )
        .withColumn(
            "business_signal",
            F.when(
                F.col("alert_level") == "HIGH",
                F.lit("High current market risk detected. Review short-term exposure."),
            )
            .when(
                F.abs(F.coalesce(F.col("rolling_corr"), F.lit(0.0))) < 0.3,
                F.lit("Weak commodity-stock relationship. Avoid commodity-only decisions."),
            )
            .otherwise(F.lit("Monitor trend continuity and ML market risk classification.")),
        )
        .withColumn("processing_time", F.current_timestamp())
    )

    return df


def upsert_and_predict(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.limit(1).count() == 0:
        print(f"Batch {batch_id}: empty batch, skipped.")
        return

    print(f"Batch {batch_id}: writing incoming rows to bronze.")

    (
        batch_df.dropDuplicates(["symbol", "market_date"])
        .write.mode("append")
        .parquet(BRONZE_PATH)
    )

    spark = batch_df.sparkSession

    history = spark.read.parquet(BRONZE_PATH)
    features = compute_features(history)

    model = PipelineModel.load(MODEL_PATH)
    predictions = model.transform(features)

    predictions = (
        predictions.withColumn(
            "probability_high_risk",
            vector_to_array("probability")[1],
        )
        .withColumn(
            "prediction_label",
            F.when(F.col("prediction") == 1.0, F.lit("HIGH RISK"))
            .otherwise(F.lit("NORMAL")),
        )
        .select(
            "market_date",
            "symbol",
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
            "prediction_label",
            "probability_high_risk",
            "alert_level",
            "business_signal",
            "kafka_timestamp",
            "processing_time",
        )
        .orderBy("symbol", "market_date")
    )

    predictions.write.mode("overwrite").parquet(PREDICTIONS_PATH)

    print(f"Batch {batch_id}: predictions refreshed at {PREDICTIONS_PATH}")


def main() -> None:
    ensure_paths()

    spark = build_spark()
    stream_df = read_kafka_stream(spark)

    print("=" * 80)
    print("Spark Streaming Current Market Risk Predictor is initializing...")
    print(f"- Kafka: {KAFKA_BOOTSTRAP}")
    print(f"- Topic: {KAFKA_TOPIC}")
    print(f"- Starting offsets: {STARTING_OFFSETS}")
    print(f"- Model: {MODEL_PATH}")
    print(f"- Prediction task: Current market risk classification: HIGH RISK / NORMAL")
    print(f"- Streaming symbol filter: {STREAM_SYMBOL_FILTER or 'ALL'}")
    print(f"- Bronze path: {BRONZE_PATH}")
    print(f"- Predictions path: {PREDICTIONS_PATH}")
    print(f"- Checkpoint: {CHECKPOINT_PATH}")
    print(f"- Rolling window: {ROLLING_WINDOW}")
    print(f"- Feature columns: {FEATURE_COLUMNS}")
    print("=" * 80)

    stream_df.printSchema()

    query = (
        stream_df.writeStream.foreachBatch(upsert_and_predict)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("Spark streaming query is active.", flush=True)

    query.awaitTermination()


if __name__ == "__main__":
    main()
