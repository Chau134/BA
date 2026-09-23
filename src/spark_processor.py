from __future__ import annotations

import os
from pathlib import Path

import pyspark
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


APP_NAME = os.getenv("SPARK_APP_NAME", "Stock-Commodity-Streaming-Processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_data")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "10"))

BRONZE_PATH = os.getenv("BRONZE_PATH", "output/parquet/bronze_market_data")
METRICS_PATH = os.getenv("METRICS_PATH", "output/parquet/market_metrics")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoints/spark_market_metrics")

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


def ensure_output_dirs() -> None:
    for path in [BRONZE_PATH, METRICS_PATH, CHECKPOINT_PATH]:
        Path(path).mkdir(parents=True, exist_ok=True)


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
        .withColumn("gold_price", F.col("gold_price").cast("double"))
        .drop("date")
        .filter(
            F.col("market_date").isNotNull()
            & F.col("symbol").isNotNull()
            & (F.length(F.col("symbol")) > 0)
            & F.col("stock_close").isNotNull()
            & F.col("oil_price").isNotNull()
            & F.col("gold_price").isNotNull()
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

    return parsed


def compute_metrics(history_df: DataFrame) -> DataFrame:
    ordered = history_df.orderBy("symbol", "market_date")

    rolling_window = (
        Window.partitionBy("symbol")
        .orderBy("market_date")
        .rowsBetween(-(ROLLING_WINDOW - 1), 0)
    )

    lag_window = Window.partitionBy("symbol").orderBy("market_date")

    metrics = (
        ordered.withColumn("stock_prev", F.lag("stock_close").over(lag_window))
        .withColumn("oil_prev", F.lag("oil_price").over(lag_window))
        .withColumn("gold_prev", F.lag("gold_price").over(lag_window))
        .withColumn("stock_ma", F.round(F.avg("stock_close").over(rolling_window), 4))
        .withColumn("oil_ma", F.round(F.avg("oil_price").over(rolling_window), 4))
        .withColumn("gold_ma", F.round(F.avg("gold_price").over(rolling_window), 4))
        .withColumn(
            "stock_return_pct",
            F.when(F.col("stock_prev").isNull(), None).otherwise(
                F.round(
                    ((F.col("stock_close") - F.col("stock_prev")) / F.col("stock_prev"))
                    * 100,
                    4,
                )
            ),
        )
        .withColumn(
            "oil_return_pct",
            F.when(F.col("oil_prev").isNull(), None).otherwise(
                F.round(
                    ((F.col("oil_price") - F.col("oil_prev")) / F.col("oil_prev"))
                    * 100,
                    4,
                )
            ),
        )
        .withColumn(
            "gold_return_pct",
            F.when(F.col("gold_prev").isNull(), None).otherwise(
                F.round(
                    ((F.col("gold_price") - F.col("gold_prev")) / F.col("gold_prev"))
                    * 100,
                    4,
                )
            ),
        )
        .withColumn(
            "stock_volatility",
            F.round(F.stddev_samp("stock_close").over(rolling_window), 4),
        )
        .withColumn(
            "oil_volatility",
            F.round(F.stddev_samp("oil_price").over(rolling_window), 4),
        )
        .withColumn(
            "gold_volatility",
            F.round(F.stddev_samp("gold_price").over(rolling_window), 4),
        )
        .withColumn(
            "rolling_corr",
            F.round(F.corr("stock_close", "oil_price").over(rolling_window), 4),
        )
        .withColumn(
            "stock_gold_corr",
            F.round(F.corr("stock_close", "gold_price").over(rolling_window), 4),
        )
        .withColumn(
            "alert_level",
            F.when(
                (
                    (F.abs(F.coalesce(F.col("oil_return_pct"), F.lit(0.0))) >= 3)
                    | (F.abs(F.coalesce(F.col("gold_return_pct"), F.lit(0.0))) >= 3)
                )
                & (
                    (F.abs(F.coalesce(F.col("rolling_corr"), F.lit(0.0))) >= 0.6)
                    | (
                        F.abs(F.coalesce(F.col("stock_gold_corr"), F.lit(0.0)))
                        >= 0.6
                    )
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
                (F.col("alert_level") == "HIGH")
                & (
                    (F.coalesce(F.col("rolling_corr"), F.lit(0.0)) > 0)
                    | (F.coalesce(F.col("stock_gold_corr"), F.lit(0.0)) > 0)
                )
                & (F.coalesce(F.col("stock_return_pct"), F.lit(0.0)) < 0),
                F.lit(
                    "Commodity shock may pressure this stock. Review short-term risk exposure."
                ),
            )
            .when(
                F.coalesce(F.col("rolling_corr"), F.lit(0.0)) <= -0.5,
                F.lit(
                    "Stock is diverging from oil. Check stock-specific catalysts or market news."
                ),
            )
            .when(
                F.coalesce(F.col("stock_gold_corr"), F.lit(0.0)) <= -0.5,
                F.lit(
                    "Stock is diverging from gold. Check risk sentiment and defensive asset movement."
                ),
            )
            .when(
                (F.abs(F.coalesce(F.col("rolling_corr"), F.lit(0.0))) < 0.3)
                & (F.abs(F.coalesce(F.col("stock_gold_corr"), F.lit(0.0))) < 0.3),
                F.lit(
                    "Weak commodity relationship. Avoid decisions based only on oil or gold movements."
                ),
            )
            .otherwise(F.lit("Monitor trend continuity across stock, oil and gold.")),
        )
        .withColumn("processing_time", F.current_timestamp())
        .select(
            "market_date",
            "symbol",
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
            "alert_level",
            "business_signal",
            "kafka_timestamp",
            "processing_time",
        )
    )

    return metrics


def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty batch, skipped.")
        return

    print(f"Batch {batch_id}: writing incoming rows to bronze.")

    (
        batch_df.dropDuplicates(["symbol", "market_date"])
        .write.mode("append")
        .parquet(BRONZE_PATH)
    )

    spark = batch_df.sparkSession

    history = (
        spark.read.parquet(BRONZE_PATH)
        .dropDuplicates(["symbol", "market_date"])
        .orderBy("symbol", "market_date")
    )

    metrics = compute_metrics(history)

    metrics.write.mode("overwrite").parquet(METRICS_PATH)

    print(f"Batch {batch_id}: metrics refreshed at {METRICS_PATH}")


def main() -> None:
    ensure_output_dirs()

    spark = build_spark()
    stream_df = read_kafka_stream(spark)

    print("Streaming schema:")
    stream_df.printSchema()

    query = (
        stream_df.writeStream.foreachBatch(upsert_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("=" * 80)
    print("Spark Structured Streaming is initializing...")
    print(f"- Kafka: {KAFKA_BOOTSTRAP}")
    print(f"- Topic: {KAFKA_TOPIC}")
    print(f"- Bronze path: {BRONZE_PATH}")
    print(f"- Metrics path: {METRICS_PATH}")
    print(f"- Checkpoint: {CHECKPOINT_PATH}")
    print(f"- Rolling window: {ROLLING_WINDOW} rows")
    print("=" * 80)

    print("Spark streaming query is active.", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
