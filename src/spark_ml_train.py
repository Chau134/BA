from __future__ import annotations

import csv
import json
import os
import shutil
from pathlib import Path

import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


APP_NAME = os.getenv("SPARK_ML_APP_NAME", "Current-Market-Risk-Spark-ML-Training")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_WINDOWS_FS_PACKAGE = os.getenv(
    "SPARK_WINDOWS_FS_PACKAGE",
    "com.globalmentor:hadoop-bare-naked-local-fs:0.1.0",
)

INPUT_PATH = os.getenv("ML_INPUT_PATH", "output/market_analysis_results.csv")

MODEL_PATH = os.getenv("MODEL_PATH", "models/stock_direction_rf")
MODEL_META_PATH = os.getenv("MODEL_META_PATH", "models/stock_direction_rf_meta.json")

EVAL_PATH = os.getenv("ML_EVAL_PATH", "output/ml_evaluation.csv")
FEATURE_IMPORTANCE_PATH = os.getenv(
    "ML_FEATURE_IMPORTANCE_PATH",
    "output/ml_feature_importance.csv",
)
PRED_SAMPLE_PATH = os.getenv(
    "ML_PRED_SAMPLE_PATH",
    "output/ml_predictions_sample.csv",
)

TRAIN_RATIO = float(os.getenv("ML_TRAIN_RATIO", "0.8"))
SEED = int(os.getenv("ML_SEED", "42"))

ML_SYMBOL_FILTER = os.getenv("ML_SYMBOL_FILTER", "").strip()

LABEL_COL = os.getenv("ML_LABEL_COL", "target_market_risk")

USE_CLASS_WEIGHT = os.getenv("USE_CLASS_WEIGHT", "false").lower() == "true"

FEATURE_COLUMNS = [
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


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if os.name == "nt":
        builder = builder.config("spark.jars.packages", SPARK_WINDOWS_FS_PACKAGE)

    spark = builder.getOrCreate()
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


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def parse_symbol_filter() -> list[str]:
    if not ML_SYMBOL_FILTER:
        return []

    return [s.strip() for s in ML_SYMBOL_FILTER.split(",") if s.strip()]


def safe_quantile(df, col: str, q: float, default: float = 0.0) -> float:
    try:
        result = df.approxQuantile(col, [q], 0.01)
        if result and result[0] is not None:
            return float(result[0])
    except Exception:
        pass

    return default


def load_training_data(spark: SparkSession):
    if not Path(INPUT_PATH).exists():
        raise FileNotFoundError(
            f"Missing ML input file: {INPUT_PATH}. "
            f"Run: python mapreduce_analysis.py"
        )

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    if "date" not in df.columns:
        raise ValueError("ML input must contain date column.")

    if "symbol" not in df.columns:
        raise ValueError("ML input must contain symbol column.")

    df = df.withColumn("date", F.to_date("date"))
    df = df.withColumn("symbol", F.trim(F.col("symbol")))

    symbol_filter = parse_symbol_filter()

    if symbol_filter:
        df = df.filter(F.col("symbol").isin(*symbol_filter))
        print(f"[FILTER] Training only symbols: {symbol_filter}")
    else:
        print("[FILTER] No ML_SYMBOL_FILTER provided. Training all symbols.")

    if df.limit(1).count() == 0:
        raise ValueError(
            f"No data after ML_SYMBOL_FILTER={ML_SYMBOL_FILTER}. "
            f"Check symbol name in output/market_analysis_results.csv."
        )

    if "gold_price" not in df.columns:
        df = df.withColumn("gold_price", F.lit(0.0))

    for col in FEATURE_COLUMNS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(0.0))

        df = df.withColumn(col, F.col(col).cast("double"))

    # Fallback: create current market risk target if missing.
    if LABEL_COL not in df.columns:
        print(f"[TARGET] {LABEL_COL} missing. Creating fallback current risk label.")

        stock_vol_threshold = safe_quantile(df, "stock_volatility", 0.75)
        oil_vol_threshold = safe_quantile(df, "oil_volatility", 0.75)
        gold_vol_threshold = safe_quantile(df, "gold_volatility", 0.75)

        df = df.withColumn(
            LABEL_COL,
            F.when(
                (F.abs(F.coalesce(F.col("stock_return_pct"), F.lit(0.0))) >= 2.0)
                | (F.abs(F.coalesce(F.col("oil_return_pct"), F.lit(0.0))) >= 2.0)
                | (F.abs(F.coalesce(F.col("gold_return_pct"), F.lit(0.0))) >= 2.0)
                | (F.coalesce(F.col("stock_volatility"), F.lit(0.0)) >= stock_vol_threshold)
                | (F.coalesce(F.col("oil_volatility"), F.lit(0.0)) >= oil_vol_threshold)
                | (F.coalesce(F.col("gold_volatility"), F.lit(0.0)) >= gold_vol_threshold)
                | (F.col("alert_level") == "HIGH"),
                1.0,
            ).otherwise(0.0),
        )

    df = df.withColumn(LABEL_COL, F.col(LABEL_COL).cast("double"))
    df = df.dropna(subset=["date", "symbol", "stock_close", LABEL_COL])
    df = df.fillna(0.0, subset=FEATURE_COLUMNS)

    for col in FEATURE_COLUMNS:
        df = df.withColumn(
            col,
            F.when(F.isnan(F.col(col)) | F.col(col).isNull(), F.lit(0.0))
            .otherwise(F.col(col)),
        )

    return df


def add_class_weights(df):
    counts = {
        float(row[LABEL_COL]): row["count"]
        for row in df.groupBy(LABEL_COL).count().collect()
    }

    total = sum(counts.values())

    if total == 0 or len(counts) < 2:
        return df.withColumn("class_weight", F.lit(1.0))

    count_0 = counts.get(0.0, 1)
    count_1 = counts.get(1.0, 1)

    weight_0 = total / (2.0 * count_0)
    weight_1 = total / (2.0 * count_1)

    print(f"[WEIGHT] class 0 weight: {weight_0:.4f}")
    print(f"[WEIGHT] class 1 weight: {weight_1:.4f}")

    return df.withColumn(
        "class_weight",
        F.when(F.col(LABEL_COL) == 1.0, F.lit(weight_1)).otherwise(F.lit(weight_0)),
    )


def split_train_test(df):
    """
    Time-based split per symbol.
    """

    from pyspark.sql import Window

    window = Window.partitionBy("symbol").orderBy("date")
    count_window = Window.partitionBy("symbol")

    df = (
        df.withColumn("_row_number", F.row_number().over(window))
        .withColumn("_row_count", F.count("*").over(count_window))
        .withColumn("_split_ratio", F.col("_row_number") / F.col("_row_count"))
    )

    train_df = df.filter(F.col("_split_ratio") <= TRAIN_RATIO)
    test_df = df.filter(F.col("_split_ratio") > TRAIN_RATIO)

    if test_df.limit(1).count() == 0:
        train_df, test_df = df.randomSplit([TRAIN_RATIO, 1 - TRAIN_RATIO], seed=SEED)

    return train_df, test_df


def compute_baseline(test_df) -> dict:
    total = test_df.count()

    if total == 0:
        return {
            "baseline_majority_class": None,
            "baseline_accuracy": 0.0,
            "class_0_count": 0,
            "class_1_count": 0,
        }

    counts = {
        int(row[LABEL_COL]): row["count"]
        for row in test_df.groupBy(LABEL_COL).count().collect()
    }

    class_0_count = counts.get(0, 0)
    class_1_count = counts.get(1, 0)

    if class_1_count >= class_0_count:
        majority_class = 1
        baseline_accuracy = class_1_count / total
    else:
        majority_class = 0
        baseline_accuracy = class_0_count / total

    return {
        "baseline_majority_class": majority_class,
        "baseline_accuracy": baseline_accuracy,
        "class_0_count": class_0_count,
        "class_1_count": class_1_count,
    }


def train_model(train_df):
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep",
    )

    rf_params = {
        "featuresCol": "features",
        "labelCol": LABEL_COL,
        "predictionCol": "prediction",
        "probabilityCol": "probability",
        "rawPredictionCol": "rawPrediction",
        "numTrees": int(os.getenv("RF_NUM_TREES", "150")),
        "maxDepth": int(os.getenv("RF_MAX_DEPTH", "5")),
        "minInstancesPerNode": int(os.getenv("RF_MIN_INSTANCES_PER_NODE", "10")),
        "seed": SEED,
    }

    if USE_CLASS_WEIGHT:
        rf_params["weightCol"] = "class_weight"

    rf = RandomForestClassifier(**rf_params)

    pipeline = Pipeline(stages=[assembler, rf])
    return pipeline.fit(train_df)


def safe_evaluate_binary(predictions) -> float:
    try:
        evaluator = BinaryClassificationEvaluator(
            labelCol=LABEL_COL,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        return float(evaluator.evaluate(predictions))
    except Exception:
        return 0.0


def evaluate_model(model, test_df):
    predictions = model.transform(test_df)

    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="accuracy",
    )

    precision_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="weightedPrecision",
    )

    recall_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="weightedRecall",
    )

    f1_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="f1",
    )

    auc = safe_evaluate_binary(predictions)
    accuracy = float(accuracy_eval.evaluate(predictions))
    precision = float(precision_eval.evaluate(predictions))
    recall = float(recall_eval.evaluate(predictions))
    f1 = float(f1_eval.evaluate(predictions))

    baseline = compute_baseline(test_df)

    metrics = {
        "symbol_filter": ML_SYMBOL_FILTER or "ALL",
        "label_col": LABEL_COL,
        "prediction_task": "CURRENT_MARKET_RISK_CLASSIFICATION",
        "rows_test": test_df.count(),
        "class_0_count": baseline["class_0_count"],
        "class_1_count": baseline["class_1_count"],
        "baseline_majority_class": baseline["baseline_majority_class"],
        "baseline_accuracy": baseline["baseline_accuracy"],
        "auc": auc,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "improvement_over_baseline": accuracy - baseline["baseline_accuracy"],
    }

    return predictions, metrics


def save_metrics(metrics: dict) -> None:
    ensure_parent(EVAL_PATH)

    with open(EVAL_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(metrics.keys()))
        writer.writeheader()
        writer.writerow(metrics)

    print(f"[OUTPUT] Evaluation saved to {EVAL_PATH}")


def save_feature_importance(model) -> None:
    ensure_parent(FEATURE_IMPORTANCE_PATH)

    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray().tolist()

    rows = [
        {
            "feature": feature,
            "importance": float(importance),
        }
        for feature, importance in zip(FEATURE_COLUMNS, importances)
    ]

    rows = sorted(rows, key=lambda x: x["importance"], reverse=True)

    with open(FEATURE_IMPORTANCE_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["feature", "importance"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OUTPUT] Feature importance saved to {FEATURE_IMPORTANCE_PATH}")


def save_prediction_sample(predictions) -> None:
    ensure_parent(PRED_SAMPLE_PATH)

    out = (
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
            "date",
            "symbol",
            "stock_close",
            "oil_price",
            "gold_price",
            LABEL_COL,
            "prediction",
            "prediction_label",
            "probability_high_risk",
        )
        .orderBy(F.desc("date"))
        .limit(200)
    )

    out.toPandas().to_csv(PRED_SAMPLE_PATH, index=False)

    print(f"[OUTPUT] Prediction sample saved to {PRED_SAMPLE_PATH}")


def save_model(model) -> None:
    model_path = Path(MODEL_PATH)
    meta_path = Path(MODEL_META_PATH)

    if model_path.exists():
        shutil.rmtree(model_path)

    if meta_path.exists():
        meta_path.unlink()

    model_path.parent.mkdir(parents=True, exist_ok=True)

    model.save(MODEL_PATH)

    meta = {
        "model_path": MODEL_PATH,
        "feature_columns": FEATURE_COLUMNS,
        "label_col": LABEL_COL,
        "algorithm": "RandomForestClassifier",
        "prediction_task": "current_market_risk_classification",
        "positive_class": "HIGH RISK",
        "negative_class": "NORMAL",
        "risk_definition": "Current market risk based on return, volatility, correlation and alert level.",
        "symbol_filter": ML_SYMBOL_FILTER or "ALL",
        "train_ratio": TRAIN_RATIO,
        "use_class_weight": USE_CLASS_WEIGHT,
        "rf_num_trees": int(os.getenv("RF_NUM_TREES", "150")),
        "rf_max_depth": int(os.getenv("RF_MAX_DEPTH", "5")),
        "rf_min_instances_per_node": int(os.getenv("RF_MIN_INSTANCES_PER_NODE", "10")),
        "pyspark_version": pyspark.__version__,
    }

    with open(MODEL_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"[OUTPUT] Model saved to {MODEL_PATH}")
    print(f"[OUTPUT] Model metadata saved to {MODEL_META_PATH}")


def main() -> None:
    print("=" * 80)
    print("SPARK MLLIB TRAINING - CURRENT MARKET RISK")
    print("=" * 80)
    print(f"Input: {INPUT_PATH}")
    print(f"Symbol filter: {ML_SYMBOL_FILTER or 'ALL'}")
    print(f"Label column: {LABEL_COL}")
    print(f"Use class weight: {USE_CLASS_WEIGHT}")
    print(f"Model path: {MODEL_PATH}")
    print(f"Train ratio: {TRAIN_RATIO}")

    spark = build_spark()

    df = load_training_data(spark)

    total_rows = df.count()
    symbols = [r["symbol"] for r in df.select("symbol").distinct().collect()]

    print(f"[LOAD] Rows for ML: {total_rows}")
    print(f"[LOAD] Symbols: {symbols}")

    label_counts = df.groupBy(LABEL_COL).count().orderBy(LABEL_COL).collect()
    print("[LOAD] Label distribution:")
    for row in label_counts:
        print(f"  label={row[LABEL_COL]} count={row['count']}")

    if USE_CLASS_WEIGHT:
        df = add_class_weights(df)
    else:
        print("[WEIGHT] Class weight disabled.")
        df = df.withColumn("class_weight", F.lit(1.0))

    train_df, test_df = split_train_test(df)

    print(f"[SPLIT] Train rows: {train_df.count()}")
    print(f"[SPLIT] Test rows: {test_df.count()}")

    model = train_model(train_df)
    predictions, metrics = evaluate_model(model, test_df)

    print("[EVALUATION]")
    for key, value in metrics.items():
        print(f"- {key}: {value}")

    save_model(model)
    save_metrics(metrics)
    save_feature_importance(model)
    save_prediction_sample(predictions)

    spark.stop()

    print("=" * 80)
    print("Spark ML current market risk training completed.")
    print("=" * 80)


if __name__ == "__main__":
    main()
