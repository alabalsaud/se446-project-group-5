#!/usr/bin/env python3
# ============================================
# SE446 - Milestone 2: Spark ML Pipeline (spark-submit)
# Group 5
#
# Phase B (Tasks 5–7): edit author lines below with your name + student ID.
# Task 5–6: Alabalsaud (ID: ________)
# Task 7:   Lalbabtain (ID: ________)
# ============================================
"""
Standalone ML pipeline for arrest prediction. Intended for:

  spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --driver-memory 512m \\
    --num-executors 1 \\
    --executor-memory 1g \\
    --executor-cores 1 \\
    --conf spark.driver.maxResultSize=128m \\
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.12 \\
    --conf spark.executorEnv.PYSPARK_PYTHON=python3.12 \\
    m2_spark_ml.py --hdfs-path hdfs:///data/chicago_crimes.csv --sample-fraction 0.05

Local laptop (first-time / Task 9 style, no CSV needed):

  python3 m2_spark_ml.py --master 'local[*]' --synthetic-rows 10000 --sample-fraction 1.0

Cluster-mode drivers log to YARN; use: yarn logs -applicationId <appId>
"""

from __future__ import annotations

import argparse
import time
from typing import Any

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SE446 M2: Spark MLlib arrest classifier")
    p.add_argument(
        "--master",
        default=None,
        help='Spark master URL, e.g. "local[*]" for laptop smoke tests.',
    )
    p.add_argument(
        "--hdfs-path",
        default="hdfs:///data/chicago_crimes.csv",
        help="Input CSV path (ignored if --synthetic-rows > 0)",
    )
    p.add_argument(
        "--sample-fraction",
        type=float,
        default=0.05,
        help="Row sample rate for training (use <1.0 on small YARN containers)",
    )
    p.add_argument(
        "--sample-seed",
        type=int,
        default=42,
        help="Seed for sampling and splits",
    )
    p.add_argument(
        "--synthetic-rows",
        type=int,
        default=0,
        help="If > 0, generate this many labelled rows locally (Tasks 9 / laptop debugging). Ignores CSV path.",
    )
    return p.parse_args()


def build_spark(master: str | None) -> SparkSession:
    b = SparkSession.builder.appName("SE446_M2_G5_ML")
    if master:
        b = b.master(master)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def prepare_ml_frame(df_raw, sample_fraction: float, seed: int):
    """Common cleaning / label / hour features for ML (CSV or synthetic)."""
    df = (
        df_raw.select(
            F.col("Primary Type").alias("Primary Type"),
            F.col("Location Description").alias("Location Description"),
            F.col("Arrest").alias("Arrest"),
            F.col("Domestic").alias("Domestic"),
            F.col("District").cast("int").alias("District"),
            F.col("Year").cast("int").alias("Year"),
            F.col("Date").alias("Date"),
        )
        .withColumn(
            "label",
            F.when(F.lower(F.col("Arrest").cast("string")).isin("true", "1", "yes"), 1).otherwise(0),
        )
        .withColumn("Domestic_str", F.lower(F.col("Domestic").cast("string")))
        .withColumn(
            "Hour",
            F.coalesce(
                F.hour(F.to_timestamp(F.col("Date"), "MM/dd/yyyy hh:mm:ss a")),
                F.hour(F.to_timestamp(F.col("Date"), "MM/dd/yyyy HH:mm:ss")),
            ),
        )
    )

    df = df.fillna({"District": -1, "Hour": -1})

    if sample_fraction < 1.0:
        df = df.sample(withReplacement=False, fraction=sample_fraction, seed=seed)

    df = df.filter(F.col("label").isin(0, 1))
    return df


def load_csv(spark: SparkSession, path: str) -> Any:
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def load_synthetic(spark: SparkSession, n: int, seed: int) -> Any:
    """Small in-memory labelled sample for local[*] runs (Course Task 9 style)."""
    import numpy as np
    import pandas as pd

    crime_types = [
        "THEFT",
        "BATTERY",
        "CRIMINAL DAMAGE",
        "NARCOTICS",
        "ASSAULT",
        "MOTOR VEHICLE THEFT",
        "BURGLARY",
        "OTHER OFFENSE",
        "DECEPTIVE PRACTICE",
        "ROBBERY",
    ]
    locations = [
        "STREET",
        "RESIDENCE",
        "APARTMENT",
        "SIDEWALK",
        "PARKING LOT/GARAGE(NON.RESID.)",
        "OTHER",
        "GROCERY FOOD STORE",
        "DEPARTMENT STORE",
        "AIRPORT/AIRCRAFT",
        "BAR OR TAVERN",
    ]

    rng = np.random.default_rng(seed)
    years = rng.integers(2001, 2026, size=n)
    districts = rng.integers(1, 26, size=n)
    hour = rng.integers(0, 24, size=n)
    domestic = rng.random(size=n) < 0.2

    ctype_idx = rng.integers(0, len(crime_types), size=n)
    base_p = np.array([0.22, 0.25, 0.18, 0.55, 0.28, 0.15, 0.20, 0.12, 0.10, 0.30])
    p = base_p[ctype_idx] + 0.05 * domestic.astype(float)
    p = np.clip(p, 0.02, 0.85)
    arrest = rng.random(size=n) < p

    loc_idx = rng.integers(0, len(locations), size=n)
    primary = np.array(crime_types)[ctype_idx]
    loc = np.array(locations)[loc_idx]

    month = rng.integers(1, 13, size=n)
    day = rng.integers(1, 29, size=n)
    minute = rng.integers(0, 60, size=n)
    second = rng.integers(0, 60, size=n)
    ampm = np.where(hour < 12, "AM", "PM")
    h12 = np.where((hour % 12) == 0, 12, hour % 12)
    date_str = [
        f"{m:02d}/{d:02d}/{y:04d} {hh:02d}:{mm:02d}:{ss:02d} {ap}"
        for m, d, y, hh, mm, ss, ap in zip(month, day, years, h12, minute, second, ampm)
    ]

    pdf = pd.DataFrame(
        {
            "Primary Type": primary,
            "Location Description": loc,
            "Arrest": arrest,
            "Domestic": domestic,
            "District": districts.astype(int),
            "Year": years.astype(int),
            "Date": date_str,
        }
    )
    return spark.createDataFrame(pdf)


def load_and_prepare(spark: SparkSession, path: str, sample_fraction: float, seed: int):
    df_raw = load_csv(spark, path)
    return prepare_ml_frame(df_raw, sample_fraction, seed)


def confusion_counts(predictions) -> dict[str, int]:
    tp = predictions.filter((F.col("label") == 1) & (F.col("prediction") == 1)).count()
    tn = predictions.filter((F.col("label") == 0) & (F.col("prediction") == 0)).count()
    fp = predictions.filter((F.col("label") == 0) & (F.col("prediction") == 1)).count()
    fn = predictions.filter((F.col("label") == 1) & (F.col("prediction") == 0)).count()
    return {"TN": tn, "FP": fp, "FN": fn, "TP": tp}


def eval_metrics(predictions) -> dict[str, float]:
    auc = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction",
        labelCol="label",
        metricName="areaUnderROC",
    ).evaluate(predictions)

    acc_eval = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy",
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    )
    pr_eval = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedPrecision",
    )
    rc_eval = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedRecall",
    )

    return {
        "AUC-ROC": float(auc),
        "Accuracy": float(acc_eval.evaluate(predictions)),
        "F1": float(f1_eval.evaluate(predictions)),
        "Precision": float(pr_eval.evaluate(predictions)),
        "Recall": float(rc_eval.evaluate(predictions)),
    }


def build_feature_pipeline(classifier: Any) -> Pipeline:
    crime_indexer = StringIndexer(
        inputCol="Primary Type",
        outputCol="crime_index",
        handleInvalid="skip",
    )
    domestic_indexer = StringIndexer(
        inputCol="Domestic_str",
        outputCol="domestic_index",
        handleInvalid="skip",
    )
    assembler = VectorAssembler(
        inputCols=["District", "crime_index", "Hour", "domestic_index"],
        outputCol="features",
        handleInvalid="skip",
    )
    return Pipeline(stages=[crime_indexer, domestic_indexer, assembler, classifier])


def train_eval_classifier(clf: Any, train_df, test_df):
    pipeline = build_feature_pipeline(clf)
    t0 = time.perf_counter()
    model = pipeline.fit(train_df)
    train_s = time.perf_counter() - t0

    preds = model.transform(test_df)
    metrics = eval_metrics(preds)
    cm = confusion_counts(preds)
    return metrics, cm, train_s, model


def print_feature_importance_rf(rf_model) -> None:
    # Last stage is the classifier
    rf_stage = rf_model.stages[-1]
    feats = ["District", "crime_index", "Hour", "domestic_index"]
    imp = rf_stage.featureImportances.toArray().tolist()
    ranked = sorted(zip(feats, imp), key=lambda x: x[1], reverse=True)
    print("\n=== Task 7: Random Forest feature importances ===")
    for f, v in ranked:
        print(f"{f:16s}  {v:.6f}")


def main() -> int:
    args = parse_args()
    spark = build_spark(args.master)

    print("=== SE446 M2 ML (spark-submit) ===")
    print(f"Master: {spark.sparkContext.master}")
    print(f"App name: {spark.sparkContext.appName}")
    if args.synthetic_rows > 0:
        print(f"Synthetic rows: {args.synthetic_rows}")
    else:
        print(f"Reading: {args.hdfs_path}")
    print(f"Sample fraction: {args.sample_fraction} (seed={args.sample_seed})")

    if args.synthetic_rows > 0:
        raw = load_synthetic(spark, args.synthetic_rows, args.sample_seed)
        df = prepare_ml_frame(raw, args.sample_fraction, args.sample_seed)
    else:
        df = load_and_prepare(
            spark,
            args.hdfs_path,
            sample_fraction=args.sample_fraction,
            seed=args.sample_seed,
        )

    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    n = df.count()
    print(f"Row count after filter/sample: {n}")

    seed = args.sample_seed
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=seed)
    train_df = train_df.persist(StorageLevel.MEMORY_AND_DISK)
    test_df = test_df.persist(StorageLevel.MEMORY_AND_DISK)
    train_df.count()
    test_df.count()

    models = {
        "LogisticRegression": LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=100,
            regParam=0.01,
        ),
        "RandomForest": RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=100,
            maxDepth=5,
            seed=seed,
        ),
        "GBT": GBTClassifier(
            featuresCol="features",
            labelCol="label",
            maxIter=50,
            maxDepth=5,
            seed=seed,
        ),
    }

    results: dict[str, tuple[dict[str, float], dict[str, int], float, Any]] = {}

    for mname, clf in models.items():
        metrics, cm, train_s, fitted = train_eval_classifier(clf, train_df, test_df)
        results[mname] = (metrics, cm, train_s, fitted)
        print("\n" + "=" * 72)
        print(f"Model: {mname}")
        print(f"Training time (s): {train_s:.3f}")
        print("Metrics:", metrics)
        print("Confusion matrix:", cm)
        if mname == "RandomForest":
            print_feature_importance_rf(fitted)

    print("\n=== Side-by-side (Task 6) ===")
    hdr = (
        f"{'Model':<20}"
        f"{'AUC-ROC':>10}{'Acc':>10}{'F1':>10}{'Prec':>10}{'Rec':>10}"
        f"{'TN':>8}{'FP':>8}{'FN':>8}{'TP':>8}{'TrainS':>10}"
    )
    print(hdr)
    for mname, (metrics, cm, train_s, _fitted) in results.items():
        print(
            f"{mname:<20}"
            f"{metrics['AUC-ROC']:>10.4f}"
            f"{metrics['Accuracy']:>10.4f}"
            f"{metrics['F1']:>10.4f}"
            f"{metrics['Precision']:>10.4f}"
            f"{metrics['Recall']:>10.4f}"
            f"{cm['TN']:>8}{cm['FP']:>8}{cm['FN']:>8}{cm['TP']:>8}"
            f"{train_s:>10.3f}"
        )

    spark.stop()
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
