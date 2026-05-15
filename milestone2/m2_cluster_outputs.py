#!/usr/bin/env python3
# ============================================
# SE446 M2 — Export each task result to separate files (full HDFS dataset)
# Group 5 — run on cluster after: ssh YOUR_USER@134.209.172.50
#
# Phase A (Tasks 1–4): full df after cleaning (matches notebook SQL/DataFrame logic).
# Phase B (Tasks 5–7): optional sample on cluster (--ml-sample-fraction, default 0.05).
#
# Author: Group 5 (add IDs before submission)
# ============================================
"""
Run on the cluster after SSH from your terminal (password/key).
After SSH:

  cd ~/se446-project-group-5/milestone2   # run from milestone2 folder
  export PYTHONPATH="$(pwd)"

  spark-submit --master yarn --deploy-mode client \\
    --driver-memory 2g \\
    --executor-memory 2g \\
    --num-executors 2 \\
    --executor-cores 2 \\
    m2_cluster_outputs.py \\
      --master yarn \\
      --input hdfs:///data/chicago_crimes.csv \\
      --output-dir output/m2_real \\
      --ml-sample-fraction 0.05

Outputs land under ./output/m2_real/ (driver local dir — use client deploy mode).

Skip ML only:

  spark-submit ... m2_cluster_outputs.py --skip-ml ...
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from m2_spark_ml import (
    build_spark,
    confusion_counts,
    eval_metrics,
    load_csv,
    prepare_ml_frame,
    train_eval_classifier,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="M2 cluster exports — separate file per task")
    p.add_argument("--master", default="yarn", help='Spark master (use "yarn" on cluster)')
    p.add_argument("--input", default="hdfs:///data/chicago_crimes.csv", help="Chicago CSV path")
    p.add_argument(
        "--output-dir",
        default="output/m2_real",
        help="Directory on driver filesystem for CSV/JSON exports",
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--ml-sample-fraction",
        type=float,
        default=0.05,
        help="Fraction of cleaned rows for Tasks 5–7 (course guidance for cluster memory)",
    )
    p.add_argument("--skip-ml", action="store_true", help="Only Tasks 1–4")
    return p.parse_args()


def df_features_json_rows(transformed_sample):
    """Turn small Spark DF rows into records with human-readable features."""
    rows_out = []
    for row in transformed_sample.collect():
        v = row["features"]
        vec = v.toArray().tolist() if hasattr(v, "toArray") else list(v)
        rows_out.append(
            {
                "Primary Type": row["Primary Type"],
                "District": row["District"],
                "Hour": row["Hour"],
                "Domestic_str": row["Domestic_str"],
                "feature_positions_note": "[0]=District [1]=crime_index [2]=Hour [3]=domestic_index",
                "features_vector": vec,
            }
        )
    return rows_out


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark(args.master if args.master else None)
    spark.sparkContext.setLogLevel("WARN")

    meta = {
        "utc_timestamp": datetime.now(timezone.utc).isoformat(),
        "spark_master": spark.sparkContext.master,
        "app_name": spark.sparkContext.appName,
        "input_path": args.input,
        "output_dir": str(out_dir),
    }

    t_load = time.perf_counter()
    raw = load_csv(spark, args.input)
    df_full = prepare_ml_frame(raw, sample_fraction=1.0, seed=args.seed).persist(StorageLevel.MEMORY_AND_DISK)
    n_rows = df_full.count()
    meta["cleaned_row_count"] = int(n_rows)
    meta["seconds_load_and_prepare"] = round(time.perf_counter() - t_load, 3)
    (out_dir / "RUN_META.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # ---------- Task 1 ----------
    t1 = df_full.groupBy("Primary Type").count().orderBy(F.col("count").desc()).limit(10)
    t1.toPandas().to_csv(out_dir / "task01_crime_types_top10.csv", index=False)

    # ---------- Task 2 (Spark SQL) ----------
    df_full.createOrReplaceTempView("crimes")
    q2 = """
    SELECT `Location Description`, COUNT(*) AS total
    FROM crimes
    GROUP BY `Location Description`
    ORDER BY total DESC
    LIMIT 10
    """
    spark.sql(q2).toPandas().to_csv(out_dir / "task02_location_hotspots_top10.csv", index=False)

    # ---------- Task 3 ----------
    df_full.groupBy("Year").count().orderBy("Year").toPandas().to_csv(
        out_dir / "task03_crimes_per_year.csv", index=False
    )

    # ---------- Task 4 ----------
    overall = df_full.select(
        F.avg(F.col("label").cast("double")).alias("arrest_rate"),
        F.count(F.lit(1)).alias("n"),
    ).toPandas()
    overall.to_csv(out_dir / "task04_arrest_overall.csv", index=False)

    by_vol = (
        df_full.groupBy("Primary Type")
        .agg(
            F.avg(F.col("label").cast("double")).alias("arrest_rate"),
            F.count(F.lit(1)).alias("n"),
        )
        .orderBy(F.col("n").desc())
        .limit(10)
    )
    by_vol.toPandas().to_csv(out_dir / "task04_arrest_rate_top10_crime_types_by_volume.csv", index=False)

    by_vol_hi = by_vol.orderBy(F.col("arrest_rate").desc())
    by_vol_hi.toPandas().to_csv(out_dir / "task04b_among_top10_volume_sorted_by_arrest_rate_desc.csv", index=False)

    if args.skip_ml:
        spark.stop()
        print(f"Wrote Tasks 1–4 under {out_dir}")
        return 0

    # ---------- Phase B on sampled data ----------
    df_ml = df_full.sample(withReplacement=False, fraction=args.ml_sample_fraction, seed=args.seed).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    meta_ml = {"ml_sample_fraction": args.ml_sample_fraction, "ml_row_count": int(df_ml.count())}
    (out_dir / "TASK05_07_META.json").write_text(json.dumps(meta_ml, indent=2), encoding="utf-8")

    seed = args.seed
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=seed)
    train_df = train_df.persist(StorageLevel.MEMORY_AND_DISK)
    test_df = test_df.persist(StorageLevel.MEMORY_AND_DISK)
    train_df.count()
    test_df.count()

    crime_indexer = StringIndexer(inputCol="Primary Type", outputCol="crime_index", handleInvalid="skip")
    domestic_indexer = StringIndexer(inputCol="Domestic_str", outputCol="domestic_index", handleInvalid="skip")
    assembler = VectorAssembler(
        inputCols=["District", "crime_index", "Hour", "domestic_index"],
        outputCol="features",
        handleInvalid="skip",
    )
    prep = Pipeline(stages=[crime_indexer, domestic_indexer, assembler])
    prep_model = prep.fit(train_df)

    sample_t = prep_model.transform(train_df.limit(5)).select(
        "Primary Type", "District", "Hour", "Domestic_str", "features"
    )
    task5_payload = df_features_json_rows(sample_t)
    (out_dir / "task05_features_sample_5rows.json").write_text(json.dumps(task5_payload, indent=2), encoding="utf-8")

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

    comparison_rows = []
    rf_model = None

    for name, clf in models.items():
        metrics, cm, train_s, fitted = train_eval_classifier(clf, train_df, test_df)
        comparison_rows.append(
            {
                "Model": name,
                **metrics,
                **cm,
                "TrainSeconds": train_s,
            }
        )
        if name == "RandomForest":
            rf_model = fitted

    pd.DataFrame(comparison_rows).to_csv(out_dir / "task06_model_comparison.csv", index=False)

    # Confusion matrices separate files for clarity
    for row in comparison_rows:
        name = row["Model"]
        cm_only = {k: row[k] for k in ("TN", "FP", "FN", "TP")}
        (out_dir / f"task06_confusion_{name}.json").write_text(json.dumps(cm_only, indent=2), encoding="utf-8")

    if rf_model is None:
        raise RuntimeError("RandomForest model missing")

    rf_stage = rf_model.stages[-1]
    feats = ["District", "crime_index", "Hour", "domestic_index"]
    imp = rf_stage.featureImportances.toArray().tolist()
    pd.DataFrame({"feature": feats, "importance": imp}).sort_values(
        "importance", ascending=False
    ).to_csv(out_dir / "task07_rf_feature_importance.csv", index=False)

    spark.stop()
    print(f"Done. All exports under {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
