#!/usr/bin/env python3
# ============================================
# Task 4: Arrest Rate Analysis (overall + by crime type)
# Author: Lalbabtain (ID: ________)
# Group 5 — SE446 M2
# ============================================
"""Overall arrest rate and per-type rates for top-10 crime types by volume."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Task 4 — arrest rate analysis")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--input", default="hdfs:///data/chicago_crimes.csv")
    p.add_argument("--output-dir", default="output/m2_real")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out = args.output_dir
    os.makedirs(out, exist_ok=True)

    spark = SparkSession.builder.appName("SE446_M2_G5_Task4_Lalbabtain").master(args.master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
        .withColumn(
            "label",
            F.when(F.lower(F.col("Arrest").cast("string")).isin("true", "1", "yes"), 1).otherwise(0),
        )
    )

    overall = df.select(
        F.avg(F.col("label").cast("double")).alias("arrest_rate"),
        F.count(F.lit(1)).alias("n"),
    )
    overall.toPandas().to_csv(f"{out}/task04_arrest_overall.csv", index=False)

    by_vol = (
        df.groupBy("Primary Type")
        .agg(
            F.avg(F.col("label").cast("double")).alias("arrest_rate"),
            F.count(F.lit(1)).alias("n"),
        )
        .orderBy(F.col("n").desc())
        .limit(10)
    )
    by_vol.toPandas().to_csv(f"{out}/task04_arrest_rate_top10_crime_types_by_volume.csv", index=False)
    by_vol.orderBy(F.col("arrest_rate").desc()).toPandas().to_csv(
        f"{out}/task04b_among_top10_volume_sorted_by_arrest_rate_desc.csv", index=False
    )

    print("=== Task 4: Overall arrest rate ===")
    overall.show(truncate=False)
    print("=== Top 10 crime types by volume (with arrest rate) ===")
    by_vol.show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
