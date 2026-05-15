#!/usr/bin/env python3
# ============================================
# Task 2: Location Hotspots (Spark SQL)
# Author: Mkalissa (ID: ________)
# Group 5 — SE446 M2
# ============================================
"""Top-10 crime locations using spark.sql() on temp view `crimes`."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Task 2 — location hotspots (Spark SQL)")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--input", default="hdfs:///data/chicago_crimes.csv")
    p.add_argument(
        "--output-csv",
        default="output/m2_real/task02_location_hotspots_top10.csv",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("SE446_M2_G5_Task2_Mkalissa")
        .master(args.master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
    )
    df.createOrReplaceTempView("crimes")

    result = spark.sql(
        """
        SELECT `Location Description`, COUNT(*) AS total
        FROM crimes
        GROUP BY `Location Description`
        ORDER BY total DESC
        LIMIT 10
        """
    )

    print("=== Task 2: Top 10 location hotspots (Spark SQL) ===")
    result.show(truncate=False)

    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)
    result.toPandas().to_csv(args.output_csv, index=False)
    print(f"Wrote {args.output_csv}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
