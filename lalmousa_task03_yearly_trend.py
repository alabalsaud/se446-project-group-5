#!/usr/bin/env python3
# ============================================
# Task 3: Crime Trend Over Years (Spark + matplotlib)
# Author: Lalmousa (ID: ________)
# Group 5 — SE446 M2
# ============================================
"""Yearly crime counts; saves CSV and optional line chart (local mode)."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Task 3 — crimes per year")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--input", default="hdfs:///data/chicago_crimes.csv")
    p.add_argument("--output-csv", default="output/m2_real/task03_crimes_per_year.csv")
    p.add_argument("--plot", action="store_true", help="Write PNG when master is local[*]")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("SE446_M2_G3_Task3_Lalmousa").master(args.master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
        .withColumn("Year", F.col("Year").cast("int"))
        .filter(F.col("Year").isNotNull())
    )

    yearly = df.groupBy("Year").count().orderBy("Year")
    pdf = yearly.toPandas()
    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)
    pdf.to_csv(args.output_csv, index=False)
    print(f"Wrote {args.output_csv}")
    print(pdf.tail(10))

    if args.plot and args.master.startswith("local"):
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 5))
        plt.plot(pdf["Year"], pdf["count"], marker="o")
        plt.title("Chicago crimes per year (Group 5)")
        plt.xlabel("Year")
        plt.ylabel("Count")
        plt.grid(True, alpha=0.3)
        out_png = "output/m2_real/task03_crimes_per_year.png"
        plt.savefig(out_png, dpi=120, bbox_inches="tight")
        print(f"Wrote {out_png}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
