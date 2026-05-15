#!/usr/bin/env python3
# ============================================
# Task 7: Random Forest Feature Importances + interpretation notes
# Author: Lalbabtain (ID: ________)
# Group 5 — SE446 M2
# ============================================
"""
Print RF feature importances from a fitted pipeline (run after Task 5/6 training).

For full training use m2_spark_ml.py on cluster. This script documents Task 7 outputs
and can load pre-exported CSV from output/m2_real/task07_rf_feature_importance.csv.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Task 7 — RF feature importances")
    p.add_argument(
        "--csv",
        default="output/m2_real/task07_rf_feature_importance.csv",
        help="Exported importance table from cluster/notebook",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.csv)
    if not path.exists():
        print(f"Missing {path}. Run ML pipeline on cluster first.")
        return 1

    df = pd.read_csv(path).sort_values("importance", ascending=False)
    print("=== Task 7: Random Forest feature importances ===")
    print(df.to_string(index=False))

    top = df.iloc[0]
    print(
        "\nInterpretation:\n"
        f"- Most important feature: {top['feature']} ({top['importance']:.4f})\n"
        "- crime_index (Primary Type) usually dominates → matches Task 4 where arrest rates\n"
        "  differ sharply by type (e.g. NARCOTICS very high vs THEFT low).\n"
        "- Logistic Regression underperforms tree models because arrest outcome is non-linear\n"
        "  in crime type; trees split on indexed type without assuming linear effect.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
