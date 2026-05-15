#!/usr/bin/env bash
# Run on cluster after: ssh USER@134.209.172.50
# Usage:
#   ./scripts/run_m2_cluster.sh phase_a          # Tasks 1-4 full HDFS
#   ./scripts/run_m2_cluster.sh full             # Tasks 1-7 (ML sampled)
#   ./scripts/run_m2_cluster.sh spark_submit_ml  # Task 11 cluster deploy
set -euo pipefail

source /etc/profile.d/hadoop.sh
export SPARK_HOME=/opt/spark
export PATH="$SPARK_HOME/bin:$PATH"
PY4J="$(ls "$SPARK_HOME"/python/lib/py4j-*.zip | head -1)"
export PYTHONPATH="$SPARK_HOME/python:$PY4J:$(cd "$(dirname "$0")/.." && pwd)"
cd "$(dirname "$0")/.."

MODE="${1:-phase_a}"
HDFS_IN="${HDFS_IN:-hdfs:///data/chicago_crimes.csv}"
OUT="output/m2_real"
mkdir -p "$OUT" output/spark_submit

COMMON=(
  --master yarn
  --deploy-mode client
  --driver-memory 512m
  --executor-memory 1g
  --num-executors 1
  --executor-cores 1
  --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3"
  --conf "spark.executorEnv.PYSPARK_PYTHON=python3"
)

case "$MODE" in
  phase_a)
    spark-submit "${COMMON[@]}" m2_cluster_outputs.py \
      --master yarn --input "$HDFS_IN" --output-dir "$OUT" --skip-ml
    ;;
  full)
    spark-submit "${COMMON[@]}" m2_cluster_outputs.py \
      --master yarn --input "$HDFS_IN" --output-dir "$OUT" --ml-sample-fraction 0.05
    ;;
  spark_submit_ml)
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --driver-memory 512m \
      --num-executors 1 \
      --executor-memory 1g \
      --executor-cores 1 \
      --conf spark.driver.maxResultSize=128m \
      --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 \
      --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
      m2_spark_ml.py --hdfs-path "$HDFS_IN" --sample-fraction 0.05
    ;;
  *)
    echo "Unknown mode: $MODE (phase_a | full | spark_submit_ml)" >&2
    exit 1
    ;;
esac
