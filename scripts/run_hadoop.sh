#!/bin/bash
# Run MapReduce on Hadoop cluster
# Usage: ./scripts/run_hadoop.sh [task2|task3|task4|task5]


TASK=${1:-task2}
USER_ID=${2:-$(whoami)}
INPUT="/data/chicago_crimes_sample.csv"   # Use chicago_crimes.csv for final run
OUTPUT="/user/$USER_ID/project/m1/$TASK"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

case $TASK in
    task2) MAPPER="src/mapper_task2.py" ;;
    task3) MAPPER="src/mapper_task3.py" ;;
    task4) MAPPER="src/mapper_task4.py" ;;
    task5) MAPPER="src/mapper_task5.py" ;;
    *)
        echo "Usage: $0 [task2|task3|task4|task5] [your_user_id]"
        echo "  task2: Crime Type Distribution"
        echo "  task3: Location Hotspots"
        echo "  task4: Crimes per Year"
        echo "  task5: Arrest vs No Arrest"
        exit 1
        ;;
esac

# Delete existing output (MapReduce won't overwrite)
echo "Removing existing output (if any)..."
hdfs dfs -rm -r "$OUTPUT" 2>/dev/null || true

MAPPER_FILE=$(basename "$MAPPER")
echo "Running MapReduce - Task: $TASK"
echo "Input: $INPUT"
echo "Output: $OUTPUT"
echo "---"

mapred streaming \
  -files "$PROJECT_DIR/$MAPPER,$PROJECT_DIR/src/reducer.py" \
  -mapper "python3 $MAPPER_FILE" \
  -reducer "python3 reducer.py" \
  -input "$INPUT" \
  -output "$OUTPUT"
