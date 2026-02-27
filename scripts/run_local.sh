#!/bin/bash
# Run MapReduce locally (no Hadoop needed)
# Usage: ./scripts/run_local.sh [task2|task3|task4|task5] [input.csv]

TASK=${1:-task2}
INPUT=${2:-chicago_crimes_sample.csv}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

case $TASK in
    task2) MAPPER="src/mapper_task2.py" ;;
    task3) MAPPER="src/mapper_task3.py" ;;
    task4) MAPPER="src/mapper_task4.py" ;;
    task5) MAPPER="src/mapper_task5.py" ;;
    *)
        echo "Usage: $0 [task2|task3|task4|task5] [input.csv]"
        echo "  task2: Crime Type Distribution"
        echo "  task3: Location Hotspots"
        echo "  task4: Crimes per Year"
        echo "  task5: Arrest vs No Arrest"
        exit 1
        ;;
esac

if [[ ! -f "$INPUT" ]]; then
    echo "Error: Input file '$INPUT' not found"
    exit 1
fi

echo "Running Task: $TASK"
echo "Input: $INPUT"
echo "---"
cat "$INPUT" | python3 "$MAPPER" | sort | python3 src/reducer.py
