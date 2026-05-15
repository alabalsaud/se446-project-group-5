"""
Task 2: Crime Type Distribution
Outputs: Primary Type (index 5) -> 1
"""
import csv
import io
import sys

stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
for row in csv.reader(stdin):
    if not row or row[0] == 'ID':  # Skip header
        continue
    if len(row) > 5:
        crime_type = row[5].strip() or 'UNKNOWN'
        print(f"{crime_type}\t1")
