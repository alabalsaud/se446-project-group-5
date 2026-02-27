"""
Task 3: Location Hotspots
"""
import csv
import io
import sys

stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
for row in csv.reader(stdin):
    if not row or row[0] == 'ID':  # Skip header
        continue
    if len(row) > 7:
        location = row[7].strip() or 'UNKNOWN'
        print(f"{location}\t1")
