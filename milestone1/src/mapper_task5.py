"""
Task 5: Law Enforcement Analysis - Arrest vs No Arrest
Outputs: Arrest status (index 8) -> 1
"""
import csv
import io
import sys

stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
for row in csv.reader(stdin):
    if not row or row[0] == 'ID':  # Skip header
        continue
    if len(row) > 8:
        arrest = row[8].strip().lower()
        if arrest in ('true', 'false'):
            status = 'Arrested' if arrest == 'true' else 'Not Arrested'
            print(f"{status}\t1")
