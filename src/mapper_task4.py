"""
Task 4: The Time Dimension - Crimes per Year
Parses Date (index 2) to extract year. Format: MM/DD/YYYY HH:MM:SS AM/PM
"""
import csv
import io
import sys

stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
for row in csv.reader(stdin):
    if not row or row[0] == 'ID':  # Skip header
        continue
    if len(row) > 2:
        date_str = row[2].strip()
        if not date_str:
            continue
        # Split by space, take date part (MM/DD/YYYY), then split by / for year
        parts = date_str.split()
        if parts:
            date_part = parts[0]
            year_parts = date_part.split('/')
            if len(year_parts) == 3:
                year = year_parts[2]
                print(f"{year}\t1")
