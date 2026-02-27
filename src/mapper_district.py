#!/usr/bin/env python3
"""
Lab 02 Base: Arrests by District
From 02_intermediate_mapreduce_lab.md — filters for Arrest=true, groups by District.
Schema: Arrest=index 8, District=index 11
"""
import sys

ARREST_IDX = 8
DISTRICT_IDX = 11

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split(',')
    if len(parts) <= DISTRICT_IDX:
        continue
    arrest_status = parts[ARREST_IDX].lower()
    district = parts[DISTRICT_IDX]
    if arrest_status == 'true':
        print(f"{district}\t1")
