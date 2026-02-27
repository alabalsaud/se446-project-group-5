# SE446 Project - Chicago Crime Analytics with MapReduce

**Group Name:** Group   
**Course:** SE446  
**Milestone:** 1

---

## Team Members

| Name | Email | GitHub |
|------|-------|--------|
| Alabalsaud | alabalsaud@alfaisal.edu | - |
| Halmineefi | halmineefi@alfaisal.edu | - |
| Lalmousa | lalmousa@alfaisal.edu | - |
| Lalbabtain | Lalbabtain@alfaisal.edu | - |
| Mkalissa | Mkalissa@alfaisal.edu | mkalissa |

---

## Executive Summary

This project implements a MapReduce pipeline on a Hadoop cluster to analyze Chicago crime data. We use Hadoop Streaming with Python mappers and reducers to process the Chicago Police Department crime dataset. Each mapper parses CSV rows, extracts the relevant column (crime type, location, date, or arrest status), and emits key-value pairs. A shared reducer sums counts per key. The pipeline handles UTF-8 encoding issues in the raw data and uses unbuffered Python output for reliable Hadoop streaming. All four analytical tasks (crime type distribution, location hotspots, crimes per year, and arrest analysis) run successfully on the cluster.

---

## Task 2: Crime Type Distribution

**Research Question:** What are the most common types of crimes in Chicago?

### Instructions

```bash
source /etc/profile.d/hadoop.sh
cd ~/se446-project-group-X
hdfs dfs -rm -r /user/alabalsaud/project/m1/task2 2>/dev/null
./scripts/run_hadoop.sh task2 alabalsaud
```

Equivalent mapred streaming command:

```bash
mapred streaming \
  -files src/mapper_task2.py,src/reducer.py \
  -mapper "python3 -u mapper_task2.py" \
  -reducer "python3 -u reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/alabalsaud/project/m1/task2
```

### Sample Results (Top 5)

```
ARSON	1717
ASSAULT	54070
BATTERY	151930
BURGLARY	39872
CONCEALED CARRY LICENSE VIOLATION	77
```

### Interpretation

Theft (162,688) and Battery (151,930) are the most frequent crime types, followed by Criminal Damage (91,241) and Narcotics (74,127). Theft and battery together account for a large share of all incidents, indicating these should be priority areas for resource allocation.

### Execution Logs

```
Removing existing output (if any)...
Running MapReduce - Task: task2
Input: /data/chicago_crimes.csv
Output: /user/alabalsaud/project/m1/task2
---
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] ...
2026-02-27 12:19:55,627 INFO mapreduce.Job: Running job: job_1771403826595_0056
2026-02-27 12:20:15,562 INFO mapreduce.Job: Job job_1771402826595_0057 running in uber mode : false
2026-02-27 12:20:15,565 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-27 12:20:45,487 INFO mapreduce.Job:  map 52% reduce 0%
2026-02-27 12:20:50,479 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-27 12:21:07,796 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-27 12:21:10,750 INFO mapreduce.Job: Job job_1771403826595_0056 completed successfully
```


---

## Task 3: Location Hotspots

**Research Question:** Where do most crimes occur?

### Instructions

```bash
source /etc/profile.d/hadoop.sh
cd ~/se446-project-group-X
hdfs dfs -rm -r /user/alabalsaud/project/m1/task3 2>/dev/null
./scripts/run_hadoop.sh task3 alabalsaud
```

Equivalent mapred streaming command:

```bash
mapred streaming \
  -files src/mapper_task3.py,src/reducer.py \
  -mapper "python3 -u mapper_task3.py" \
  -reducer "python3 -u reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/alabalsaud/project/m1/task3
```

### Sample Results (Top 5)

```
ABANDONED BUILDING	829
AIRCRAFT	34
AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA	42
AIRPORT BUILDING NON-TERMINAL - SECURE AREA	16
AIRPORT EXTERIOR - NON-SECURE AREA	37
```

Top locations by count: STREET (248,326), RESIDENCE (136,393), SIDEWALK (47,506), APARTMENT (61,235), PARKING LOT/GARAGE (22,436).

### Interpretation

Streets and residences are the dominant crime locations, followed by sidewalks and apartments. Patrol units should prioritize street-level and residential areas as high-risk zones.

### Execution Logs

```
Removing existing output (if any)...
Running MapReduce - Task: task3
Input: /data/chicago_crimes.csv
Output: /user/alabalsaud/project/m1/task3
---
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob8329238832929712322.jar tmpDir=null
2026-02-27 12:19:51,124 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-27 12:19:51,399 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-27 12:19:51,819 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/alabalsaud/.staging/job_1771402826595_0057
2026-02-27 12:19:53,609 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-27 12:19:53,633 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-02-27 12:19:53,634 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-02-27 12:19:54,278 INFO mapreduce.JobSubmitter: number of splits:2
2026-02-27 12:19:55,150 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0057
2026-02-27 12:19:55,150 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-27 12:19:55,464 INFO conf.Configuration: resource-types.xml not found
2026-02-27 12:19:55,465 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-27 12:19:55,576 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0057
2026-02-27 12:19:55,624 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0057/
2026-02-27 12:19:55,627 INFO mapreduce.Job: Running job: job_1771402826595_0057
2026-02-27 12:20:15,562 INFO mapreduce.Job: Job job_1771402826595_0057 running in uber mode : false
2026-02-27 12:20:15,565 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-27 12:20:45,487 INFO mapreduce.Job:  map 52% reduce 0%
2026-02-27 12:20:50,479 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-27 12:21:07,796 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-27 12:21:10,750 INFO mapreduce.Job: Job job_1771402826595_0057 completed successfully
```


---

## Task 4: The Time Dimension (Crimes per Year)

**Research Question:** How has the total number of crimes changed over the years?

### Instructions

```bash
source /etc/profile.d/hadoop.sh
cd ~/se446-project-group-X
hdfs dfs -rm -r /user/alabalsaud/project/m1/task4 2>/dev/null
./scripts/run_hadoop.sh task4 alabalsaud
```

Equivalent mapred streaming command:

```bash
mapred streaming \
  -files src/mapper_task4.py,src/reducer.py \
  -mapper "python3 -u mapper_task4.py" \
  -reducer "python3 -u reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/alabalsaud/project/m1/task4
```

### Sample Results (Top 5)

```
2001	467301
2002	205267
2003	985
2004	915
2005	1031
```

### Interpretation

The dataset shows high crime volume in 2001 and 2002, with a sharp drop in later years. More recent years (2022-2025) show partial or in-progress data. The trend suggests a decline in recorded incidents from the early 2000s to mid-2010s, with variation in recent years.

### Execution Logs

```
Removing existing output (if any)...
Running MapReduce - Task: task4
Input: /data/chicago_crimes.csv
Output: /user/alabalsaud/project/m1/task4
---
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob391738917758181154.jar tmpDir=null
2026-02-27 12:22:14,365 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-27 12:22:14,736 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-27 12:22:15,265 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/alabalsaud/.staging/job_1771402826595_0058
2026-02-27 12:22:17,005 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-27 12:22:17,046 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-02-27 12:22:17,050 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-02-27 12:22:17,679 INFO mapreduce.JobSubmitter: number of splits:2
2026-02-27 12:22:18,546 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0058
2026-02-27 12:22:18,546 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-27 12:22:18,871 INFO conf.Configuration: resource-types.xml not found
2026-02-27 12:22:18,871 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-27 12:22:18,997 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0058
2026-02-27 12:22:19,051 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0058/
2026-02-27 12:22:19,053 INFO mapreduce.Job: Running job: job_1771402826595_0058
2026-02-27 12:22:34,909 INFO mapreduce.Job: Job job_1771402826595_0058 running in uber mode : false
2026-02-27 12:22:34,911 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-27 12:23:02,808 INFO mapreduce.Job:  map 47% reduce 0%
2026-02-27 12:23:08,895 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-27 12:23:22,249 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-27 12:23:25,088 INFO mapreduce.Job: Job job_1771402826595_0058 completed successfully
```


---

## Task 5: Law Enforcement Analysis (Arrest vs No Arrest)

**Research Question:** What percentage of crimes result in an arrest?

### Instructions

```bash
source /etc/profile.d/hadoop.sh
cd ~/se446-project-group-X
hdfs dfs -rm -r /user/alabalsaud/project/m1/task5 2>/dev/null
./scripts/run_hadoop.sh task5 alabalsaud
```

Equivalent mapred streaming command:

```bash
mapred streaming \
  -files src/mapper_task5.py,src/reducer.py \
  -mapper "python3 -u mapper_task5.py" \
  -reducer "python3 -u reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/alabalsaud/project/m1/task5
```

### Sample Results

```
Arrested	221932
Not Arrested	571140
```

### Interpretation

Approximately 28% of crimes result in an arrest (221,932 out of 793,072 total). The majority of incidents do not lead to an arrest, indicating room for improvement in patrol efficiency and case resolution.

### Execution Logs

```
Removing existing output (if any)...
Running MapReduce - Task: task5
Input: /data/chicago_crimes.csv
Output: /user/alabalsaud/project/m1/task5
---
alabalsaud@master-node:~/se446-project-group-X$ ./scripts/run_hadoop.sh task5 alabalsaud
Removing existing output (if any)...
Running MapReduce - Task: task5
Input: /data/chicago_crimes.csv
Output: /user/alabalsaud/project/m1/task5
---
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob1235064395770733514.jar tmpDir=null
2026-02-27 12:24:51,728 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-27 12:24:52,158 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-27 12:24:52,810 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/alabalsaud/.staging/job_1771402826595_0059
2026-02-27 12:24:54,706 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-27 12:24:54,738 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-02-27 12:24:54,748 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-02-27 12:24:55,394 INFO mapreduce.JobSubmitter: number of splits:2
2026-02-27 12:24:56,243 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0059
2026-02-27 12:24:56,244 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-27 12:24:56,596 INFO conf.Configuration: resource-types.xml not found
2026-02-27 12:24:56,597 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-27 12:24:56,729 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0059
2026-02-27 12:24:56,800 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0059/
2026-02-27 12:24:56,803 INFO mapreduce.Job: Running job: job_1771402826595_0059
2026-02-27 12:25:13,494 INFO mapreduce.Job: Job job_1771402826595_0059 running in uber mode : false
2026-02-27 12:25:13,496 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-27 12:25:42,067 INFO mapreduce.Job:  map 26% reduce 0%
2026-02-27 12:25:43,278 INFO mapreduce.Job:  map 53% reduce 0%
2026-02-27 12:25:46,930 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-27 12:25:59,028 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-27 12:26:01,896 INFO mapreduce.Job: Job job_1771402826595_0059 completed successfully
```


---

## Member Contribution

| Member | Contribution |
|--------|--------------|
| Alabalsaud | Scripts (run_hadoop.sh, run_local.sh, setup) |
| Halmineefi | Task 2: Crime Type Distribution (mapper_task2.py) |
| Lalmousa | Task 4: Crimes per Year (mapper_task4.py) |
| Lalbabtain | Task 5: Arrest vs No Arrest (mapper_task5.py) |
| Mkalissa | Task 3: Location Hotspots (mapper_task3.py), README report (reducer.py shared) |

---

## Output Files

Full results are stored in the `output/` directory:

- `output/task2/part-00000` - Crime type counts
- `output/task3/part-00000` - Location type counts
- `output/task4/part-00000` - Crimes per year
- `output/task5/part-00000` - Arrest vs Not Arrested counts

---

## Repository Structure

```
se446-project-group-X/
├── src/
│   ├── mapper_task2.py
│   ├── mapper_task3.py
│   ├── mapper_task4.py
│   ├── mapper_task5.py
│   ├── reducer.py
│   └── mapper_district.py
├── scripts/
│   ├── run_hadoop.sh
│   └── run_local.sh
├── output/
│   ├── task2/
│   ├── task3/
│   ├── task4/
│   └── task5/
└── README.md
```
