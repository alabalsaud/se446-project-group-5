# Milestone 1 — Chicago Crime Analytics (MapReduce)

**Group 5** | SE446 | Hadoop Streaming + HDFS cluster

---

## 1. Team members

| Name | Email |
|------|-------|
| Alabalsaud | alabalsaud@alfaisal.edu |
| Halmineefi | halmineefi@alfaisal.edu |
| Lalmousa | lalmousa@alfaisal.edu |
| Lalbabtain | Lalbabtain@alfaisal.edu |
| Mkalissa | Mkalissa@alfaisal.edu |

---

## 2. Executive summary

We built a **MapReduce** pipeline on the course Hadoop cluster to analyze Chicago Police crime records. Python **mappers** parse `chicago_crimes.csv`, emit key–value pairs, and a shared **reducer** sums counts. Four jobs answer: top crime types, location hotspots, crimes per year, and arrest vs no-arrest. All jobs completed on the cluster; results are under `output/`.

---

## 3. Repository layout

```
milestone1/
├── README.md           ← this report
├── setup-hadoop.sh
├── hadoop-config/      ← cluster XML snippets
├── src/                ← mappers + reducers
├── scripts/
│   ├── run_hadoop.sh   ← run on cluster
│   └── run_local.sh    ← local test
└── output/
    ├── task2/          ← crime types
    ├── task3/          ← locations
    ├── task4/          ← years
    └── task5/          ← arrests
```

---

## 4. How to run (cluster)

```bash
ssh YOUR_USER@134.209.172.50
source /etc/profile.d/hadoop.sh
cd ~/se446-project-group-5/milestone1

hdfs dfs -rm -r /user/YOUR_USER/project/m1/task2 2>/dev/null
./scripts/run_hadoop.sh task2 YOUR_USER
# task3, task4, task5 similarly
```

Input on cluster: `/data/chicago_crimes.csv` (edit `INPUT` in `scripts/run_hadoop.sh` if using sample file).

---

## 5. Tasks and results

### Task 2 — Crime type distribution (Halmineefi)

**Question:** What are the most common crime types?

| Primary Type | Count |
|--------------|------:|
| THEFT | 162,688 |
| BATTERY | 151,930 |
| CRIMINAL DAMAGE | 91,241 |
| NARCOTICS | 74,127 |
| ASSAULT | 54,070 |

**Interpretation:** Theft and battery dominate; prioritize prevention and patrol for these categories.

Output: `output/task2/part-00000`

---

### Task 3 — Location hotspots (Mkalissa)

**Question:** Where do crimes occur most?

| Location | Count |
|----------|------:|
| STREET | 248,326 |
| RESIDENCE | 136,393 |
| APARTMENT | 61,235 |
| SIDEWALK | 47,506 |
| PARKING LOT/GARAGE | 22,436 |

**Interpretation:** Streets and residences are the highest-risk zones for patrol allocation.

Output: `output/task3/part-00000`

---

### Task 4 — Crimes per year (Lalmousa)

**Question:** How has crime volume changed over time?

| Year | Count |
|------|------:|
| 2001 | 467,301 |
| 2002 | 205,267 |
| 2023 | 81,461 |

**Interpretation:** Very high counts in 2001–2002; later years show lower reporting volume (data collection / filtering effects).

Output: `output/task4/part-00000`

---

### Task 5 — Arrest analysis (Lalbabtain)

**Question:** What share of crimes lead to an arrest?

| Outcome | Count |
|---------|------:|
| Arrested | 221,932 |
| Not Arrested | 571,140 |
| **Rate** | **~27.98%** |

**Interpretation:** Most incidents do not result in arrest; large room to study factors that improve arrest outcomes (addressed further in Milestone 2 ML).

Output: `output/task5/part-00000`

---

## 6. Member contributions (M1)

| Member | Contribution |
|--------|--------------|
| Alabalsaud | `run_hadoop.sh`, `run_local.sh`, `setup-hadoop.sh` |
| Halmineefi | `mapper_task2.py` (crime types) |
| Mkalissa | `mapper_task3.py`, `reducer.py` |
| Lalmousa | `mapper_task4.py` (years) |
| Lalbabtain | `mapper_task5.py` (arrest) |

---

## 7. Deliverables checklist (M1)

- [x] Python mappers/reducers in `src/`
- [x] Cluster run scripts in `scripts/`
- [x] HDFS output copies in `output/task2` … `task5`
- [x] This README (task questions, results, interpretation, contributions)
