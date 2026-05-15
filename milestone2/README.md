# Milestone 2 — Chicago Crime Analytics (Spark + MLlib)

**Group 5** | SE446 | Spark DataFrames, Spark SQL, MLlib, YARN

---

## 1. Team members

| Name | Email | M2 focus |
|------|-------|----------|
| Alabalsaud | alabalsaud@alfaisal.edu | Tasks 9–11, `m2_spark_ml.py`, cluster scripts |
| Halmineefi | halmineefi@alfaisal.edu | Task 1, Task 6 |
| Mkalissa | Mkalissa@alfaisal.edu | Task 2 (Spark SQL) |
| Lalmousa | lalmousa@alfaisal.edu | Task 3 (trend + plot) |
| Lalbabtain | Lalbabtain@alfaisal.edu | Task 4, Task 7 |

---

## 2. Executive summary

We **reproduced** Milestone 1 analytics with **Spark** (DataFrames + SQL) on the same HDFS dataset (**793,073** rows after cleaning), then built an **MLlib** pipeline to predict **Arrest** (0/1). Three classifiers were compared: **Logistic Regression**, **Random Forest**, and **GBT**. **GBT** achieved the best AUC (~0.82) on a 5% cluster sample. Deployment was demonstrated in **local[*]**, **YARN client**, and **spark-submit cluster** modes.

---

## 3. Repository layout

```
milestone2/                          ← all M2 code and outputs live here
├── README.md
├── M2_Spark_ML_Group5.ipynb
├── m2_spark_ml.py
├── m2_cluster_outputs.py
├── scripts/run_m2_cluster.sh
├── src/
│   ├── mkalissa_task02_location_hotspots.py
│   ├── lalmousa_task03_yearly_trend.py
│   ├── lalbabtain_task04_arrest_analysis.py
│   └── lalbabtain_task07_rf_importance.py
└── output/
    ├── m2_real/
    ├── spark_submit/
    └── local_ml_run.log
```

Always `cd milestone2` before running scripts or the notebook.

---

## 4. Local setup

```bash
cd milestone2
pip install "pyspark>=3.5" numpy pandas matplotlib jupyter
java -version   # JDK 11 or 17 required
```

**Notebook:** `jupyter notebook M2_Spark_ML_Group5.ipynb`  
Set `M2_RUN_MODE=local` (10k synthetic rows) or `M2_RUN_MODE=yarn_client` on cluster.

**Task 9 smoke test:**
```bash
python3 m2_spark_ml.py --master 'local[*]' --synthetic-rows 10000
```

---

## 5. M1 vs M2 comparison (Tasks 1–4)

Spark on `hdfs:///data/chicago_crimes.csv` **matches MapReduce counts exactly**.

| Task | M1 (MapReduce) | M2 (Spark) | Match |
|------|----------------|------------|:-----:|
| 1 Top types | THEFT 162,688; BATTERY 151,930 | `output/m2_real/task01_crime_types_top10.csv` | Yes |
| 2 Locations | STREET 248,326; RESIDENCE 136,393 | `task02_location_hotspots_top10.csv` | Yes |
| 3 By year | 2001: 467,301; 2023: 81,461 | `task03_crimes_per_year.csv` | Yes |
| 4 Arrest rate | 27.98% (221,932 / 793,072) | 27.98% (`task04_arrest_overall.csv`) | Yes |

**Speed / ease:** Spark Phase A (~47s load + aggregations, client mode) vs ~1–2 min per MapReduce job with separate mapper/reducer files. Spark needed fewer artifacts and was easier to iterate.

---

## 6. Phase A — Spark analytics (Tasks 1–4)

| Task | Method | Output file |
|------|--------|-------------|
| 1 | `groupBy("Primary Type").count()` | `task01_crime_types_top10.csv` |
| 2 | `spark.sql()` on view `crimes` | `task02_location_hotspots_top10.csv` |
| 3 | `groupBy("Year").count()` + matplotlib (local) | `task03_crimes_per_year.csv` |
| 4 | Overall + per-type arrest rate | `task04_*.csv` |

**Task 4 highlights:** NARCOTICS arrest rate ~**99.9%**; THEFT ~**14.2%**; BURGLARY ~**6.7%**.

---

## 7. Phase B — ML pipeline (Tasks 5–7)

**Features:** `District`, `crime_index` (Primary Type), `Hour`, `domestic_index`  
**Split:** 80/20, `seed=42` | **Cluster training sample:** 5% of rows (`sample-fraction 0.05`)

### Task 6 — Model comparison (cluster sample)

| Model | AUC-ROC | Accuracy | F1 | Train (s) |
|-------|--------:|---------:|---:|----------:|
| Logistic Regression | 0.613 | 0.721 | 0.625 | 24 |
| Random Forest | 0.805 | 0.809 | 0.772 | 33 |
| **GBT** | **0.824** | **0.845** | **0.829** | 89 (spark-submit) |

See `output/m2_real/task06_model_comparison.csv` and `task06_confusion_*.json`.

### Task 7 — Feature importance (Random Forest)

| Feature | Importance |
|---------|----------:|
| crime_index | 0.973 |
| Hour | 0.012 |
| domestic_index | 0.011 |
| District | 0.004 |

**Interpretation:** Crime type drives arrests, consistent with Task 4. **LR underperforms** because arrest probability is highly non-linear in type; trees model splits on type without a linear assumption.

**Best model:** **GBT** (highest AUC/F1 on test split).

---

## 8. Deployment (Tasks 9–11)

| Task | Mode | Evidence |
|------|------|----------|
| 9 | `local[*]`, 10k rows | `output/local_ml_run.log` (`Master: local[*]`) |
| 10 | YARN client, full HDFS | `output/m2_real/RUN_META.json` — **793,073** rows, `Master: yarn` |
| 11 | `spark-submit --deploy-mode cluster` | `output/spark_submit/run.log`, `submit_transcript.txt` |

**Task 11 cluster command** (run from `milestone2/` on cluster):
```bash
source /etc/profile.d/hadoop.sh
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
cd ~/se446-project-group-5/milestone2
./scripts/run_m2_cluster.sh spark_submit_ml
# Logs: yarn logs -applicationId <appId>
```

Application **application_1778738889964_0005** — **SUCCEEDED**.

---

## 9. Member contributions (M2) & Git branches

| Member | Branch (example) | Files |
|--------|------------------|-------|
| Alabalsaud | `task9-11-alabalsaud` | `m2_spark_ml.py`, `scripts/run_m2_cluster.sh`, `output/spark_submit/` |
| Halmineefi | `task1-halmineefi` | `M2_Spark_ML_Group5.ipynb`, task01 + task06 outputs |
| Mkalissa | `task2-mkalissa` | `src/mkalissa_task02_*.py`, task02 CSV |
| Lalmousa | `task3-lalmousa` | `src/lalmousa_task03_*.py`, task03 CSV |
| Lalbabtain | `task4-7-lalbabtain` | `src/lalbabtain_task04_*.py`, task04/07 CSVs |

Each member should push from **their own GitHub account** (AssessX grades individual commit history).

---

## 10. Deliverables checklist (M2)

- [x] `M2_Spark_ML_Group5.ipynb` — Tasks 1–7
- [x] `m2_spark_ml.py` — spark-submit (Tasks 5–7)
- [x] This README (summary, M1 vs M2, ML results, deployment evidence)
- [x] `output/m2_real/` — cluster CSV/JSON per task
- [x] `output/spark_submit/run.log` — Task 11 transcript + YARN excerpt
