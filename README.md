# SE446 — Chicago Crime Analytics (Group 5)

**Course:** SE446 Big Data Engineering  
**Repository:** [alabalsaud/se446-project-group-5](https://github.com/alabalsaud/se446-project-group-5)

---

## Team Members

| Name | Email |
|------|-------|
| Alabalsaud | alabalsaud@alfaisal.edu |
| Halmineefi | halmineefi@alfaisal.edu |
| Lalmousa | lalmousa@alfaisal.edu |
| Lalbabtain | Lalbabtain@alfaisal.edu |
| Mkalissa | Mkalissa@alfaisal.edu |

---

## Repository structure

```
se446-project-group-5/
├── README.md                 ← this file (project index)
├── milestone1/               ← MapReduce (M1) — full report inside
│   ├── README.md
│   ├── src/                  ← mappers, reducers
│   ├── scripts/              ← run_hadoop.sh, run_local.sh
│   ├── hadoop-config/
│   ├── setup-hadoop.sh
│   └── output/               ← task2–task5 cluster results
└── milestone2/               ← Spark + MLlib (M2) — full report inside
    ├── README.md
    ├── M2_Spark_ML_Group5.ipynb
    ├── m2_spark_ml.py
    ├── m2_cluster_outputs.py
    ├── src/                  ← per-member task scripts
    ├── scripts/              ← run_m2_cluster.sh
    └── output/               ← m2_real/, spark_submit/, local logs
```

**Read the milestone reports:**

- **M1:** [milestone1/README.md](milestone1/README.md)
- **M2:** [milestone2/README.md](milestone2/README.md)

---

## Quick start

**Milestone 1 (cluster):**
```bash
cd milestone1
./scripts/run_hadoop.sh task2 YOUR_HDFS_USER
```

**Milestone 2 (local smoke test):**
```bash
cd milestone2
pip install "pyspark>=3.5" numpy pandas matplotlib jupyter
python3 m2_spark_ml.py --master 'local[*]' --synthetic-rows 10000
```

**Milestone 2 (cluster):**
```bash
cd milestone2
./scripts/run_m2_cluster.sh full
```

---

## Submission

Submit **this repository link** on AssessX. Ensure instructor **akoubaa** is a collaborator.
