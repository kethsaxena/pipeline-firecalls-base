# 🚒 Fire Department Big Data Pipeline

## Components
1. [python] GDrive Puller 
1. [processor]
    1. Spark Processor
1. [RDBMS] 

## Steps to Run
```
 python gPull
```

```
 python runner.py
```
## ✅ To-Do Tracker

| Task | Status | Owner | Due Date | Notes |
|------|---------|--------|----------|-------|
| Setup S3 Bronze Layer | ✅ Done | PS | Oct 20 | Working fine in staging |
| Create Delta Table for Silver Layer | 🚧 In Progress | PS | Nov 5 | Schema defined in `schema.csv` |
| Add Airflow DAG for Daily Load | ⏳ Planned | PS | Nov 10 | Use `apache/airflow:2.9.1-python3.9` |
| QA and Optimize Runtime | ⏳ Planned | PS | Nov 15 | Reduce runtime from 8min to <3min |


```markdown
```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title Flight Data Pipeline Plan
    section Setup
    Bronze Layer        :done, a1, 2025-10-15, 2025-10-20
    Silver Layer        :active, a2, 2025-10-21, 2025-11-05
    Airflow DAG         :a3, 2025-11-06, 2025-11-10
    QA + Optimization   :a4, 2025-11-11, 2025-11-15
