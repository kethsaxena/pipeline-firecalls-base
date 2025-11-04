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
## To-Do Tracker

| Task | Status | Owner | Due Date | Notes |
|------|---------|--------|----------|-------|
| Setup S3 Bronze Layer | ✅ Done | PS | Oct 20 | Working fine in staging |
| Create Delta Table for Silver Layer | 🚧 In Progress | PS | Nov 5 | Schema defined in `schema.csv` |
| Add Airflow DAG for Daily Load | ⏳ Planned | PS | Nov 10 | Use `apache/airflow:2.9.1-python3.9` |
| QA and Optimize Runtime | ⏳ Planned | PS | Nov 15 | Reduce runtime from 8min to <3min |
