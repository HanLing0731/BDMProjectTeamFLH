# BDMProjectTeamFLH
 
BDMProjectTeamFLH/
├── dags/
│   └── data_pipeline.py                  # Airflow DAG definition
├── api/
│   ├── Dockerfile
│   └── main.py                           # FastAPI ingest script
├── spark/
│   ├── Dockerfile
│   └── ingest_data.py                    # spark ingestion scripts
├── trusted_zone/
│   ├── csv_to_duckdb.py                  # transforms & loads Delta → DuckDB
│   └── photos_to_minio.py                # validates & uploads photos to MinIO
├── exploitation_zone/
│   ├── duckdb_analytics.py               # builds analytics tables in DuckDB
│   └── photo_processing.py               # unstructured → Pinecone
├── ML/
│   └── clustering.py                     # reads exploitation DB, does K-Means, writes predictions
├── data/
│   ├── delta/                            # Delta Lake tables (ingested + streaming)
│   ├── log_data/                         # raw logs, or spark checkpoints
│   ├── structured/                       # csv files
│   └── unstructured/
│       └── profile_photos/               # raw photo ingests
├── docker-compose.yml                    # brings up api, spark, redis, postgres, airflow, minio, etc.
├── requirements.txt                      # for local dev
└── .env (optional)                       # environment variable overrides
