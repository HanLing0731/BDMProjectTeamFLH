# BDMProjectTeamFLH

## Overview
**BDMProjectTeamFLH** is a data engineering pipeline that ingests, processes, and analyzes multi-modal student data to predict dropout risk and identify at-risk cohorts.  The system combines:

- **Delta Lake** (for unified batch & streaming storage)  
- **Apache Spark** (batch, streaming, and photo ingestion)  
- **FastAPI** (for structured-data ingestion endpoints)  
- **Apache Airflow** (for orchestration)  
- **DuckDB** (as trusted/exploitation-zone analytics stores)  
- **Neo4j + GDS** (for graph-based feature extraction and analytics)  
- **Scikit-learn** (for downstream clustering)  

This end-to-end PoC runs entirely in Docker Compose, enabling seamless development, testing, and deployment.

## File Structure
```
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
│   ├── photo_processing.py               # unstructured → Pinecone
│   └── populate_graph_from_duckdb.py     # populate neo4j
├── ML/
│   ├── analyse_graph_cypher.py           # graph analytics code
│   └── clustering.py                     # reads exploitation DB, does K-Means, writes predictions
├── data/
│   ├── delta/                            # Delta Lake tables (ingested + streaming)
│   ├── log_data/                         # raw logs, or spark checkpoints
│   ├── structured/                       # csv files
│   └── unstructured/
│       └── profile_photos/               # raw photo ingests
├── docker-compose.yml                    # brings up api, spark, redis, postgres, airflow, minio, etc.
└──requirements.txt                       # for local dev

```

---

## Data Processing Workflow

1. **Batch Ingestion (Task 1: `ingest_via_api`)**  
   The FastAPI container reads CSV files from `data/structured/` and POSTs each dataset to the `/ingest` endpoint, which writes Delta Lake tables under `data/delta/` (students, courses, assessments, etc.).

2. **Profile Photo Staging (Task 2: `process_profile_photos`)**  
   Inside the Spark container, `ingest_data.py --mode=photos` downloads student profile photos and stages them in `data/delta/profile_photos/` for downstream processing.

3. **Batch Log Processing (Task 3: `process_batch_logs`)**  
   Spark runs `ingest_data.py --mode=batch` to consume historical log CSVs and append them to `data/delta/batch_logs/`.

4. **Streaming Log Ingestion (Task 4: `start_streaming`)**  
   The Spark streaming job (`ingest_data.py --mode=streaming`) tails new files in `data/log_data/` and writes enriched records into `data/delta/streaming_logs/` in real time.

5. **Photo Upload to MinIO (Task 5: `photos_to_minio`)**  
   A DockerOperator mounts the trusted-zone directory and runs `photos_to_minio.py` to validate and upload staged photos from `/data/unstructured/profile_photos` into MinIO buckets.

6. **Transform & Load to DuckDB (Task 6: `transform_load_duckdb`)**  
   The `csv_to_duckdb.py` script reads cleaned Delta tables under `/data/delta/`, writes each to Parquet, and ingests them into `trusted_zone.duckdb` via DuckDB’s `parquet_scan`.

7. **Build Exploitation-Zone Analytics (Task 7: `structured_data_to_exploitation`)**  
   `duckdb_analytics.py` attaches `trusted_zone.duckdb`, generates dimension and fact tables (e.g., `student_dim`, `course_dim`, etc.), and writes them into `exploitation_zone.duckdb`.

8. **Unstructured Photo Embeddings (Task 8: `unstructured_to_exploitation`)**  
   Profile photos in DuckDB are processed by `photo_processing.py` to extract image embeddings and store them (via Pinecone) alongside structured data for ML.

9. **ML Clustering (Task 9: `ml_analysis`)**  
   The clustering script loads `exploitation_zone.duckdb`, builds a student-risk feature matrix, runs silhouette-guided K-means in Python, and writes risk labels back into the DuckDB for dashboards.

10. **Populate Neo4j Property Graph (Task 10: `populate_graph`)**  
    `populate_graph_from_duckdb.py` reads all DuckDB tables, creates Neo4j nodes (Student, Course, Assessment, MentalHealthSurvey, HealthData) and relationships (ENROLLED_IN, TOOK, BELONGS_TO, SURVEYED, ADMITTED) to form the property graph.

11. **Graph Analytics & Downstream ML (Task 11: `graph_analytics`)**  
    The Neo4j GDS plugin projects the mixed-type graph, computes Node2Vec embeddings, PageRank, and Louvain communities, then extracts embeddings into Pandas, selects an optimal k via silhouette score, runs final K-means, and writes a `risk_cohort` property back onto each Student node.

12. **Completion Notification (Task 12: `send_completion_notification`)**  
    Upon successful completion of all upstream tasks, an email notification is sent to the data-ops team.


---

## Running the Project

### 1. Clone or download the project

### 2. Open the project folder in the terminal and clean the environment
```sh
docker compose down -v
```

### 3. Start the Containers
Run the following command to start the services:
```sh
docker compose up -d
```
This will launch the container stack, which includes:
- FastAPI for structured data ingestion
- Spark for batch and streaming data processing

### 3. Running Airflow Tasks
Once the containers are up, access the Airflow web UI. The first time it will require the usual admin admin for username and password.
```sh
http://localhost:8080
```
To execute tasks:
1. Navigate to the DAGs page
2. Enable and trigger the DAGs
3. Monitor logs and confirm task completion
   
### 4. To Check the result of Machine learning task
```sh
python test.py
```

## Stopping the Project
To stop and remove all running containers:
```sh
docker compose down -v
```


## Contributors
- Filipe Albuquerque Ito Russo
- HanLing Hu
- Lucia Victoria Fernandez Sanchez 


---
This project is part of the BDMA curriculum for BDM coursework.

