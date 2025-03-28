# BDMProjectTeamFLH

bdm_project/
├── docker-compose.yml          # Main compose file
├── data/                       # ALL LOCAL DATA GOES HERE
│   ├── structured/             # Batch CSVs (vle.csv, students.csv, etc.)
│   └── log_data/               # Streaming CSVs (clean_df_*.csv)
├── api/                        # FastAPI container
│   ├── Dockerfile              # API-specific Dockerfile
│   ├── main.py                 # API code
│   └──requirements.txt        # Python dependencies
└── spark/                      # Spark container
    ├── Dockerfile              # Spark-specific Dockerfile
    └── ingest_data.py          # Streaming ingestion code



[Host Machine]
 ↓ (volume mount)
[Container: API]
 ↓ (reads)
/data/structured/xxxx.csv
 ↓ (converts)
Delta Lake (/data/delta/xxxx)

[Host Machine]
 ↓ (volume mount)
[Container: Spark]
 ↓ (monitors)
/data/log_data/clean_df_*.csv
 ↓ (batch + streams)
Delta Lake (/data/delta/streaming_logs)


[Kaggle API]
 ↓ (via kagglehub download)
[Container: Spark]
 ↓ (organizes by student_id)
/data/unstructured/profile_photos/
   ├── student_001/
   │   ├── student_001_photo1.jpg
   │   └── student_001_photo2.jpg
   └── student_002/
       └── student_002_photo1.jpg
 ↓ (converts to binary + metadata)
Delta Lake (/data/delta/profile_photos)
   └── [Delta Table]
       ├── photo_id (string)
       ├── student_id (string)
       ├── photo_bytes (binary)
       └── ingestion_time (timestamp)



/data/delta/
├── profile_photos/            # Student profile photos (binary data)
├── batch_logs/                # Processed batch log files
├── streaming_logs/            # Streaming log data
├── _checkpoints/              # Streaming checkpoints
│   └── streaming/             # Streaming job metadata
├── students/                  # Student data table
├── courses/                   # Course data table
├── assessments/               # Assessment data table
├── studentVle/                # Student VLE interactions
├── studentRegistration/       # Student registration records
├── studentAssessment/         # Student assessment results
├── student_course/            # Student-course relationships
├── healthcare_dataset_updated/ # Health data
├── students_mental_health_survey/ # Mental health data
└── vle/                       # Virtual Learning Environment data