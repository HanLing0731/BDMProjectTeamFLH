# dags/delta_lake_data_pipeline.py
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

# ───── HOST paths (on the Docker HOST) ─────────────────────
HOST_TRUSTED  = os.environ['HOST_TRUSTED_ZONE_PATH']     # e.g. /home/you/project/trusted_zone
HOST_EXPLOIT  = os.environ['HOST_EXPLOIT_ZONE_PATH']     # e.g. /home/you/project/exploitation_zone
HOST_ML       = os.environ['HOST_ML_PATH']               # e.g. /home/you/project/ML
HOST_DATA     = os.environ['HOST_DATA_PATH']             # e.g. /home/you/project/data

# ───── CONTAINER paths (inside each Airflow/DockerOperator) ─
CNT_TRUSTED  = os.environ['TRUSTED_ZONE_PATH']           # e.g. /opt/airflow/trusted_zone
CNT_EXPLOIT  = os.environ['EXPLOIT_ZONE_PATH']           # e.g. /opt/airflow/exploitation_zone
CNT_ML       = os.environ['ML_PATH']                     # e.g. /opt/airflow/ML
CNT_DATA     = os.environ['DATA_PATH']                   # e.g. /data

with DAG(
    'delta_lake_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    concurrency=10,
    tags=['spark', 'delta_lake']
) as dag:

    # Task 1: ingest via API (no mounts needed)
    ingest_via_api = BashOperator(
        task_id='ingest_via_api',
        bash_command="""
            for d in students courses assessments vle studentVle student_course \
            studentRegistration studentAssessment healthcare_dataset_updated \
            students_mental_health_survey; do
              curl -fsS --retry 3 --retry-delay 5 \
                   -X POST "http://api:8000/ingest?dataset=${d}" || exit 1
            done
        """
    )

    # Task 2,3,4: spark exec via `docker exec spark …`
    process_profile_photos = BashOperator(
        task_id='process_profile_photos',
        bash_command="docker exec spark bash -c 'cd /app && python ingest_data.py --mode=photos'"
    )
    process_batch = BashOperator(
        task_id='process_batch_logs',
        bash_command="docker exec spark bash -c 'cd /app && python ingest_data.py --mode=batch'"
    )
    start_streaming = BashOperator(
        task_id='start_log_streaming',
        bash_command="docker exec spark bash -c 'cd /app && python ingest_data.py --mode=streaming'"
    )

    # Task 5: photos → MinIO
    photos_to_minio = DockerOperator(
        task_id='photos_to_minio',
        image='spark:local',
        mounts=[
            Mount(source=HOST_TRUSTED, target='/app/trusted_zone', type='bind'),
            Mount(source=HOST_DATA,    target='/data',               type='bind'),
        ],
        environment={
            'PHOTO_DIR': '/data/unstructured/profile_photos'
        },
        command='python /app/trusted_zone/photos_to_minio.py',
        network_mode='container:spark',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock'
    )

    # Task 6: transform & load → DuckDB
    transform_load_duckdb = DockerOperator(
        task_id='transform_load_duckdb',
        image='spark:local',
        mounts=[
            Mount(source=HOST_DATA,    target=CNT_DATA,    type='bind'),
            Mount(source=HOST_TRUSTED, target='/app/trusted_zone', type='bind'),
        ],
        environment={
            'DELTA_PATH':  f'{CNT_DATA}/delta',
            'DUCKDB_PATH': f'{CNT_DATA}/trusted_zone.duckdb',
        },
        command='python /app/trusted_zone/csv_to_duckdb.py',
        network_mode='container:spark',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock'
    )

    # Task 7: structured → exploitation zone
    structured_to_exploitation = DockerOperator(
        task_id='structured_data_to_exploitation',
        image='spark:local',
        mounts=[
            Mount(source=HOST_EXPLOIT, target='/app/exploitation_zone', type='bind'),
            Mount(source=HOST_DATA,    target=CNT_DATA,    type='bind'),
        ],
        environment={
            'EXPLOIT_DB': f'{CNT_DATA}/exploitation_zone.duckdb',
            'TRUSTED_DB': f'{CNT_DATA}/trusted_zone.duckdb',
        },
        command='python /app/exploitation_zone/duckdb_analytics.py',
        network_mode='container:spark',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock'
    )

    # Task 8: unstructured → exploitation
    unstructured_to_exploitation = DockerOperator(
        task_id='unstructured_data_to_exploitation',
        image='spark:local',
        mounts=[
            Mount(source=HOST_EXPLOIT, target='/app/exploitation_zone', type='bind'),
            Mount(source=HOST_DATA,    target=CNT_DATA,    type='bind'),
        ],
        environment={
            'PHOTO_DIR':           f'{CNT_DATA}/unstructured/profile_photos',
            'PROCESSED_PHOTO_DIR': f'{CNT_DATA}/structured/profile_photos',
            'PINECONE_API_KEY':    os.environ['PINECONE_API_KEY'],
            'PINECONE_ENV':        os.environ['PINECONE_ENV']
        },
        command='python /app/exploitation_zone/photo_processing.py',
        network_mode='container:spark',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock'
    )

    # Task 9: ML clustering
    ml_analysis = DockerOperator(
        task_id='ml_analysis',
        image='spark:local',
        mounts=[
            Mount(source=HOST_ML,   target='/app/ML',    type='bind'),
            Mount(source=HOST_DATA, target=CNT_DATA,  type='bind'),
        ],
        environment={
            'DUCKDB_PATH': f'{CNT_DATA}/exploitation_zone.duckdb'
        },
        command='python /app/ML/clustering.py',
        network_mode='container:spark',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock'
    )

    # Task 10: email on success
    send_notification = BashOperator(
        task_id='send_completion_notification',
        bash_command=(
            'echo "Pipeline completed successfully" '
            '| mail -s "Delta Lake Pipeline Status" admin@example.com'
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # ── Dependencies ────────────────────────────────────────────────────────
    ingest_via_api >> [process_batch, process_profile_photos]
    process_profile_photos >> photos_to_minio
    process_batch >> [transform_load_duckdb, start_streaming]
    photos_to_minio >> transform_load_duckdb
    transform_load_duckdb >> structured_to_exploitation
    structured_to_exploitation >> [ml_analysis, unstructured_to_exploitation]
    [ml_analysis, unstructured_to_exploitation, start_streaming] >> send_notification
