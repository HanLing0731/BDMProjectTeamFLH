from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount
import requests

# Before running, ensure the local data directory is changed to the correct path
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def check_streaming_health():
    """Custom health check for streaming job"""
    import requests
    try:
        response = requests.get("http://spark:4040/api/v1/applications", timeout=10)
        if response.status_code == 200:
            print("Streaming job is running")
            return True
    except Exception as e:
        print(f"Streaming health check failed: {str(e)}")
    return False

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

    ingest_via_api = DockerOperator(
        task_id='ingest_via_api',
        image='bdmprojectteamflh-api:latest',
        command=[
            'sh', '-c',
            'for d in students courses assessments vle studentVle student_course studentRegistration studentAssessment healthcare_dataset_updated students_mental_health_survey; do '
            'curl -f -X POST "http://localhost:8000/ingest?dataset=${d}" || exit 1; '
            'done'
        ],
        entrypoint="", 
        mount_tmp_dir=False,
        network_mode='host',
        mounts=[
            Mount(source='/Users/huuuuhuuuu/BDMProjectTeamFLH/data', 
                target='/data', 
                type='bind')
        ],
        docker_url="unix://var/run/docker.sock",
        auto_remove=True
    )
        
    # Task 2: Process photos
    process_photos = DockerOperator(
        task_id='process_profile_photos',
        image='bdmprojectteamflh-spark:latest',
        api_version='auto',
        auto_remove=False, 
        docker_url="unix:///var/run/docker.sock",
        network_mode='host',
        command=[
        "/bin/bash", "-c",
        "cd /app && /opt/bitnami/python/bin/python ingest_data.py --mode=photos || exit 1"
    ],
        entrypoint="", 
        mounts=[
            Mount(source='/Users/huuuuhuuuu/BDMProjectTeamFLH/data', target='/data', type='bind')
        ],
        environment={
            'DELTA_LAKE_PATH': '/data/delta',
            'PHOTO_DIR': '/data/unstructured/profile_photos',
            'API_HOST': 'api',
            'API_PORT': '8000'
        }
    )

    # Task 3: Process batch logs
    process_batch = DockerOperator(
        task_id='process_batch_logs',
        image='bdmprojectteamflh-spark:latest',
        api_version='auto',
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode='bridge',
        command=[
        "/bin/bash", "-c",
        "cd /app && /opt/bitnami/python/bin/python ingest_data.py --mode=batch || exit 1"
    ],
        entrypoint="", 
        mounts=[
            Mount(source='/Users/huuuuhuuuu/BDMProjectTeamFLH/data', target='/data', type='bind')
        ],
        environment={
            'DELTA_LAKE_PATH': '/data/delta',
            'LOG_DATA_PATH': '/data/log_data',
            'API_HOST': 'api',
            'API_PORT': '8000'
        },
        working_dir='/app'
    )

    # Task 4: Start streaming 
    start_streaming = DockerOperator(
    task_id='start_log_streaming',
    image='bdmprojectteamflh-spark:latest',
    command=[
        "/bin/bash", "-c",
        "cd /app && /opt/bitnami/python/bin/python ingest_data.py --mode=streaming || exit 1"
    ],
    mounts=[
        Mount(source='/Users/huuuuhuuuu/BDMProjectTeamFLH/data', target='/data', type='bind')
    ],
    environment={
        'DELTA_LAKE_PATH': '/data/delta',
        'LOG_DATA_PATH': '/data/log_data'
    }
)

    # Task 5: Health check
    def check_streaming_health():
        """Check if streaming application is running"""
        try:
            response = requests.get("http://spark:4040/api/v1/applications", timeout=10)
            apps = response.json()
            return any('StreamingQuery' in app.get('name', '') for app in apps)
        except Exception as e:
            print(f"Health check failed: {str(e)}")
            return False

    streaming_health_check = PythonOperator(
        task_id='verify_streaming_health',
        python_callable=check_streaming_health,
        retries=2,
        retry_delay=timedelta(minutes=1))
    
    # Task 6: Notifications
    send_notification = BashOperator(
        task_id='send_completion_notification',
        bash_command='echo "Pipeline completed successfully" | mail -s "Delta Lake Pipeline Status" admin@example.com',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Workflow definition

    ingest_via_api >> process_photos
    process_photos >> process_batch 

    # After batch processing completes:
    process_batch >> [start_streaming, streaming_health_check]

    # Streaming and health check run in parallel
    # Health check continuously monitors streaming status
    streaming_health_check >> send_notification

    # Notification also waits for batch completion
    process_batch >> send_notification

    # Final notification waits for both:
    # - Batch completion (directly)
    # - Health check confirmation (indirectly)
    send_notification << [process_batch, streaming_health_check]