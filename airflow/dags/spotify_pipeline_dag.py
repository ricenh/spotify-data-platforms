"""
Spotify Data Pipeline DAG
Runs daily to extract, load, and transform Spotify listening data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add src to Python path
sys.path.insert(0, "/opt/airflow")

# Default arguments
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "spotify_data_pipeline",
    default_args=default_args,
    description="Daily Spotify listening data pipeline",
    schedule_interval="0 2 * * *",  # Run at 2 AM daily
    catchup=False,
    tags=["spotify", "etl", "data-pipeline"],
)


# Task functions
def extract_recently_played_task():
    """Extract recently played tracks from Spotify API"""
    from src.extract.extract_recently_played import extract_recently_played

    print("Starting extraction of recently played tracks...")
    result = extract_recently_played()
    print(f"Extraction complete: {result}")
    return result


def extract_tracks_task():
    """Extract track details"""
    from src.extract.extract_tracks import extract_tracks

    print("Starting extraction of track details...")
    result = extract_tracks()
    print(f"Extraction complete: {result}")
    return result


def extract_audio_features_task():
    """Extract audio features"""
    from src.extract.extract_audio_features import extract_audio_features

    print("Starting extraction of audio features...")
    result = extract_audio_features()
    print(f"Extraction complete: {result}")
    return result


def load_artists_task():
    """Load artists to RDS"""
    from src.load.load_artists import load_artists

    print("Loading artists to RDS...")
    load_artists()
    print("Artists loaded successfully")


def load_tracks_task():
    """Load tracks to RDS"""
    from src.load.load_tracks import load_tracks

    print("Loading tracks to RDS...")
    load_tracks()
    print("Tracks loaded successfully")


def load_plays_task():
    """Load plays to RDS"""
    from src.load.load_plays import load_plays

    print("Loading plays to RDS...")
    load_plays()
    print("Plays loaded successfully")


def load_audio_features_task():
    """Load audio features to RDS"""
    from src.load.load_audio_features import load_audio_features

    print("Loading audio features to RDS...")
    load_audio_features()
    print("Audio features loaded successfully")


# Define tasks
extract_recently_played = PythonOperator(
    task_id="extract_recently_played",
    python_callable=extract_recently_played_task,
    dag=dag,
)

extract_tracks = PythonOperator(
    task_id="extract_tracks",
    python_callable=extract_tracks_task,
    dag=dag,
)

extract_audio_features = PythonOperator(
    task_id="extract_audio_features",
    python_callable=extract_audio_features_task,
    dag=dag,
)

load_artists = PythonOperator(
    task_id="load_artists",
    python_callable=load_artists_task,
    dag=dag,
)

load_tracks = PythonOperator(
    task_id="load_tracks",
    python_callable=load_tracks_task,
    dag=dag,
)

load_plays = PythonOperator(
    task_id="load_plays",
    python_callable=load_plays_task,
    dag=dag,
)

load_audio_features = PythonOperator(
    task_id="load_audio_features",
    python_callable=load_audio_features_task,
    dag=dag,
)

run_dbt = BashOperator(
    task_id="run_dbt_models",
    bash_command="cd /opt/airflow/spotify_analytics && dbt run",
    env={"DBT_PROFILES_DIR": "/opt/airflow/spotify_analytics", **os.environ},
    dag=dag,
)

run_dbt_tests = BashOperator(
    task_id="run_dbt_tests",
    bash_command="cd /opt/airflow/spotify_analytics && dbt test",
    env={"DBT_PROFILES_DIR": "/opt/airflow/spotify_analytics", **os.environ},
    dag=dag,
)

# Define task dependencies
# Extraction phase
extract_recently_played >> [extract_tracks, extract_audio_features, load_plays]

# Load phase
extract_tracks >> load_artists >> load_tracks
extract_audio_features >> load_audio_features

# dbt runs after all loads
[load_tracks, load_plays, load_audio_features] >> run_dbt >> run_dbt_tests
