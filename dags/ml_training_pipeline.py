"""DAG 2: run manually after some Gold files are ready."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from src import ml_pipeline


@dag(
    dag_id="retail_ml_training_pipeline",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,  # Manual for class; switch to "0 9 * * 1" for Mondays at 09:00 UTC.
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["workshop", "machine-learning"],
)
def ml_training_pipeline():
    @task
    def prepare_features():
        ml_pipeline.prepare_features()

    @task
    def train_model():
        ml_pipeline.train_model()

    @task
    def evaluate_model():
        ml_pipeline.evaluate_model()

    @task
    def save_model():
        ml_pipeline.save_model()

    prepare_features() >> train_model() >> evaluate_model() >> save_model()


ml_training_pipeline()
