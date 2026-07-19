import pytest
from pydantic import ValidationError
from src.config import Settings

def test_settings_validation_enforces_extra_forbid() -> None:
    """Verifies that the Settings model forbids extra fields to ensure strict configuration sanitization."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            GCP_PROJECT_ID="test-project",
            GCS_BUCKET_NAME="test-bucket",
            extra_field_not_allowed="should-fail"
        )
    assert "extra_field_not_allowed" in str(exc_info.value)
    assert "Extra inputs are not permitted" in str(exc_info.value)

def test_settings_computed_paths_are_dry_and_correct() -> None:
    """Validates dynamic generation of Iceberg warehouse and checkpoint GCS paths."""
    settings = Settings(
        GCP_PROJECT_ID="olist-lakehouse-v25",
        GCS_BUCKET_NAME="olist-streaming-lakehouse-bucket",
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
    )
    
    # Assert correct computed paths
    assert settings.gcs_warehouse_path == "gs://olist-streaming-lakehouse-bucket/warehouse"
    assert settings.gcs_checkpoint_path == "gs://olist-streaming-lakehouse-bucket/checkpoints/stream_processor"

def test_settings_default_values_are_correct() -> None:
    """Ensures base defaults match production expectations (e.g. 10 seconds spark trigger)."""
    assert Settings.model_fields["SPARK_TRIGGER_PROCESSING_TIME"].default == "10 seconds"
    assert Settings.model_fields["KAFKA_SPEED_MULTIPLIER"].default == 10000.0
    assert Settings.model_fields["INJECT_ERROR"].default is False

