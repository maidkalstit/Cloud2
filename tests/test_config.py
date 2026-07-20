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

def test_settings_type_casting() -> None:
    """Verifies that Pydantic automatically casts compatible types (e.g. string to float)."""
    settings = Settings(
        GCP_PROJECT_ID="test-project",
        GCS_BUCKET_NAME="test-bucket",
        STRUCTURAL_ERROR_RATE="0.05",
        BUSINESS_ERROR_RATE="0.08",
        SPARK_MAX_OFFSETS_PER_TRIGGER="1500"
    )
    assert isinstance(settings.STRUCTURAL_ERROR_RATE, float)
    assert settings.STRUCTURAL_ERROR_RATE == 0.05
    assert isinstance(settings.BUSINESS_ERROR_RATE, float)
    assert settings.BUSINESS_ERROR_RATE == 0.08
    assert isinstance(settings.SPARK_MAX_OFFSETS_PER_TRIGGER, int)
    assert settings.SPARK_MAX_OFFSETS_PER_TRIGGER == 1500

def test_settings_validation_errors() -> None:
    """Verifies that validation errors are raised for incompatible types."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            GCP_PROJECT_ID="test-project",
            GCS_BUCKET_NAME="test-bucket",
            STRUCTURAL_ERROR_RATE="not-a-float"
        )
    assert "Input should be a valid number" in str(exc_info.value)


