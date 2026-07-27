from pydantic import Field, computed_field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Manages and validates all application configuration parameters loaded from environment variables.
    Ensures fail-fast behavior during system startup if configurations are invalid.
    """
    
    # --- GCP Infrastructure Configurations ---
    GCP_PROJECT_ID: str = Field(..., description="The Google Cloud Project ID for BigQuery and BigLake.")
    GCS_BUCKET_NAME: str = Field(..., description="The globally unique GCS bucket name for Lakehouse storage.")
    
    # --- Kafka Broker Configurations ---
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092", description="Comma-separated list of host/port pairs.")
    KAFKA_TOPIC_RAW_TRANSACTIONS: str = Field(default="raw_transactions", description="Primary topic for incoming stream.")
    KAFKA_TOPIC_DLQ_STRUCTURAL: str = Field(default="raw_transactions_bf_dlq", description="DLQ Tier 1 for Avro decoding failures.")
    SCHEMA_REGISTRY_URL: str = Field(default="http://localhost:8081", description="Confluent Schema Registry endpoint.")
    
    # --- Fault Injection Settings (Strictly Float for Probabilities) ---
    INJECT_ERROR: bool = Field(default=False, description="Master toggle for simulating data faults.")
    STRUCTURAL_ERROR_RATE: float = Field(default=0.02, ge=0.0, le=1.0, description="Probability rate for Kafka-boundary corruptions.")
    BUSINESS_ERROR_RATE: float = Field(default=0.03, ge=0.0, le=1.0, description="Probability rate for Pydantic/GE business rule failures.")
    
    # --- Spark Tuning Parameters ---
    SPARK_MAX_OFFSETS_PER_TRIGGER: int = Field(default=1000, ge=1, description="Rate limit on maximum offsets processed per trigger interval.")
    SPARK_TRIGGER_PROCESSING_TIME: str = Field(default="15 seconds", description="Trigger interval for streaming micro-batches.")
    KAFKA_SPEED_MULTIPLIER: float = Field(default=10000.0, ge=1.0, description="Multiplier for time-based CSV event replay speed.")

    
    # --- Security & Alerting Endpoints (Masked via SecretStr) ---
    SLACK_WEBHOOK_URL: SecretStr | None = Field(default=None, description="Slack Webhook URL masked to prevent accidental log leakage.")

    # --- Derived Configurations via computed_field (DRY Principle) ---
    @computed_field
    @property
    def gcs_warehouse_path(self) -> str:
        """Dynamically builds the Iceberg warehouse path location."""
        return f"gs://{self.GCS_BUCKET_NAME}/warehouse"

    @computed_field
    @property
    def gcs_checkpoint_path(self) -> str:
        """Dynamically builds the Spark Structured Streaming checkpoint location."""
        return f"gs://{self.GCS_BUCKET_NAME}/checkpoints/stream_processor"

    # --- Pydantic Engine Curation Layout ---
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid"  # Strictly reject unauthorized or misspelled environment variables
    )

# Instantiate the singleton configuration object to be imported across modules
config = Settings()