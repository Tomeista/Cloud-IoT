from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_events_topic: str = "sensor-events"
    kafka_aggregates_topic: str = "sensor-aggregates"
    kafka_alerts_topic: str = "sensor-alerts"
    kafka_late_topic: str = "sensor-late-events"

    s3_endpoint: str = "http://seaweedfs:8333"
    s3_access_key: str = "seaweedadmin"
    s3_secret_key: str = "seaweedadmin"
    s3_bucket: str = "iot-lakehouse"
    s3_region: str = "us-east-1"
    s3_archive_max_batch: int = 500
    s3_archive_flush_seconds: int = 60

    # A Delta table takes exactly one writer, so the archiver runs in its own
    # single-replica Deployment and the serving replicas start with this off.
    # Both use the same image; only this flag differs.
    archiver_enabled: bool = True

    class Config:
        env_prefix = "APP_"


settings = Settings()
