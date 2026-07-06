from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_events_topic: str = "sensor-events"
    kafka_aggregates_topic: str = "sensor-aggregates"
    kafka_alerts_topic: str = "sensor-alerts"

    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "iot-lakehouse"

    simulator_num_sensors: int = 20
    simulator_interval: float = 1.0

    class Config:
        env_prefix = "APP_"


settings = Settings()
