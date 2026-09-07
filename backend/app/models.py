from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class SensorEvent(BaseModel):
    sensor_id: str
    # Timezone-aware: the Flink job derives watermarks from this field, and a
    # naive timestamp would be read as local time there instead of UTC.
    event_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    sensor_type: str
    value: float
    unit: str
    location: str


class SensorAggregate(BaseModel):
    window_start: str
    window_end: str
    sensor_id: str
    sensor_type: str
    location: str
    avg_value: float
    min_value: float
    max_value: float
    event_count: int


class SensorAlert(BaseModel):
    id: Optional[str] = None
    sensor_id: str
    sensor_type: str
    location: str
    value: float
    threshold: float
    timestamp: str
    severity: str = "warning"
