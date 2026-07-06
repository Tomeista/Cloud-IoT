from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class SensorEvent(BaseModel):
    sensor_id: str
    event_time: datetime = Field(default_factory=lambda: datetime.utcnow())
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
