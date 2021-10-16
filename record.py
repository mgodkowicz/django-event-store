from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class Record:
    event_id: str
    data: dict
    metadata: dict
    event_type: str
    timestamp: datetime
    valid_at: datetime
    serialized_records: dict = field(default_factory=dict, compare=False)

