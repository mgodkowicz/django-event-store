from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class Record:
    event_id: str
    data: dict
    metadata: dict
    event_type: str
    timestamp: datetime.timestamp
    valid_at: datetime.timestamp
    serialized_records: dict = field(default_factory=dict, compare=False)

    def to_dict(self):
        return {
            "event_id": self.event_id,
            "data": self.data,
            "metadata": self.metadata,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "valid_at": self.valid_at,
        }

    def serialize(self, serializer):
        return Record(
            event_id=self.event_id,
            event_type=self.event_type,
            data=serializer.dumps(self.data),
            metadata=serializer.dumps(self.metadata),
            timestamp=datetime.now().timestamp(),
            valid_at=datetime.now().timestamp(),
        )
