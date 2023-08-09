from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SerializedRecord:
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/serialized_record.rb
    """

    class StringsRequired(TypeError):
        pass

    event_id: str
    data: str
    metadata: str
    event_type: str
    timestamp: str
    valid_at: str

    def __hash__(self):
        return hash(
            (
                self.event_id,
                self.data,
                self.metadata,
                self.event_type,
                self.timestamp,
                self.valid_at,
            )
        ) ^ hash(self.__class__)

    def __post_init__(self):
        if not all(isinstance(v, str) for v in [self.event_id, self.event_type]):
            raise SerializedRecord.StringsRequired(
                "event_id and event_type both must be strings"
            )

    @classmethod
    def new(
        cls,
        event_id: str,
        data: str,
        metadata: str,
        event_type: str,
        timestamp: str,
        valid_at: str,
    ):
        return cls(
            event_id=event_id,
            data=data,
            metadata=metadata,
            event_type=event_type,
            timestamp=timestamp,
            valid_at=valid_at,
        )

    def to_dict(self):
        return {
            "event_id": self.event_id,
            "data": self.data,
            "metadata": self.metadata,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "valid_at": self.valid_at,
        }

    def deserialize(self, serializer):
        from event_store.record import Record

        return Record.new(
            event_id=self.event_id,
            data=serializer.load(self.data),
            metadata=serializer.load(self.metadata),
            event_type=self.event_type,
            timestamp=datetime.fromisoformat(self.timestamp),
            valid_at=datetime.fromisoformat(self.valid_at),
        )
