from dataclasses import dataclass, field
from datetime import datetime

from frozendict import frozendict


@dataclass(frozen=True)
class Record:
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/record.rb
    """

    class StringsRequired(TypeError):
        pass

    event_id: str
    data: frozendict
    metadata: frozendict
    event_type: str
    timestamp: datetime
    valid_at: datetime
    serialized_records: dict = field(default_factory=dict, compare=False)

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
            raise Record.StringsRequired("event_id and event_type both must be strings")

    @classmethod
    def new(
        cls,
        event_id: str,
        data: dict,
        metadata: dict,
        event_type: str,
        timestamp: datetime,
        valid_at: datetime,
    ):
        return cls(
            event_id=event_id,
            data=frozendict(data),
            metadata=frozendict(metadata),
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

    def serialize(self, serializer):
        from event_store.serialized_record import SerializedRecord

        return self.serialized_records.setdefault(
            serializer,
            SerializedRecord.new(
                event_id=self.event_id,
                data=serializer.dump(dict(self.data)),
                metadata=serializer.dump(dict(self.metadata)),
                event_type=self.event_type,
                timestamp=self.timestamp.isoformat(timespec="microseconds"),
                valid_at=self.valid_at.isoformat(timespec="microseconds"),
            ),
        )
