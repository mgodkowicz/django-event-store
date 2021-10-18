from event import Event
from record import Record


class DomainEvent:

    def dump(self, domain_event: Event):
        # TODO remove timestamp and valid_at, Why?
        return Record(
            event_id=domain_event.event_id,
            event_type=domain_event.event_type,
            data=domain_event.data,
            metadata=domain_event.metadata,
            timestamp=domain_event.timestamp,
            valid_at=domain_event.valid_at,
        )

    def load(self, record: Record) -> Event:
        return type(record.event_type, (Event,), {})(
            event_id=record.event_id,
            data=record.data,
            metadata=record.metadata,
        )
