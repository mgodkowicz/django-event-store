from record import Record


class EventClassRemapper:
    def __init__(self, class_map):
        self.class_map = class_map

    def dump(self, record):
        return record

    def load(self, record):
        return Record(
            event_id=record.event_id,
            event_type=self.class_map.get(record.event_type, record.event_type),
            data=record.data,
            metadata=record.metadata,
            timestamp=record.timestamp,
            valid_at=record.valid_at,
        )
