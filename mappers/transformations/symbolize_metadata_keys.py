from record import Record


class SymbolizeMetadataKeys:
    def dump(self, record):
        return self._symbolize(record)

    def load(self, record):
        return self._symbolize(record)

    def _symbolize(self, record):
        return Record(
            event_id=record.event_id,
            event_type=record.event_type,
            data=record.data,
            metadata=record.metadata,
            timestamp=record.timestamp,
            valid_at=record.valid_at,
        )
