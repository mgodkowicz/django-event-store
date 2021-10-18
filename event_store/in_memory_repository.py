import json
from collections import defaultdict
from dataclasses import dataclass
from math import inf

from specification import Specification, SpecificationResult
from stream import Stream, GLOBAL_STREAM


class EventDuplicatedInStream(BaseException):
    pass

@dataclass
class EventInStream:
    event_id: str
    position: int


class FakeSerializer:
    @staticmethod
    def dumps(args):
        return args


class InMemoryRepository:

    def __init__(self, serializer=None):
        self.serializer = serializer or FakeSerializer  # JSONEncoder?
        self.streams = defaultdict(list)
        self.storage = {}

    def append_to_stream(self, records, stream, expected_version=None):
        serialized_records = [record.serialize(self.serializer) for record in records]

        for index, serialized_record in enumerate(serialized_records):
            if self.has_event(serialized_record.event_id):
                raise EventDuplicatedInStream()

            self.storage[serialized_record.event_id] = serialized_record
            fake_resolved_version = 1
            self._add_to_stream(stream, serialized_record, fake_resolved_version, index)

    def link_to_stream(self, event_ids, stream, expected_version):
        pass

    def read(self, spec: SpecificationResult):
        serialized_records = self.read_scope(spec)
        # breakpoint()
        # return [record.deserialize(self.serializer) for record in serialized_records]
        return [serialized_records]

    def has_event(self, event_id: str) -> bool:
        return event_id in self.storage

    def _add_to_stream(self, stream, serialized_record, resolved_version, index):
        self.streams[stream.name].append(EventInStream(serialized_record.event_id, self._compute_position(resolved_version, index)))

    def _compute_position(self, resolved_version, index):
        return resolved_version + index + 1

    def read_scope(self, spec: SpecificationResult):
        serialized_records = self.serialized_records_of_stream(spec.stream)
        serialized_records = serialized_records[:spec.limit] if spec.limit is not inf else serialized_records
        return serialized_records

    def serialized_records_of_stream(self, stream: Stream):
        if stream.is_global:
            return list(self.storage.values())
        return self.storage[stream.name]
