from collections import defaultdict
from dataclasses import dataclass
from math import inf
from typing import Dict, List, Union

from batch_enumerator import BatchIterator
from record import Record
from repository import EventsRepository, Records
from specification import SpecificationResult
from stream import Stream


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


class InMemoryRepository(EventsRepository):
    def __init__(self, serializer=None):
        self.serializer = serializer or FakeSerializer  # JSONEncoder?
        self.streams = defaultdict(list)
        self.storage: Dict[Record] = {}

    def append_to_stream(
        self, records: Records, stream: Stream, expected_version=None
    ) -> "InMemoryRepository":
        serialized_records = [record.serialize(self.serializer) for record in records]

        for index, serialized_record in enumerate(serialized_records):
            if self.has_event(serialized_record.event_id):
                raise EventDuplicatedInStream()

            self.storage[serialized_record.event_id] = serialized_record
            fake_resolved_version = 1
            self._add_to_stream(stream, serialized_record, fake_resolved_version, index)

        return self

    def link_to_stream(self, event_ids, stream, expected_version):
        pass

    def read(
        self, spec: SpecificationResult
    ) -> Union[List[Records], Record]:  # FIXME figure out the type
        serialized_records = self._read_scope(spec)
        # FIXME deserialization?
        # return [record.deserialize(self.serializer) for record in serialized_records]
        if spec.batched:

            def batch_reader(offset: int, limit: int):
                records = serialized_records[offset:]
                return records[:limit]

            res = [
                batch
                for batch in BatchIterator(
                    spec.batch_size, len(serialized_records), batch_reader
                )
            ]
            return res
        elif spec.first:
            return serialized_records[0] if serialized_records else None
        elif spec.last:
            return serialized_records[-1] if serialized_records else None

        return [serialized_records]

    def has_event(self, event_id: str) -> bool:
        return event_id in self.storage

    def delete_stream(self, stream: Stream) -> "InMemoryRepository":
        del self.streams[stream.name]
        return self

    def count(self, spec: SpecificationResult) -> int:
        return len(self._read_scope(spec))

    def _add_to_stream(
        self, stream: Stream, serialized_record: Record, resolved_version, index
    ) -> None:
        self.streams[stream.name].append(
            EventInStream(
                serialized_record.event_id,
                self._compute_position(resolved_version, index),
            )
        )

    def _compute_position(self, resolved_version: int, index: int) -> int:
        return resolved_version + index + 1

    def _read_scope(self, spec: SpecificationResult) -> Records:
        serialized_records = self._serialized_records_of_stream(spec.stream)
        serialized_records = self._ordered(serialized_records, spec)
        serialized_records = (
            serialized_records[::-1] if spec.backward else serialized_records
        )
        serialized_records = (
            serialized_records[self._index_of(serialized_records, spec.start) + 1 :]
            if spec.start
            else serialized_records
        )
        serialized_records = (
            serialized_records[: spec.limit]
            if spec.limit is not inf
            else serialized_records
        )
        if spec.with_ids is not None:
            serialized_records = [
                record
                for record in serialized_records
                if record.event_id in spec.with_ids
            ]
        if spec.with_types is not None:
            # breakpoint()
            serialized_records = [
                record
                for record in serialized_records
                if record.event_type in spec.with_types
            ]
        return serialized_records

    def _index_of(self, source: Records, event_id: str) -> int:
        for idx, record in enumerate(source):
            if record.event_id == event_id:
                return idx

    def _event_ids_of_stream(self, stream: Stream) -> List[str]:
        return [event.event_id for event in self.streams[stream.name]]

    def _serialized_records_of_stream(self, stream: Stream) -> List[Record]:
        if stream.is_global:
            return list(self.storage.values())
        events_id = self._event_ids_of_stream(stream)
        return list(
            filter(lambda event: event.event_id in events_id, self.storage.values())
        )

    def _ordered(
        self, serialized_records: Records, spec: SpecificationResult
    ) -> Records:
        return serialized_records
