from datetime import datetime
from typing import List, Optional, Sequence

from django.db import IntegrityError, transaction

from django_event_store.event_repository_reader import DjangoEventRepositoryReader
from django_event_store.models import Event as EventModel
from django_event_store.models import EventsInStreams
from event_store import EventNotFound, EventsRepository, Record
from event_store.exceptions import WrongExpectedEventVersion
from event_store.expected_version import ExpectedVersion
from event_store.repository import Records
from event_store.specification import SpecificationResult
from event_store.stream import Stream


class DjangoEventRepository(EventsRepository):
    POSITION_SHIFT = 1

    def __init__(self):
        # fixme, configurable
        self.event_class = EventModel
        self.stream_class = EventsInStreams
        self.repo_reader = DjangoEventRepositoryReader(
            self.event_class, self.stream_class
        )

    def append_to_stream(
        self,
        records: Records,
        stream: Stream,
        expected_version: Optional[ExpectedVersion] = ExpectedVersion.any(),
    ) -> "DjangoEventRepository":
        # FIXME figure out how to handle transactions
        with transaction.atomic():
            self._add_to_stream(
                [record.event_id for record in records], stream, expected_version
            )
            self.event_class.objects.bulk_create(
                [self.event_class(**self._record_to_dict(record)) for record in records]
            )
        return self

    def link_to_stream(
        self,
        event_ids: List[str],
        stream: Stream,
        expected_version: Optional[ExpectedVersion] = ExpectedVersion.any(),
    ) -> "EventsRepository":
        with transaction.atomic():
            if self.event_class.objects.filter(event_id__in=event_ids).count() != len(
                event_ids
            ):
                # TODO raise id of missing event
                raise EventNotFound()
            self._add_to_stream(event_ids, stream, expected_version)
        return self

    def read(self, spec: SpecificationResult) -> List[Records]:
        return self.repo_reader.read(spec)

    def has_event(self, event_id: str) -> bool:
        return self.repo_reader.has_event(event_id)

    def delete_stream(self, stream: Stream) -> "EventsRepository":
        self.stream_class.objects.filter(stream=stream.name).delete()
        return self

    def count(self, spec: SpecificationResult) -> int:
        pass

    def streams_of(self, event_id: str) -> list:
        pass

    def position_in_stream(self, event_id: str, stream: Stream) -> int:
        return self.repo_reader.position_in_stream(event_id, stream)

    def _add_to_stream(
        self,
        events_ids: Sequence[str],
        stream: Stream,
        expected_version: Optional[ExpectedVersion] = None,
    ) -> "DjangoEventRepository":
        def last_stream_version(stream_: Stream):
            return (
                self.stream_class.objects.filter(stream=stream_.name)
                .order_by("-position")
                .first()
            )

        resolved_version = expected_version.resolve_for(stream, last_stream_version)

        in_stream = [
            self.stream_class(
                stream=stream.name,
                position=self._compute_position(resolved_version, index),
                event_id=event_id,
            )
            for index, event_id in enumerate(events_ids)
        ]
        try:
            self.stream_class.objects.bulk_create(in_stream)
        except IntegrityError:
            # TODO IndexViolationDetector based on DB error msg
            raise WrongExpectedEventVersion()

        return self

    def _compute_position(self, resolved_version: int, index: int) -> Optional[int]:
        if resolved_version is not None:
            return resolved_version + index + self.POSITION_SHIFT
        return None

    def _record_to_dict(self, record: Record) -> dict:
        return {
            "event_id": record.event_id,
            "data": record.data,
            "metadata": record.metadata,
            "event_type": record.event_type,
            "created_at": datetime.fromtimestamp(record.timestamp),
            "valid_at": datetime.fromtimestamp(record.valid_at or record.timestamp),
        }
