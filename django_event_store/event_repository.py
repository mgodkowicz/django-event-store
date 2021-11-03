from datetime import datetime
from typing import List, Optional, Sequence

from django.db import transaction, IntegrityError
from django.utils.timezone import make_aware

from exceptions import WrongExpectedEventVersion
from expected_version import ExpectedVersion
from repository import Records
from specification import SpecificationResult
from stream import Stream

from django_event_store.event_repository_reader import DjangoEventRepositoryReader
from django_event_store.models import Event as EventModel
from django_event_store.models import EventsInStreams
from event_store import Event, EventsRepository, Record, EventNotFound


class DjangoEventRepository(EventsRepository):
    POSITION_SHIFT = 1

    def __init__(self):
        # fixme, configurable
        self.event_class = EventModel
        self.stream_class = EventsInStreams

    def append_to_stream(
        self,
        records: Records,
        stream: Stream,
        expected_version: Optional[ExpectedVersion] = ExpectedVersion.none(),
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
        expected_version: Optional[ExpectedVersion] = ExpectedVersion.none(),
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
        return DjangoEventRepositoryReader(self.event_class, self.stream_class).read(
            spec
        )

    def has_event(self, event_id: str) -> bool:
        pass

    def delete_stream(self, stream: Stream) -> "EventsRepository":
        pass

    def count(self, spec: SpecificationResult) -> int:
        pass

    def streams_of(self, event_id: str) -> list:
        pass

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
