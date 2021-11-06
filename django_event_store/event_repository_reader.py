import math
from typing import Union

from django_event_store.models import Event, EventsInStreams
from event_store import Record
from event_store.batch_enumerator import BatchIterator
from event_store.specification import SpecificationResult


class DjangoEventRepositoryReader:
    def __init__(self, event_class, stream_class):
        self.event_class = event_class
        self.stream_class = stream_class

    def read(self, spec: SpecificationResult):
        stream = self._read_scope(spec)
        if spec.batched:

            def batch_reader(offset: int, limit: int):
                return stream[offset : offset + limit]

            return [
                [self._to_record(event) for event in batch]
                for batch in BatchIterator(spec.batch_size, spec.limit, batch_reader)
            ]
        return [self._to_record(event) for event in stream]

    def has_event(self, event_id: str) -> bool:
        return self.event_class.objects.filter(event_id=event_id).exists()

    def _read_scope(self, spec: SpecificationResult):
        if spec.stream.is_global:
            stream = self.event_class.objects
            if spec.with_ids is not None:
                stream = stream.filter(event_id__in=spec.with_ids)
            if spec.with_types is not None:
                stream = stream.filter(event_type__in=spec.with_types)

            stream = self._ordered(stream, spec)
            stream = self._order(stream, spec)

            if spec.limit is not math.inf:
                stream = stream.all()[: spec.limit]

            return stream.all()
        else:
            # TODO rething if FK would be better after all
            stream = self.stream_class.objects.filter(
                stream=spec.stream.name
            ).select_related("event")
            stream = self._order(stream, spec)
            stream = self._ordered(stream, spec)
            if spec.limit is not math.inf:
                stream = stream.all()[: spec.limit]

            return stream.all()

    def _ordered(self, stream, spec):
        return stream

    def _order(self, stream, spec):
        # see what we can do here. In rails its based on id
        if spec.backward:
            return stream.order_by("-created_at")
        return stream.order_by("created_at")

    def _to_record(self, event: Union[Event, EventsInStreams]):
        if isinstance(event, Event):
            return Record(
                event_id=event.event_id,
                metadata=event.metadata,
                data=event.data,
                event_type=event.event_type,
                timestamp=event.created_at.timestamp(),
                valid_at=event.valid_at.timestamp() or event.created_at.timestamp(),
            )
        elif isinstance(event, EventsInStreams):
            return Record(
                event_id=event.event.event_id,
                metadata=event.event.metadata,
                data=event.event.data,
                event_type=event.event.event_type,
                timestamp=event.event.created_at.timestamp(),
                valid_at=event.event.valid_at.timestamp()
                or event.event.created_at.timestamp(),
            )
