import math
from typing import Union

from django_event_store.models import Event, EventsInStreams
from event_store import EventNotFound, Record
from event_store.batch_enumerator import BatchIterator
from event_store.specification import SpecificationResult
from event_store.stream import Stream


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
        elif spec.first:
            record = stream.first()
            return self._to_record(record) if record else None

        return [self._to_record(event) for event in stream]

    def has_event(self, event_id: str) -> bool:
        return self.event_class.objects.filter(event_id=event_id).exists()

    def position_in_stream(self, event_id: str, stream: Stream) -> int:
        try:
            return (
                self.stream_class.objects.only("position")
                .get(event_id=event_id, stream=stream.name)
                .position
            )
        except self.stream_class.DoesNotExist:
            raise EventNotFound()

    def _read_scope(self, spec: SpecificationResult):
        if spec.stream.is_global:
            return self._read_scope_for_global(spec)
        return self._read_scope_for_local(spec)

    def _read_scope_for_global(self, spec):
        stream = self.event_class.objects

        if spec.with_ids is not None:
            stream = stream.filter(event_id__in=spec.with_ids)

        # Uncomment when tests written
        # if spec.with_types is not None:
        #     stream = stream.filter(event_type__in=spec.with_types)

        if spec.start:
            stream = stream.filter(**self._start_condition_global(spec))
        if spec.stop:
            stream = stream.filter(**self._stop_condition_global(spec))

        stream = self._ordered(stream, spec)
        stream = self._order(stream, spec)

        if spec.limit is not math.inf:
            stream = stream.all()[: spec.limit]

        return stream.all()

    def _read_scope_for_local(self, spec):
        stream = self.stream_class.objects.filter(
            stream=spec.stream.name
        ).select_related("event")

        if spec.with_ids is not None:
            stream = stream.filter(event_id__in=spec.with_ids)

        stream = self._ordered(stream, spec)
        stream = self._order(stream, spec)

        if spec.start:
            stream = stream.filter(**self._start_condition(spec))
        if spec.stop:
            stream = stream.filter(**self._stop_condition(spec))

        if spec.limit is not math.inf:
            stream = stream.all()[: spec.limit]

        return stream.all()

    def _start_condition(self, spec):
        event_in_stream = self.stream_class.objects.only("id").get(
            event_id=spec.start, stream=spec.stream.name
        )
        return self._start_offset_condition(event_in_stream.id, spec)

    def _stop_condition(self, spec):
        event_in_stream = self.stream_class.objects.only("id").get(
            event_id=spec.stop, stream=spec.stream.name
        )
        return self._stop_offset_condition(event_in_stream.id, spec)

    def _start_condition_global(self, spec):
        event_in_stream = self.event_class.objects.only("id").get(event_id=spec.start)
        return self._start_offset_condition(event_in_stream.id, spec)

    def _stop_condition_global(self, spec):
        event_in_stream = self.event_class.objects.only("id").get(event_id=spec.stop)
        return self._stop_offset_condition(event_in_stream.id, spec)

    def _start_offset_condition(self, event_id: str, spec: SpecificationResult) -> dict:
        if spec.forward:
            return {"id__gt": event_id}
        return {"id__lt": event_id}

    def _stop_offset_condition(self, event_id: str, spec: SpecificationResult) -> dict:
        if spec.forward:
            return {"id__lt": event_id}
        return {"id__gt": event_id}

    def _ordered(self, stream, spec):
        return stream

    def _order(self, stream, spec):
        if spec.backward:
            return stream.order_by("-id")
        return stream.order_by("id")

    def _to_record(self, event: Union[Event, EventsInStreams]):
        record = event.event if isinstance(event, self.stream_class) else event
        return Record(
            event_id=record.event_id,
            metadata=record.metadata,
            data=record.data,
            event_type=record.event_type,
            timestamp=record.created_at.timestamp(),
            valid_at=record.valid_at.timestamp() or event.created_at.timestamp(),
        )
