import math
from typing import Union

from django_event_store.models import Event, EventsInStreams
from event_store import EventNotFound, Record
from event_store.batch_enumerator import BatchEnumerator
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
                for batch in BatchEnumerator.new(spec.batch_size, spec.limit, batch_reader).each()
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

    def _read_scope_for_global(self, spec: SpecificationResult):
        qs = self.event_class.objects

        if spec.with_ids is not None:
            qs = qs.filter(event_id__in=spec.with_ids)

        if spec.with_types is not None:
            qs = qs.filter(event_type__in=spec.with_types)

        if spec.start:
            qs = qs.filter(**self._start_condition_global(spec))
        if spec.stop:
            qs = qs.filter(**self._stop_condition_global(spec))

        qs = self._ordered_global(qs, spec)

        if spec.limit is not math.inf:
            qs = qs.all()[: spec.limit]

        return qs.all()

    def _read_scope_for_local(self, spec: SpecificationResult):
        qs = self.stream_class.objects.filter(stream=spec.stream.name).select_related(
            "event"
        )

        if spec.with_ids is not None:
            qs = qs.filter(event_id__in=spec.with_ids)

        if spec.with_types is not None:
            qs = qs.filter(event__event_type__in=spec.with_types)

        if spec.start:
            qs = qs.filter(**self._start_condition(spec))
        if spec.stop:
            qs = qs.filter(**self._stop_condition(spec))

        qs = self._ordered_local(qs, spec)

        if spec.limit is not math.inf:
            qs = qs.all()[: spec.limit]

        return qs.all()

    def _start_condition(self, spec: SpecificationResult):
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

    def _ordered(self, qs, spec: SpecificationResult, field: str = ""):
        conditions = []
        try:
            conditions.append(
                {
                    "as_at": self._order(f"{field}created_at", spec),
                    "as_of": self._order(f"{field}valid_at", spec),
                }[spec.time_sort_by]
            )
        except KeyError:
            pass

        conditions.append(self._order("id", spec))
        return qs.order_by(*conditions)

    def _ordered_local(self, qs, spec: SpecificationResult):
        return self._ordered(qs, spec, "event__")

    def _ordered_global(self, qs, spec: SpecificationResult):
        return self._ordered(qs, spec)

    def _order(self, field: str, spec: SpecificationResult) -> str:
        if spec.backward:
            return f"-{field}"
        return field

    def _to_record(self, event: Union[Event, EventsInStreams]) -> Record:
        record = event.event if isinstance(event, self.stream_class) else event
        return Record(
            event_id=record.event_id,
            metadata=record.metadata,
            data=record.data,
            event_type=record.event_type,
            timestamp=record.created_at.timestamp(),
            valid_at=record.valid_at.timestamp() or event.created_at.timestamp(),
        )
