from unittest import TestCase

from event_store.event import Event
from event_store.mappers.default import Default
from event_store.record import Record


class TestMappers(TestCase):
    def setUp(self) -> None:
        self.mapper = Default()
        self.domain_event = Event()

    def test_event_to_record_returns_instance_of_record(self):
        record = self.mapper.event_to_record(self.domain_event)

        assert isinstance(record, Record)
        assert record.event_id == self.domain_event.event_id
        assert record.event_type == self.domain_event.event_type

    def test_serialize_and_deserialize_gives_equal_event(self):
        record = self.mapper.event_to_record(self.domain_event)

        assert self.mapper.record_to_event(record) == self.domain_event
