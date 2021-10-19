import math
import uuid
from collections import Iterable
from contextlib import contextmanager
from unittest import TestCase

from exceptions import EventNotFound, InvalidPageSize

from event_store.event import Event
from event_store.in_memory_repository import InMemoryRepository
from event_store.mappers.default import Default
from event_store.specification import (
    InvalidPageStart,
    Specification,
    SpecificationResult,
)
from event_store.specification_reader import SpecificationReader
from event_store.stream import GLOBAL_STREAM, Stream


class TestEvent(Event):
    pass


class SpecificationTest(TestCase):
    def setUp(self) -> None:
        self.repository = InMemoryRepository()
        self.mapper = Default()
        self.specification = Specification(
            SpecificationReader(self.repository, self.mapper),
            SpecificationResult(Stream.new()),
        )

    def test_iterators(self):
        self.assertTrue(isinstance(self.specification.each(), Iterable))

    def test_direction(self):

        self.assertTrue(self.specification.result.forward)
        self.assertFalse(self.specification.result.backward)

        self.assertTrue(self.specification.forward().result.forward)
        self.assertFalse(self.specification.forward().result.backward)

        self.assertFalse(self.specification.backward().result.forward)
        self.assertTrue(self.specification.backward().result.backward)

    def test_stream_name(self):
        self.assertEqual(
            self.specification.stream("stream").result.stream.name, "stream"
        )
        self.assertEqual(self.specification.result.stream.name, GLOBAL_STREAM)
        self.assertTrue(self.specification.result.stream.is_global)
        self.assertFalse(self.specification.stream("stream").result.stream.is_global)

    def test_specification_start_from(self):
        with self.assertRaises(InvalidPageStart):
            self.specification.start_from(None)

        with self.assertRaises(InvalidPageStart):
            self.specification.start_from("")

        with self.assertRaises(EventNotFound):
            self.specification.start_from("dsds")

        with self.with_event_of_id("123"):
            self.assertEqual(self.specification.start_from("123").result.start, "123")

    def test_specification_to(self):
        with self.assertRaises(InvalidPageStart):
            self.specification.to(None)

        with self.assertRaises(InvalidPageStart):
            self.specification.to("")

        with self.assertRaises(EventNotFound):
            self.specification.to("dsds")

        with self.with_event_of_id("123"):
            self.assertEqual(self.specification.to("123").result.stop, "123")

    def test_read_as(self):
        assert self.specification.result.all is True
        assert self.specification.result.batched is False
        assert self.specification.result.first is False
        assert self.specification.result.last is False

    def test_limit(self):
        with self.assertRaises(InvalidPageSize):
            self.specification.limit(None)

        with self.assertRaises(InvalidPageSize):
            self.specification.limit(0)

        self.assertEqual(self.specification.result.limit, math.inf)
        self.assertEqual(self.specification.limit(1).result.limit, 1)
        self.assertFalse(self.specification.result.limited)
        self.assertTrue(self.specification.limit(1).result.limited)

    def test_record(
        self, event_id=None, event_type=None, data=None, timestamp=None, valid_at=None
    ):
        event_type = event_type or TestEvent
        metadata = {"timestamp": timestamp, "valid_at": valid_at}
        return self.mapper.event_to_record(
            event_type(
                event_id=event_id or uuid.uuid4(), data=data or {}, metadata=metadata
            )
        )

    @contextmanager
    def with_event_of_id(self, event_id):
        event = self.test_record(event_id)
        self.repository.append_to_stream([event], Stream.new())
        yield event
