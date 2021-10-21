import math
import uuid
from collections import Iterable
from contextlib import contextmanager
from unittest import TestCase

import pytest
from exceptions import EventNotFound, InvalidPageSize, InvalidPageStart, InvalidPageStop
from record import Record

from event_store.event import Event
from event_store.in_memory_repository import InMemoryRepository
from event_store.mappers.default import Default
from event_store.specification import Specification, SpecificationResult
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
        with self.assertRaises(InvalidPageStop):
            self.specification.to(None)

        with self.assertRaises(InvalidPageStop):
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


@pytest.fixture()
def test_record(mapper):
    def _test_record_inner(
        event_id=None, event_type=None, data=None, timestamp=None, valid_at=None
    ) -> Record:
        event_type = event_type or TestEvent
        metadata = {"timestamp": timestamp, "valid_at": valid_at}
        return mapper.event_to_record(
            event_type(
                event_id=event_id or uuid.uuid4(), data=data or {}, metadata=metadata
            )
        )

    return _test_record_inner


@pytest.fixture()
def event_of_id(test_record, repository):
    @contextmanager
    def _with_event_of_id_inner(event_id):
        event = test_record(event_id)
        repository.append_to_stream([event], Stream.new())
        yield event

    return _with_event_of_id_inner


# @pytest.fixture
# def specification(repository, mapper):
#     return Specification(
#         SpecificationReader(repository, mapper),
#         SpecificationResult(Stream.new()),
#     )


def test_should_return_events_in_batches(repository, specification, test_record):
    events = [[test_record() for _ in range(10)] for __ in range(2)]

    for batch in events:
        repository.append_to_stream(batch, stream=Stream.new("stream"))

    assert len(list(specification.stream("stream").in_batches(10).each_batch())) == 2


def test_should_return_events_in_batches_2(repository, specification, test_record):
    events = [[test_record() for _ in range(10)] for __ in range(2)]

    for batch in events:
        repository.append_to_stream(batch, stream=Stream.new("stream"))

    assert len(specification.stream("stream").in_batches(10).execute()) == 20


def test_should_return_none_when_first_event_doesnt_exist_in_stream(
    specification, event_of_id
):
    with event_of_id("123"):
        assert specification.stream("NotExisting").first() is None


def test_should_return_none_when_last_event_doesnt_exist_in_stream(
    specification, event_of_id
):
    with event_of_id("123"):
        assert specification.stream("NotExisting").last() is None


def test_should_return_first_event_in_stream(specification, repository, test_record):
    records = [test_record() for _ in range(5)]

    repository.append_to_stream(records, Stream.new())

    assert specification.first() == TestEvent(records[0].event_id)
    assert specification.start_from(records[2].event_id).first() == TestEvent(
        records[3].event_id
    )
    assert specification.start_from(
        records[2].event_id
    ).backward().first() == TestEvent(records[1].event_id)
    assert specification.start_from(records[4].event_id).first() is None
    assert specification.start_from(records[0].event_id).backward().first() is None


def test_should_return_last_event_in_stream(specification, repository, test_record):
    records = [test_record() for _ in range(5)]

    repository.append_to_stream(records, Stream.new())

    assert specification.last() == TestEvent(records[-1].event_id)
    assert specification.start_from(records[2].event_id).last() == TestEvent(
        records[-1].event_id
    )
    assert specification.start_from(records[2].event_id).backward().last() == TestEvent(
        records[0].event_id
    )
    assert specification.start_from(records[4].event_id).last() is None
    assert specification.start_from(records[0].event_id).backward().last() is None


def test_should_return_one_event_from_repository(
    specification, repository, test_record
):
    with pytest.raises(EventNotFound):
        assert specification.event("123")

    records = [test_record() for _ in range(5)]
    repository.append_to_stream(records, Stream.new())

    assert specification.event(records[0].event_id) == TestEvent(records[0].event_id)
    assert specification.event(records[3].event_id) == TestEvent(records[3].event_id)
    assert specification.events([]).execute() == []
    three_events_ids = [records[0].event_id, records[2].event_id, records[4].event_id]
    assert specification.events(three_events_ids).execute() == [
        TestEvent(event_id) for event_id in three_events_ids
    ]
    assert specification.events([records[0].event_id, "not_existing"]).execute() == [
        TestEvent(records[0].event_id)
    ]


def test_should_count_returned_events(specification, repository, test_record):
    assert specification.count() == 0

    records = [test_record() for _ in range(3)]
    repository.append_to_stream(records, Stream.new("stream1"))

    assert specification.count() == 3

    repository.append_to_stream([test_record("123")], Stream.new("Dummy"))
    assert specification.count() == 4
    assert specification.in_batches().count() == 4
    assert specification.in_batches(2).count() == 4

    assert specification.with_ids(["123"]).count() == 1
    assert specification.with_ids(["NOT_EXISTING"]).count() == 0

    assert specification.stream("stream1").count() == 3
    assert specification.stream("Dummy").count() == 1


class OrderCreated(Event):
    pass


class ProductAdded(Event):
    pass


def test_should_be_able_to_return_events_of_given_type(
    specification, repository, test_record
):
    repository.append_to_stream(
        [test_record(event_type=TestEvent)], Stream.new("dummy")
    )
    repository.append_to_stream(
        [test_record(event_type=OrderCreated)], Stream.new("dummy")
    )
    repository.append_to_stream(
        [test_record(event_type=ProductAdded)], Stream.new("dummy")
    )
    repository.append_to_stream(
        [test_record(event_type=ProductAdded)], Stream.new("dummy")
    )

    assert specification.of_type(TestEvent).count() == 1
    assert specification.of_type(OrderCreated).count() == 1
    assert specification.of_type(ProductAdded).count() == 2

    assert specification.of_types([ProductAdded, OrderCreated]).count() == 3
    assert specification.of_type(ProductAdded, OrderCreated).count() == 3
