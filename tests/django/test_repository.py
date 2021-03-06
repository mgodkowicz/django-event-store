import uuid
from datetime import datetime
from uuid import uuid4

import pytest

from django_event_store.event_repository import DjangoEventRepository
from django_event_store.models import Event as EventModel
from django_event_store.models import EventsInStreams
from event_store import Event, EventNotFound
from event_store.exceptions import WrongExpectedEventVersion
from event_store.expected_version import ExpectedVersion
from event_store.specification import Specification, SpecificationResult
from event_store.specification_reader import SpecificationReader
from event_store.stream import Stream


class Type1(Event):
    pass


class Type2(Event):
    pass


class Type3(Event):
    pass


@pytest.fixture
def django_repository():
    return DjangoEventRepository()


@pytest.mark.django_db
def test_read_in_batches_forward(django_repository, record, specification):
    records = [record() for _ in range(20)]
    django_repository.append_to_stream(
        records,
        Stream.new(),
    )

    batches = django_repository.read(
        specification.forward().limit(11).in_batches(10).result
    )

    assert len(batches) == 2
    assert len(batches[0]) == 10
    assert len(batches[1]) == 1
    assert batches[0] == records[:10]
    assert batches[1] == [records[10]]


@pytest.mark.django_db
def test_read_in_batches_backward(django_repository, record, specification):
    records = [record() for _ in range(20)]
    django_repository.append_to_stream(
        records,
        Stream.new(),
    )

    batches = django_repository.read(
        specification.backward().limit(11).in_batches(10).result
    )

    assert len(batches) == 2
    assert len(batches[0]) == 10
    assert len(batches[1]) == 1
    assert batches[0] == records[20:9:-1]
    assert batches[1] == [records[9]]


@pytest.mark.django_db
def test_read_in_batches_forward_from_named_stream(
    django_repository, record, specification
):
    records = [record() for _ in range(40)]
    django_repository.append_to_stream(
        records[:20],
        Stream.new("bazinga!"),
    )
    django_repository.append_to_stream(
        records[20:40],
        Stream.new(),
    )
    batches = django_repository.read(
        specification.stream("bazinga!").forward().limit(11).in_batches(10).result
    )

    assert len(batches) == 2
    assert len(batches[0]) == 10
    assert len(batches[1]) == 1
    assert batches[0] == records[:10]
    assert batches[1] == [records[10]]


def test_use_default_models():
    repository = DjangoEventRepository()

    assert isinstance(repository.event_class(), EventModel)
    assert isinstance(repository.stream_class(), EventsInStreams)


def test_allows_custom_base_class():
    pass


@pytest.fixture()
def event0(record):
    return record()


@pytest.fixture()
def event1(record):
    return record()


@pytest.fixture()
def event2(record):
    return record()


@pytest.fixture()
def event3(record):
    return record()


@pytest.fixture()
def event4(record):
    return record()


@pytest.fixture
def specification_with_orm(django_repository, mapper):
    return Specification(
        SpecificationReader(django_repository, mapper),
        SpecificationResult(Stream.new()),
    )


@pytest.mark.django_db
class TestRepository:
    global_stream = Stream.new()
    stream = Stream.new(str(uuid.uuid4()))
    stream_flow = Stream.new("stream_flow")

    @pytest.fixture(autouse=True)
    def repository(self, django_repository):
        self.repository = django_repository

    @pytest.fixture(autouse=True)
    def _specification(self, specification_with_orm):
        self.specification = specification_with_orm

    @pytest.fixture(autouse=True)
    def _record(self, record):
        self.record = record

    def read_events(
        self, repository, scope, stream=None, start_from=None, to=None, count=None
    ):
        if stream:
            scope = scope.stream(stream.name)
        if start_from:
            scope = scope.start_from(start_from)
        if to:
            scope = scope.to(to)
        if count:
            scope = scope.limit(count)

        return repository.read(scope.result)

    def read_events_forward(
        self, repository, stream=None, start_from=None, to=None, count=None
    ):
        return self.read_events(
            repository, self.specification, stream, start_from, to, count
        )

    def read_events_backward(
        self, repository, stream=None, start_from=None, to=None, count=None
    ):
        return self.read_events(
            repository, self.specification.backward(), stream, start_from, to, count
        )

    def test_append_to_stream_returns_self(self):
        self.repository.append_to_stream(
            [self.record()], self.stream, ExpectedVersion.none()
        ).append_to_stream([self.record()], self.stream, ExpectedVersion(0))

    def test_link_to_stream_returns_self(self):
        event1 = self.record()
        event2 = self.record()
        self.repository.append_to_stream([event1, event2], self.stream).link_to_stream(
            [event1.event_id], self.stream_flow
        ).link_to_stream([event2.event_id], Stream.new())

    def test_add_initial_event_to_new_stream(self):
        event = self.record()
        self.repository.append_to_stream([event], self.stream)
        assert self.read_events_forward(self.repository) == [event]

    def test_add_multiple_initial_event_to_new_stream(self):
        event = self.record()
        event2 = self.record()
        self.repository.append_to_stream([event, event2], self.stream)
        assert self.read_events_forward(self.repository) == [event, event2]

    def test_link_multiple_initial_events_to_a_new_stream(self):
        event = self.record()
        event2 = self.record()

        self.repository.append_to_stream([event, event2], self.stream).link_to_stream(
            [event.event_id, event2.event_id], self.stream_flow
        )

        assert self.read_events_forward(self.repository, self.stream) == [event, event2]
        assert self.read_events_forward(self.repository, self.stream_flow) == [
            event,
            event2,
        ]

    def test_correct_expected_version_on_second_write(self):
        event0 = self.record()
        event1 = self.record()
        event2 = self.record()
        event3 = self.record()

        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.none()
        )
        self.repository.append_to_stream(
            [event2, event3], self.stream, ExpectedVersion(1)
        )

        assert self.read_events_forward(self.repository, count=4) == [
            event0,
            event1,
            event2,
            event3,
        ]
        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event1,
            event2,
            event3,
        ]

    def test_correct_expected_version_on_second_link(self):
        event0 = self.record()
        event1 = self.record()
        event2 = self.record()
        event3 = self.record()

        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.none()
        ).append_to_stream(
            [event2, event3], self.stream_flow, ExpectedVersion.none()
        ).link_to_stream(
            [event0.event_id, event1.event_id], self.stream_flow, ExpectedVersion(1)
        )

        assert self.read_events_forward(self.repository, count=4) == [
            event0,
            event1,
            event2,
            event3,
        ]
        assert self.read_events_forward(self.repository, self.stream_flow) == [
            event2,
            event3,
            event0,
            event1,
        ]

    def test_incorrect_expected_version_on_second_write(self):
        event0 = self.record()
        event1 = self.record()
        event2 = self.record()
        event3 = self.record()

        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.none()
        )
        with pytest.raises(WrongExpectedEventVersion):
            self.repository.append_to_stream(
                [event2, event3], self.stream, ExpectedVersion(0)
            )

        assert self.read_events_forward(self.repository, count=4) == [event0, event1]
        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event1,
        ]

    def test_incorrect_expected_version_on_second_link(self):
        event0 = self.record()
        event1 = self.record()
        event2 = self.record()
        event3 = self.record()

        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.none()
        ).append_to_stream([event2, event3], self.stream_flow, ExpectedVersion.none())
        with pytest.raises(WrongExpectedEventVersion):
            self.repository.link_to_stream(
                [event2.event_id, event3.event_id], self.stream, ExpectedVersion(0)
            )

        assert self.read_events_forward(self.repository, count=4) == [
            event0,
            event1,
            event2,
            event3,
        ]
        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event1,
        ]

    def test_none_on_first_and_subsequent_write(self):
        event0 = self.record()
        event1 = self.record()

        self.repository.append_to_stream([event0], self.stream, ExpectedVersion.none())

        with pytest.raises(WrongExpectedEventVersion):
            self.repository.append_to_stream(
                [event1], self.stream, ExpectedVersion.none()
            )

        assert self.read_events_forward(self.repository, count=1) == [event0]

    def test_none_on_first_and_subsequent_link(self):
        event0 = self.record()
        event1 = self.record()

        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.none()
        )

        with pytest.raises(WrongExpectedEventVersion):
            self.repository.link_to_stream(
                [event1.event_id], self.stream, ExpectedVersion.none()
            )

        assert self.read_events_forward(self.repository, count=1) == [event0]

    def test_any_allows_stream_with_best_effort_order_and_no_guarantee(self):
        event0 = self.record()
        event1 = self.record()
        event2 = self.record()
        event3 = self.record()

        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.any()
        )
        self.repository.append_to_stream(
            [event2, event3], self.stream, ExpectedVersion.any()
        )

        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event1,
            event2,
            event3,
        ]

    def test_any_allows_linking_in_stream_with_best_effort_order_and_no_guarantee(self):
        event0 = self.record()
        event1 = self.record()
        event2 = self.record()
        event3 = self.record()

        self.repository.append_to_stream(
            [event0, event1, event2, event3], self.stream, ExpectedVersion.any()
        )

        self.repository.link_to_stream(
            [event0.event_id, event1.event_id], self.stream_flow, ExpectedVersion.any()
        )
        self.repository.link_to_stream(
            [event2.event_id, event3.event_id], self.stream_flow, ExpectedVersion.any()
        )

        assert self.read_events_forward(self.repository, count=4) == [
            event0,
            event1,
            event2,
            event3,
        ]
        assert self.read_events_forward(self.repository, self.stream_flow) == [
            event0,
            event1,
            event2,
            event3,
        ]

    def test_auto_queries_for_last_position_in_given_stream(self):
        self.repository.append_to_stream(
            [
                self.record(),
                self.record(),
                self.record(),
            ],
            self.stream_flow,
            ExpectedVersion.auto(),
        )
        self.repository.append_to_stream(
            [self.record(), self.record()], self.stream, ExpectedVersion.auto()
        )
        self.repository.append_to_stream(
            [self.record(), self.record()], self.stream, ExpectedVersion(1)
        )

    def test_auto_queries_for_last_position_in_given_stream_when_linking(self):
        event0 = self.record()
        event1 = self.record()
        event2 = self.record()
        self.repository.append_to_stream(
            [
                event0,
                event1,
                event2,
            ],
            self.stream_flow,
            ExpectedVersion.auto(),
        )
        self.repository.append_to_stream(
            [self.record(), self.record()], self.stream, ExpectedVersion.auto()
        )
        self.repository.link_to_stream(
            [event0.event_id, event1.event_id, event2.event_id],
            self.stream,
            ExpectedVersion(1),
        )

    def test_auto_starts_from_0(self):
        self.repository.append_to_stream(
            [self.record()], self.stream, ExpectedVersion.auto()
        )
        with pytest.raises(WrongExpectedEventVersion):
            self.repository.append_to_stream(
                [self.record()], self.stream, ExpectedVersion.none()
            )

    def test_auto_linking_starts_from_0(self):
        event0 = self.record()
        self.repository.append_to_stream(
            [event0], self.stream_flow, ExpectedVersion.auto()
        )

        self.repository.link_to_stream(
            [event0.event_id], self.stream, ExpectedVersion.auto()
        )

        with pytest.raises(WrongExpectedEventVersion):
            self.repository.append_to_stream(
                [self.record()], self.stream, ExpectedVersion.none()
            )

    def test_auto_queries_for_last_position_and_follows_in_incremental_way(
        self, event0, event1, event2, event3
    ):
        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.auto()
        )
        self.repository.append_to_stream(
            [event2, event3], self.stream, ExpectedVersion.auto()
        )

        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event1,
            event2,
            event3,
        ]

    def test_auto_queries_for_last_position_and_follows_in_incremental_way_when_linking(
        self,
    ):
        pass

    def test_auto_is_compatible_with_manual_expectation(
        self, event0, event1, event2, event3
    ):
        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.auto()
        )
        self.repository.append_to_stream(
            [event2, event3], self.stream, ExpectedVersion(1)
        )

        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event1,
            event2,
            event3,
        ]

    def test_auto_is_compatible_with_manual_expectation_when_linking(
        self, event0, event1
    ):
        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.auto()
        )
        self.repository.link_to_stream(
            [event0.event_id], self.stream_flow, ExpectedVersion.auto()
        )
        self.repository.link_to_stream(
            [event1.event_id], self.stream_flow, ExpectedVersion(0)
        )

        assert self.read_events_forward(self.repository, self.stream_flow) == [
            event0,
            event1,
        ]

    def test_manual_is_compatible_with_auto_expectation(
        self, event0, event1, event2, event3
    ):
        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.none()
        )
        self.repository.append_to_stream(
            [event2, event3], self.stream, ExpectedVersion.auto()
        )

        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event1,
            event2,
            event3,
        ]

    def test_manual_is_compatible_with_auto_expectation_when_linking(
        self, event0, event1
    ):
        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.auto()
        )
        self.repository.link_to_stream(
            [event0.event_id], self.stream_flow, ExpectedVersion.none()
        )
        self.repository.link_to_stream(
            [event1.event_id], self.stream_flow, ExpectedVersion.auto()
        )

        assert self.read_events_forward(self.repository, self.stream_flow) == [
            event0,
            event1,
        ]

    def test_should_has_event_even_after_removing_stream(self, event0):
        self.repository.append_to_stream([event0], self.stream)

        assert self.repository.has_event(event0.event_id) is True
        assert self.repository.has_event(str(uuid.uuid4())) is False

        self.repository.delete_stream(self.stream)
        assert self.repository.has_event(event0.event_id) is True

    def test_data_attributes_are_retrieved(self):
        event = self.record(data={"order_id": 2})
        self.repository.append_to_stream([event], self.stream)

        retrieved_event = self.read_events_forward(self.repository, count=1)[0]

        assert retrieved_event.data == {"order_id": 2}

    def test_metadata_attributes_are_retrieved(self):
        event = self.record(metadata={"request_id": 2})
        self.repository.append_to_stream([event], self.stream)

        retrieved_event = self.read_events_forward(self.repository, count=1)[0]

        assert retrieved_event.metadata == {"request_id": 2}

    def test_position_in_stream(self, event0, event1):
        self.repository.append_to_stream(
            [event0, event1], self.stream, ExpectedVersion.auto()
        )

        assert self.repository.position_in_stream(event0.event_id, self.stream) == 0
        assert self.repository.position_in_stream(event1.event_id, self.stream) == 1

    def test_position_in_stream_event_not_found(self, event0):
        with pytest.raises(EventNotFound):
            self.repository.position_in_stream(event0.event_id, self.stream)

    def test_position_in_stream_when_event_published_without_position(self, event0):
        self.repository.append_to_stream([event0], self.stream, ExpectedVersion.any())

        assert self.repository.position_in_stream(event0.event_id, self.stream) is None

    def test_read_events_backward(self, event0, event2, event1):
        self.repository.append_to_stream([event0, event1, event2], self.stream)

        assert self.read_events_backward(self.repository, self.stream) == [
            event2,
            event1,
            event0,
        ]

    def test_read_batch_of_events_from_stream_forward_and_backward(self):
        events = [self.record(event_id=uuid4()) for _ in range(10)]
        self.repository.append_to_stream(events, self.stream, ExpectedVersion.none())

        assert (
            self.read_events_forward(self.repository, self.stream, count=3)
            == events[:3]
        )
        assert (
            self.read_events_forward(self.repository, self.stream, count=100) == events
        )
        assert (
            self.read_events_forward(
                self.repository, self.stream, start_from=events[4].event_id
            )
            == events[5:]
        )
        assert (
            self.read_events_forward(
                self.repository, self.stream, start_from=events[4].event_id, count=4
            )
            == events[5:9]
        )
        assert (
            self.read_events_forward(
                self.repository, self.stream, start_from=events[4].event_id, count=100
            )
            == events[5:]
        )
        assert (
            self.read_events_forward(
                self.repository, self.stream, to=events[4].event_id, count=3
            )
            == events[:3]
        )
        assert (
            self.read_events_forward(
                self.repository, self.stream, to=events[4].event_id, count=100
            )
            == events[:4]
        )

        assert self.read_events_backward(self.repository, self.stream, count=3) == list(
            reversed(events[7:10])
        )
        assert (
            self.read_events_backward(self.repository, self.stream, count=100)
            == events[::-1]
        )
        assert self.read_events_backward(
            self.repository, self.stream, start_from=events[4].event_id, count=4
        ) == list(reversed(events[0:4]))
        assert self.read_events_backward(
            self.repository, self.stream, start_from=events[4].event_id, count=100
        ) == list(
            reversed(events[0:4])
        )  #
        assert self.read_events_backward(
            self.repository, self.stream, to=events[4].event_id, count=4
        ) == list(reversed(events[6:10]))
        assert self.read_events_backward(
            self.repository, self.stream, to=events[4].event_id, count=100
        ) == list(reversed(events[5:10]))

    def test_read_all_streams_events_forward_and_backward(
        self, event0, event1, event2, event3, event4
    ):
        self.repository.append_to_stream([event0], self.stream, ExpectedVersion.none())
        self.repository.append_to_stream(
            [event1], self.stream_flow, ExpectedVersion.none()
        )
        self.repository.append_to_stream([event2], self.stream, ExpectedVersion(0))
        self.repository.append_to_stream([event3], self.stream_flow, ExpectedVersion(0))
        self.repository.append_to_stream([event4], self.stream_flow, ExpectedVersion(1))

        assert self.read_events_forward(self.repository, self.stream) == [
            event0,
            event2,
        ]
        assert self.read_events_backward(self.repository, self.stream) == [
            event2,
            event0,
        ]

    def test_read_batch_of_events_from_all_streams_forward_and_backward(self):
        events = [self.record(event_id=uuid4()) for _ in range(10)]
        self.repository.append_to_stream(
            events, Stream.new(str(uuid.uuid4())), ExpectedVersion.none()
        )

        assert self.read_events_forward(self.repository, count=3) == events[:3]
        assert self.read_events_forward(self.repository, count=100) == events
        assert (
            self.read_events_forward(self.repository, start_from=events[4].event_id)
            == events[5:]
        )
        assert (
            self.read_events_forward(
                self.repository, start_from=events[4].event_id, count=4
            )
            == events[5:9]
        )
        assert (
            self.read_events_forward(
                self.repository, start_from=events[4].event_id, count=100
            )
            == events[5:]
        )
        assert (
            self.read_events_forward(self.repository, to=events[4].event_id, count=3)
            == events[:3]
        )
        assert (
            self.read_events_forward(self.repository, to=events[4].event_id, count=100)
            == events[:4]
        )

        assert self.read_events_backward(self.repository, count=3) == list(
            reversed(events[7:10])
        )
        assert self.read_events_backward(self.repository, count=100) == events[::-1]
        assert self.read_events_backward(
            self.repository, start_from=events[4].event_id, count=4
        ) == list(reversed(events[0:4]))
        assert self.read_events_backward(
            self.repository, start_from=events[4].event_id, count=100
        ) == list(reversed(events[0:4]))
        assert self.read_events_backward(
            self.repository, to=events[4].event_id, count=4
        ) == list(reversed(events[6:10]))
        assert self.read_events_backward(
            self.repository, to=events[4].event_id, count=100
        ) == list(reversed(events[5:10]))

    def test_read_events_with_specific_id_from_global_scope(
        self, event0, event1, event2
    ):
        self.repository.append_to_stream(
            [event2, event0], self.stream, ExpectedVersion.any()
        )

        spec = self.specification.with_ids([event0.event_id]).read_first().result
        assert self.repository.read(spec) == event0

        spec = self.specification.with_ids([event2.event_id]).read_first().result
        assert self.repository.read(spec) == event2

        spec = self.specification.with_ids([event1.event_id]).read_first().result
        assert self.repository.read(spec) is None

        spec = self.specification.with_ids([]).result
        assert self.repository.read(spec) == []

        spec = (
            self.specification.with_ids([event2.event_id, event0.event_id])
            .in_batches(2)
            .result
        )
        assert self.repository.read(spec)[0] == [event2, event0]

    def test_read_events_with_specific_id_from_local_scope(
        self, event0, event1, event2
    ):
        self.repository.append_to_stream(
            [event2, event0], self.stream, ExpectedVersion.any()
        )

        spec = (
            self.specification.stream(self.stream.name)
            .with_ids([event0.event_id])
            .read_first()
            .result
        )
        assert self.repository.read(spec) == event0

        spec = (
            self.specification.stream(self.stream.name)
            .with_ids([event2.event_id])
            .read_first()
            .result
        )
        assert self.repository.read(spec) == event2

        spec = (
            self.specification.stream(self.stream.name)
            .with_ids([event1.event_id])
            .read_first()
            .result
        )
        assert self.repository.read(spec) is None

        spec = self.specification.stream(self.stream.name).with_ids([]).result
        assert self.repository.read(spec) == []

        spec = (
            self.specification.stream(self.stream.name)
            .with_ids([event2.event_id, event0.event_id])
            .in_batches(2)
            .result
        )
        assert self.repository.read(spec)[0] == [event2, event0]

    def test_read_events_of_type_from_global_scope(self):
        event1 = self.record(event_type=Type1.__name__)
        event2 = self.record(event_type=Type2.__name__)
        event3 = self.record(event_type=Type1.__name__)
        self.repository.append_to_stream(
            [event1, event2, event3], self.stream, ExpectedVersion.any()
        )

        assert self.repository.read(self.specification.of_types([Type1]).result) == [
            event1,
            event3,
        ]
        assert self.repository.read(self.specification.of_type(Type2).result) == [
            event2
        ]
        assert self.repository.read(self.specification.of_types([Type3]).result) == []

    def test_read_events_of_type_from_local_scope(self):
        event1 = self.record(event_type=Type1.__name__)
        event2 = self.record(event_type=Type2.__name__)
        event3 = self.record(event_type=Type1.__name__)
        self.repository.append_to_stream(
            [event1, event2, event3], self.stream, ExpectedVersion.any()
        )

        assert self.repository.read(
            self.specification.stream(self.stream.name).of_types([Type1]).result
        ) == [event1, event3]
        assert self.repository.read(
            self.specification.stream(self.stream.name).of_type(Type2).result
        ) == [event2]
        assert (
            self.repository.read(
                self.specification.stream(self.stream.name).of_types([Type3]).result
            )
            == []
        )

    def test_time_order_is_respected(self):
        event1 = self.record(
            timestamp=datetime(2021, 1, 1), valid_at=datetime(2021, 1, 9)
        )
        event2 = self.record(
            timestamp=datetime(2021, 1, 3), valid_at=datetime(2021, 1, 6)
        )
        event3 = self.record(
            timestamp=datetime(2021, 1, 2), valid_at=datetime(2021, 1, 3)
        )

        self.repository.append_to_stream([event1, event2, event3], self.stream)

        assert self.repository.read(self.specification.result) == [
            event1,
            event2,
            event3,
        ]
        assert self.repository.read(self.specification.as_at().result) == [
            event1,
            event3,
            event2,
        ]
        assert self.repository.read(self.specification.as_at().backward().result) == [
            event2,
            event3,
            event1,
        ]
        assert self.repository.read(self.specification.as_of().result) == [
            event3,
            event2,
            event1,
        ]
        assert self.repository.read(self.specification.as_of().backward().result) == [
            event1,
            event2,
            event3,
        ]

    def test_time_order_is_respected_in_local_scope(self):
        event1 = self.record(
            timestamp=datetime(2021, 1, 1), valid_at=datetime(2021, 1, 9)
        )
        event2 = self.record(
            timestamp=datetime(2021, 1, 3), valid_at=datetime(2021, 1, 6)
        )
        event3 = self.record(
            timestamp=datetime(2021, 1, 2), valid_at=datetime(2021, 1, 3)
        )

        self.repository.append_to_stream([event1, event2, event3], self.stream)

        assert self.repository.read(
            self.specification.stream(self.stream.name).result
        ) == [
            event1,
            event2,
            event3,
        ]
        assert self.repository.read(
            self.specification.stream(self.stream.name).as_at().result
        ) == [
            event1,
            event3,
            event2,
        ]
        assert self.repository.read(
            self.specification.stream(self.stream.name).as_at().backward().result
        ) == [
            event2,
            event3,
            event1,
        ]
        assert self.repository.read(
            self.specification.stream(self.stream.name).as_of().result
        ) == [
            event3,
            event2,
            event1,
        ]
        assert self.repository.read(
            self.specification.stream(self.stream.name).as_of().backward().result
        ) == [
            event1,
            event2,
            event3,
        ]


def unlimited_concurrency_for_any_everything_should_succeed():
    pass


def unlimited_concurrency_for_any_everything_should_succeed_when_linking():
    pass


def limited_concurrency_for_auto_some_operations_will_fail_without_outside_lock():
    pass


def events_not_persisted_if_append_failed():
    pass


def linking_non_existent_event():
    pass


def read_returns_enumerator():
    pass


def can_store_arbitrary_binary_data():
    pass


def changes_events():
    pass


def cannot_change_unexisting_event():
    pass


def does_not_change_timestamp():
    pass


def timestamp_precision():
    pass


def fetching_records_older_than_specified_date_in_stream():
    pass


def fetching_records_older_than_or_equal_to_specified_date_in_stream():
    pass


def fetching_records_newer_than_specified_date_in_stream():
    pass


def fetching_records_newer_than_or_equal_to_specified_date_in_stream():
    pass


def fetching_records_older_than_specified_date():
    pass


def fetching_records_older_than_or_equal_to_specified_date():
    pass


def fetching_records_newer_than_specified_date():
    pass


def fetching_records_newer_than_or_equal_to_specified_date():
    pass


def fetching_records_from_disjoint_periods():
    pass


def fetching_records_within_time_range():
    pass


def time_order_is_respected():
    pass


def time_order_is_respected_with_batches():
    pass
