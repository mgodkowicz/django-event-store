import uuid

import pytest

from django_event_store.event_repository import DjangoEventRepository
from django_event_store.models import Event, EventsInStreams
from event_store.exceptions import WrongExpectedEventVersion
from event_store.expected_version import ExpectedVersion
from event_store.stream import Stream


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

    assert isinstance(repository.event_class(), Event)
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


@pytest.mark.django_db
class TestRepository:
    global_stream = Stream.new()
    stream = Stream.new(str(uuid.uuid4()))
    stream_flow = Stream.new("stream_flow")

    @pytest.fixture(autouse=True)
    def repository(self, django_repository):
        self.repository = django_repository

    @pytest.fixture(autouse=True)
    def _specification(self, specification):
        self.specification = specification

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
