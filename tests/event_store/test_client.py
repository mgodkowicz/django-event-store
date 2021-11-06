import uuid
from unittest.mock import Mock

import pytest

from event_store.event import Event
from event_store.exceptions import (
    EventNotFound,
    IncorrectStreamData,
    InvalidPageSize,
    InvalidPageStart,
)
from event_store.stream import Stream


class TestEvent(Event):
    pass


class TestEvent2(Event):
    pass


class InvalidTestHandler:
    pass


class TestHandler:
    def __init__(self):
        self.events = []

    def __call__(self, event):
        self.events.append(event)


@pytest.fixture()
def test_event():
    return TestEvent()


def test_publish_returns_self_when_success(client):
    assert client.publish(TestEvent(), stream_name="stream") == client


def test_should_call_event_handlers_on_publish(client):
    handler = Mock()
    test_event = TestEvent()
    client.subscribe(handler, [TestEvent])

    client.publish(test_event)

    handler.assert_called_once()


def test_notifies_subscribed_handlers(client):
    test_event = TestEvent()
    test_event2 = TestEvent2()

    handler = TestHandler()
    another_handler = TestHandler()

    client.subscribe(handler, [TestEvent, TestEvent2])
    client.subscribe(another_handler, [TestEvent2])

    client.publish(test_event)
    client.publish(test_event2)

    assert handler.events == [test_event, test_event2]

    assert another_handler.events == [test_event2]


def test_raises_error_when_no_valid_method_on_handler(client):
    handler = InvalidTestHandler()

    with pytest.raises(TypeError):
        client.subscribe(handler, [TestEvent])


def test_subscribe_by_event_type_string(client, test_event):
    handler = TestHandler()

    client.subscribe(handler, ["TestEvent"])
    client.publish(test_event)

    assert handler.events == [test_event]


def test_publish_to_default_stream_when_not_specified(client):
    test_event = TestEvent()

    client.publish(test_event)

    assert client.read().limit(100).execute() == [test_event]


def test_append_to_default_stream_when_not_specified(event_store):
    test_event = TestEvent(event_id=str(uuid.uuid4()))

    event_store.append(test_event)

    assert event_store.read().limit(100).execute() == [test_event]


def test_publish_first_event_should_expect_any_stream_state(event_store):
    first_event = TestEvent()
    event_store.publish(first_event)

    assert event_store.read().execute() == [first_event]


def test_publish_next_event_should_expect_any_stream_state(event_store):
    first_event = TestEvent()
    second_event = TestEvent()
    event_store.append(first_event)
    event_store.publish(second_event)

    assert event_store.read().execute() == [first_event, second_event]


# def test_publish_first_event_should_fail_if_stream_is_not_empty(event_store):
#     first_event = TestEvent()
#     event_store.append(first_event)
#
#     with pytest.raises(WrongExpectedEventVersion):  # FIXME
#         event_store.publish(TestEvent(), expected_version=None)
#     assert event_store.read().to_list() == [first_event]


@pytest.fixture
def stream():
    return Stream.new("stream")


def test_should_append_many_events(event_store, stream):
    first_event = TestEvent()
    second_event = TestEvent()

    event_store.append(
        [first_event, second_event],
        "stream",
    )

    assert event_store.read().stream("stream").execute() == [first_event, second_event]


def test_should_read_only_up_to_page_size_from_stream(event_store):
    for _ in range(102):
        event_store.append(TestEvent(), "stream")

    # FIXME find out what's going on with PAGE_SIZE
    assert len(event_store.read().stream("stream").limit(10).execute()) == 10
    assert len(event_store.read().backward().stream("stream").limit(10).execute()) == 10


def test_should_raise_exception_when_stream_name_is_incorrect(event_store):
    with pytest.raises(IncorrectStreamData):
        event_store.read().stream(None).execute()

    with pytest.raises(IncorrectStreamData):
        event_store.read().stream("").execute()


def test_should_raise_when_event_doesnt_exist(event_store):
    with pytest.raises(EventNotFound):
        event_store.read().stream("stream_name").start_from("0")


def test_should_raise_when_event_id_is_not_given_or_invalid(event_store):
    with pytest.raises(InvalidPageStart):
        event_store.read().stream("stream_name").start_from(None)
    with pytest.raises(InvalidPageStart):
        event_store.read().stream("stream_name").start_from("")


def test_should_raise_when_event_page_size_is_invalid(event_store):
    with pytest.raises(InvalidPageSize):
        event_store.read().stream("stream_name").limit(0)

    with pytest.raises(InvalidPageSize):
        event_store.read().stream("stream_name").limit(-1)

    with pytest.raises(InvalidPageSize):
        event_store.read().stream("stream_name").limit(-1.0)


def test_should_return_all_events_ordered_forward(event_store):
    for idx in range(5):
        event_store.publish(TestEvent(event_id=str(idx)), stream_name="stream_name")

    events = event_store.read().stream("stream_name").start_from("1").limit(3).execute()

    assert events[0] == TestEvent(event_id="2")
    assert events[1] == TestEvent(event_id="3")


@pytest.fixture()
def four_events():
    return [TestEvent(event_id=str(idx)) for idx in range(4)]


def test_should_return_specified_amount_of_events_ordered_forward(
    event_store, four_events
):
    event_store.publish(four_events, stream_name="stream_name")

    events = event_store.read().stream("stream_name").start_from("1").execute()

    assert events[0] == TestEvent(event_id="2")


def test_should_return_selected_events_ordered_backward(event_store, four_events):
    event_store.publish(four_events, stream_name="stream_name")

    events = (
        event_store.read()
        .backward()
        .stream("stream_name")
        .start_from("2")
        .limit(3)
        .execute()
    )

    assert events[0] == TestEvent(event_id="1")
    assert events[1] == TestEvent(event_id="0")


def test_should_return_all_events_ordered_backward(event_store, four_events):
    event_store.publish(four_events, stream_name="stream_name")

    events = event_store.read().backward().stream("stream_name").execute()

    assert events[0] == TestEvent(event_id="3")
    assert events[1] == TestEvent(event_id="2")
    assert events[2] == TestEvent(event_id="1")
    assert events[3] == TestEvent(event_id="0")


def test_should_successfully_delete_streams_of_events(event_store):
    for _ in range(4):
        event_store.publish(TestEvent(), stream_name="test_1")
    for _ in range(4):
        event_store.publish(TestEvent(), stream_name="test_2")

    all_events = event_store.read().limit(100).execute()
    assert len(all_events) == 8

    event_store.delete_stream("test_2")
    all_events = event_store.read().limit(100).execute()
    assert len(all_events) == 8
    assert event_store.read().stream("test_2").execute() == []


def test_should_return_list_of_streams_where_event_is_stored(event_store):
    event_1 = TestEvent()
    event_2 = TestEvent()
    event_3 = TestEvent()
    event_store.publish([event_1, event_2], stream_name="stream1")
    event_store.publish([event_3], stream_name="stream2")
    event_store.link([event_1.event_id], stream_name="stream2")

    assert event_store.streams_of(event_1.event_id) == [
        Stream.new("stream1"),
        Stream.new("stream2"),
    ]
    assert event_store.streams_of(event_2.event_id) == [Stream.new("stream1")]
    assert event_store.streams_of(event_3.event_id) == [Stream.new("stream2")]
