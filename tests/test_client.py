import uuid
from unittest import TestCase
from unittest.mock import Mock

import pytest

from client import Client
from event import Event


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


class EventStoreClientTest(TestCase):
    test_event = TestEvent()
    test_event2 = TestEvent2()

    def setUp(self) -> None:
        self.client = Client(
            repository=Mock(),  # Fixme
            mapper=Mock(),
        )
        self.stream = uuid.uuid4()
        self.correlation_id = uuid.uuid4()

    def test_publish_returns_self_when_success(self):
        self.assertEqual(
            self.client.publish(TestEvent(), stream_name=self.stream), self.client
        )

    def test_should_call_event_handlers_on_publish(self):
        handler = Mock()
        test_event = TestEvent()
        self.client.subscribe(handler, [TestEvent])

        self.client.publish(test_event)

        handler.assert_called_once()

    def test_notifies_subscribed_handlers(self):
        handler = TestHandler()
        another_handler = TestHandler()

        self.client.subscribe(handler, [TestEvent, TestEvent2])
        self.client.subscribe(another_handler, [TestEvent2])

        self.client.publish(self.test_event)
        self.client.publish(self.test_event2)

        self.assertEqual(handler.events, [self.test_event, self.test_event2])
        self.assertEqual(another_handler.events, [self.test_event2])

    def test_raises_error_when_no_valid_method_on_handler(self):
        handler = InvalidTestHandler()

        with self.assertRaises(TypeError):
            self.client.subscribe(handler, [TestEvent])

    def test_subscribe_by_event_type_string(self):
        handler = TestHandler()

        self.client.subscribe(handler, ["TestEvent"])
        self.client.publish(self.test_event)

        self.assertEqual(handler.events, [self.test_event])


    # def test_publish_to_default_stream_when_not_specified(self):
    #     test_event = TestEvent()
    #     self.assertEqual(
    #         self.client.publish(test_event), self.client
    #     )
    #     self.assertEqual(
    #         self.client.read().limit(100), [test_event]
    #     )


@pytest.fixture
def event_store(repository, mapper) -> Client:
    return Client(
        repository=repository,
        mapper=mapper,
    )


def test_append_to_default_stream_when_not_specified(event_store):
    test_event = TestEvent(event_id=str(uuid.uuid4()))
    event_store.append(test_event)

    assert event_store.read().limit(100).to_list() == [test_event]


def test_publish_first_event_should_expect_any_stream_state(event_store):
    first_event = TestEvent()
    event_store.publish(first_event)

    assert event_store.read().to_list() == [first_event]
