import uuid
from unittest import TestCase
from unittest.mock import Mock

from client import Client
from event import Event


class TestEvent(Event):
    pass


class EventStoreClientTest(TestCase):
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
        self.client.subscribe(handler, test_event)

        self.client.publish(test_event)

        handler.assert_called_once()

    # def test_publish_to_default_stream_when_not_specified(self):
    #     test_event = TestEvent()
    #     self.assertEqual(
    #         self.client.publish(test_event), self.client
    #     )
    #     self.assertEqual(
    #         self.client.read().limit(100), [test_event]
    #     )
