import datetime
import time
from unittest import TestCase

from event import Event


class TestCreated(Event): pass
class TestDeleted(Event): pass


class EventTest(TestCase):

    def test_default_values(self):
        event = TestCreated()

        self.assertIsNotNone(event.event_id)
        self.assertIsNotNone(event.data)
        self.assertIsNotNone(event.metadata)
        self.assertEqual(event.event_type, "TestCreated")
        self.assertEqual(event.data, {})
        self.assertEqual(event.metadata, {})
        self.assertIsNone(event.metadata.get("timestamp"), None)
        self.assertIsNone(event.metadata.get("valid_at"), None)

    def test_init_attributes_are_used_as_event_data(self):
        event = TestCreated(data={"sample": 123})

        self.assertEqual(event.data["sample"], 123)

    def test_initial_event_id_used_as_event_id(self):
        event = TestCreated(event_id=234)

        self.assertEqual(event.event_id, "234")
        self.assertEqual(event.data, {})

    def test_two_event_are_equal_if_their_attributes_are_equal(self):
        event_data = {"foo": "bar"}
        event_metadata = {"timestmap": datetime.datetime.utcnow()}

        event = TestCreated(event_id="1", data=event_data, metadata=event_metadata)
        same_event = TestCreated(event_id="1", data=event_data, metadata=event_metadata)

        self.assertEqual(event, same_event)

    def test_two_event_are_not_equal_if_their_payload_is_different(self):
        event_data = {"foo": "bar"}
        event_metadata = {"timestmap": datetime.datetime.utcnow()}

        event = TestCreated(event_id="1", data=event_data, metadata=event_metadata)
        different_event = TestCreated(event_id="1", data={"price": 123, **event_data}, metadata=event_metadata)

        self.assertNotEqual(event, different_event)

    def test_two_event_are_not_equal_if_their_types_are_different(self):
        event_metadata = {"timestmap": datetime.datetime.utcnow()}

        event = TestCreated(event_id="1", metadata=event_metadata)
        different_event = TestDeleted(event_id="1", metadata=event_metadata)

        self.assertNotEqual(event, different_event)

    def test_only_events_with_the_same_class_event_id_and_data_are_equal(self):
        event_1 = TestCreated()
        event_2 = TestDeleted()
        self.assertFalse(event_1 == event_2)

        event_1 = TestCreated(event_id="1", data={"test": 123})
        event_2 = TestDeleted(event_id="1", data={"test": 123})
        self.assertFalse(event_1 == event_2)

        event_1 = TestCreated(event_id="1", data={"test": 123})
        event_2 = TestCreated(event_id="1", data={"test": 234})
        self.assertFalse(event_1 == event_2)

        event_1 = TestCreated(event_id="1", data={"test": 123}, metadata={"does": "not matter"})
        event_2 = TestCreated(event_id="1", data={"test": 123}, metadata={"really": "yes"})
        self.assertTrue(event_1 == event_2)
