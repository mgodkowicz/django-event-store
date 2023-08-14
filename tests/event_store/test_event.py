from datetime import datetime
from unittest import TestCase

from event_store.event import Event
from support.helpers import TimeEnrichment


class TestCreated(Event):
    pass


class TestDeleted(Event):
    pass


class TestEvent(TestCase):

    ######################################################################################################################################
    # Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/spec/event_lint.rb #
    ######################################################################################################################################

    def test_provides_event_id_as_string(self):
        event = TestCreated.new()

        self.assertIsInstance(event.event_id, str)
        self.assertNotEqual(event.event_id, "")
        self.assertNotEqual(event.event_id, None)

    def test_provides_message_id_as_string(self):
        event = TestCreated.new()

        self.assertIsInstance(event.message_id, str)
        self.assertNotEqual(event.message_id, "")
        self.assertNotEqual(event.message_id, None)

    def test_message_id_is_the_same_as_event_id(self):
        event = TestCreated.new()

        self.assertEqual(event.message_id, event.event_id)

    def test_provides_event_type_as_string(self):
        event = TestCreated.new()

        self.assertIsInstance(event.event_type, str)
        self.assertNotEqual(event.event_type, "")
        self.assertNotEqual(event.event_type, None)

    #####################################################################################################################
    # Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/spec/get_event_spec.rb #
    #####################################################################################################################

    def test_default_values(self):
        event = TestCreated.new()

        self.assertIsNotNone(event.event_id)
        self.assertIsNotNone(event.data)
        self.assertIsNotNone(event.metadata)
        self.assertEqual(event.data, {})
        self.assertEqual(event.metadata, {})
        self.assertIsNone(event.metadata.get("timestamp"))
        self.assertIsNone(event.metadata.get("valid_at"))
        self.assertIsNone(event.timestamp)
        self.assertIsNone(event.valid_at)
        self.assertEqual(event.event_type, "TestCreated")

    def test_init_attributes_are_used_as_event_data(self):
        event = TestCreated.new(data={"sample": 123})

        self.assertEqual(event.data["sample"], 123)
        self.assertEqual(event.data, {"sample": 123})

    def test_init_event_id_is_used_as_event_id(self):
        event = TestCreated.new(event_id="234")

        self.assertEqual(event.event_id, "234")

    def test_init_metadata_is_used_as_event_metadata_with_timestamp_changed(self):
        timestamp = datetime(2016, 3, 10, 15, 20)
        event = TestCreated.new(
            metadata={"created_by": "Someone", "timestamp": timestamp}
        )

        self.assertEqual(event.timestamp, timestamp)
        self.assertEqual(event.metadata["timestamp"], timestamp)
        self.assertEqual(event.metadata["created_by"], "Someone")

    def test_init_valid_at_is_used_as_event_metadata_with_validity_time_changed(self):
        valid_at = datetime(2016, 3, 10, 15, 20)
        event = TestCreated.new(
            metadata={"created_by": "Someone", "valid_at": valid_at}
        )

        self.assertEqual(event.valid_at, valid_at)
        self.assertEqual(event.metadata["valid_at"], valid_at)
        self.assertEqual(event.metadata["created_by"], "Someone")

    def test_two_events_are_not_equal_if_their_payload_is_different(self):
        event_data = {"foo": "bar"}
        event_metadata = {"timestamp": datetime.utcnow()}

        event = TestCreated.new(
            event_id="1",
            data=event_data,
            metadata=event_metadata,
        )
        different_event = TestCreated.new(
            event_id="1",
            data={"price": 123, **event_data},
            metadata=event_metadata,
        )

        self.assertNotEqual(event, different_event)

    def test_two_events_are_not_equal_if_their_types_are_different(self):
        event_data = {"foo": "bar"}
        event_metadata = {"timestamp": datetime.utcnow()}

        event = TestCreated.new(
            event_id="1",
            data=event_data,
            metadata=event_metadata,
        )
        different_event = TestDeleted.new(
            event_id="1",
            data=event_data,
            metadata=event_metadata,
        )
        self.assertNotEqual(event, different_event)

    def test_only_events_with_the_same_class_event_id_and_data_are_equal(self):
        event_1 = TestCreated.new()
        event_2 = TestCreated.new()
        self.assertNotEqual(event_1, event_2)

        event_1 = TestCreated.new(event_id="1", data={"test": 123})
        event_2 = TestDeleted.new(event_id="1", data={"test": 123})
        self.assertNotEqual(event_1, event_2)

        event_1 = TestCreated.new(event_id="1", data={"test": 123})
        event_2 = TestCreated.new(event_id="1", data={"test": 234})
        self.assertNotEqual(event_1, event_2)

        event_1 = TestCreated.new(
            event_id="1", data={"test": 123}, metadata={"does": "not matter"}
        )
        event_2 = TestCreated.new(
            event_id="1", data={"test": 123}, metadata={"really": "yes"}
        )
        self.assertEqual(event_1, event_2)

    def test_timestamp(self):
        event = TestCreated.new()
        self.assertIsNone(event.timestamp)

        TimeEnrichment.apply_to(event)
        self.assertIsNotNone(event.timestamp)
        self.assertEqual(event.timestamp, event.metadata["timestamp"])

    def test_valid_at(self):
        event = TestCreated.new()
        self.assertIsNone(event.valid_at)

        TimeEnrichment.apply_to(event)
        self.assertIsNotNone(event.valid_at)
        self.assertEqual(event.valid_at, event.metadata["valid_at"])

    def test_hash(self):
        self.assertEqual(
            hash(Event.new(event_id="1")),
            hash(Event.new(event_id="1")),
        )
        self.assertNotEqual(
            hash(Event.new(event_id="1")),
            hash(Event.new(event_id="2")),
        )
        self.assertNotEqual(
            hash(Event.new(event_id="1")),
            hash(Event.new(event_id="1", metadata={"event_type": "OtherType"})),
        )
        # TODO: 4th case

    def test_uses_metadata_and_its_restrictions(self):
        with self.assertRaises(TypeError):
            TestCreated.new(metadata={"key": object()})

        with self.assertRaises(TypeError):
            TestCreated.new(metadata={object(): "value"})

    def test_allow_overriding_event_type_when_event_class_not_to_be_found(self):
        event = Event.new(metadata={"event_type": "Doh"})
        self.assertEqual(event.event_type, "Doh")

    def test_two_events_are_not_equal_if_their_overridden_event_types_are_different(
        self,
    ):
        event_1 = Event.new(
            event_id="1",
            data={"foo": "bar"},
            metadata={"event_type": "one"},
        )
        event_2 = Event.new(
            event_id="1",
            data={"foo": "bar"},
            metadata={"event_type": "two"},
        )
        self.assertNotEqual(event_1, event_2)
