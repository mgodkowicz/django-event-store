from unittest import TestCase

from event_store.event_type_resolver import EventTypeResolver


class TestEvent:
    pass


class TestEventTypeResolver(TestCase):
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/spec/event_type_resolver_spec.rb
    """

    def test_resolves_event_type_from_class(self):
        assert EventTypeResolver.new().call(TestEvent) == "TestEvent"
