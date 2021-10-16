from unittest import TestCase

from event_store.event import Event
from event_store.subscriptions import Subscriptions


class TestHandler:
    def __init__(self):
        self.events = []


class Test1DomainEvent(Event):
    pass


class Test2DomainEvent(Event):
    pass


class SubscriptionsTest(TestCase):

    def setUp(self) -> None:
        self.Test1DomainEvent = Test1DomainEvent()
        self.Test2DomainEvent = Test2DomainEvent()

        self.subscription = Subscriptions()

    def test_should_return_all_subscribed_handlers(self):
        handler = TestHandler()
        another_handler = TestHandler()

        self.subscription.add_subscription(handler, [self.Test1DomainEvent, self.Test2DomainEvent])
        self.subscription.add_subscription(another_handler, [self.Test2DomainEvent])

        assert self.subscription.all_for("Test1DomainEvent") == [handler]
        assert self.subscription.all_for("Test2DomainEvent") == [handler, another_handler]

    def test_should_subscribe_by_type_of_event_which_is_string(self):
        handler = TestHandler()

        self.subscription.add_subscription(handler, ["Test1DomainEvent"])

        assert self.subscription.all_for("Test1DomainEvent") == [handler]

    def test_should_subscribe_by_type_of_event_which_is_class(self):
        handler = TestHandler()

        self.subscription.add_subscription(handler, [self.Test1DomainEvent])

        assert self.subscription.all_for("Test1DomainEvent") == [handler]
