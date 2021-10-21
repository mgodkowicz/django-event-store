from event_store.event import Event


class TestHandler:
    pass


class Test1DomainEvent(Event):
    pass


class Test2DomainEvent(Event):
    pass


def test_should_return_all_subscribed_handlers(subscription):
    handler = TestHandler()
    another_handler = TestHandler()

    subscription.add_subscription(handler, [Test1DomainEvent, Test2DomainEvent])
    subscription.add_subscription(another_handler, [Test2DomainEvent])

    assert subscription.all_for("Test1DomainEvent") == [handler]
    assert subscription.all_for("Test2DomainEvent") == [
        handler,
        another_handler,
    ]


def test_should_subscribe_by_type_of_event_which_is_string(subscription):
    handler = TestHandler()

    subscription.add_subscription(handler, ["Test1DomainEvent"])

    assert subscription.all_for("Test1DomainEvent") == [handler]


def test_should_subscribe_by_type_of_event_which_is_class(subscription):
    handler = TestHandler()

    subscription.add_subscription(handler, [Test1DomainEvent])

    assert subscription.all_for("Test1DomainEvent") == [handler]


def test_should_subscribe_to_all_events(subscription):
    handler = TestHandler()

    subscription.add_global_subscription(handler)

    assert subscription.all_for("Test") == [handler]
