from typing import Callable, Iterable

from dispatcher import Dispatcher
from event import Event
from subscriptions import Subscriptions


class Broker:
    def __init__(self, subscriptions: Subscriptions, dispatcher: Dispatcher):
        self.subscriptions = subscriptions
        self.dispatcher = dispatcher

    # def __call__(self, *args, **kwargs):
    def call(self, event: Event, record):
        subscribers = self.subscriptions.all_for(event.event_type)
        for subscriber in subscribers:
            self.dispatcher(subscriber, event, record)

    def add_subscription(self, subscriber: Callable, event_types: Iterable):
        # verify_subscription(subscriber)
        if not callable(subscriber):
            raise TypeError("Handler have to be callable.")
        self.subscriptions.add_subscription(subscriber, event_types)
