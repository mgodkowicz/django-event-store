from collections import defaultdict
from typing import Dict, TypeVar, List, Callable


class Subscriptions:
    def __init__(self, event_type_resolver=None):
        self.event_type_resolver: Callable = event_type_resolver or self._default_event_type_resolver
        self._local = LocalSubscriptions()
        self._global = GlobalSubscriptions()

    def add_subscription(self, subscriber, event_types):
        self._local.add(subscriber, self.resolve_event_types(event_types))

    def add_global_subscription(self, subscriber):
        self._global.add(subscriber)

    def all_for(self, event_type) -> list:
        return self._local.all_for(event_type)

    def _default_event_type_resolver(self, event_type):
        if isinstance(event_type, str):
            return event_type
        return str(event_type.__name__)

    def resolve_event_types(self, event_types: List) -> List:
        return [
            self.event_type_resolver(event_type)
            for event_type in event_types
        ]


EventType = TypeVar("EventType")


class LocalSubscriptions:
    def __init__(self):
        self.subscriptions: Dict[EventType, list] = defaultdict(list)

    def add(self, subscription, event_types):
        for event_type in event_types:
            self.subscriptions[event_type].append(subscription)

    def all_for(self, event_type: EventType) -> list:
        return self.subscriptions.get(event_type, [])


class GlobalSubscriptions:
    def __init__(self):
        self.subscriptions: list = []

    def add(self, subscription):
        if subscription not in self.subscriptions:
            self.subscriptions.append(subscription)

    def all_for(self, _: EventType) -> list:
        return self.subscriptions
