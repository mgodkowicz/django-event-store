from typing import Sequence

from event_store.dispatcher import DispatcherBase


class ComposedDispatcher(DispatcherBase):
    def __init__(self, dispatchers: Sequence[DispatcherBase]):
        self.dispatchers = dispatchers

    def dispatch(self, subscriber, event, record):
        for dispatcher in self.dispatchers:
            if dispatcher.verify(subscriber):
                dispatcher.dispatch(subscriber, event, record)
                break

    def verify(self, subscriber) -> bool:
        for dispatcher in self.dispatchers:
            dispatcher.verify(subscriber)
        return True
