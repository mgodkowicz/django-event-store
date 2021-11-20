from abc import ABC, abstractmethod


class DispatcherBase(ABC):
    # TODO rename to dispatch for example?
    @abstractmethod
    def dispatch(self, subscriber, event, record):
        pass

    @abstractmethod
    def verify(self, subscriber) -> bool:
        pass


class Dispatcher(DispatcherBase):
    def dispatch(self, subscriber, event, _) -> None:
        subscriber = subscriber() if isinstance(subscriber, type) else subscriber
        subscriber(event)

    def verify(self, subscriber) -> bool:
        if not callable(subscriber):
            return False
        return True
