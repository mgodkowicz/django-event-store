from event_store.dispatcher import DispatcherBase


class ImmediateCeleryDispatcher(DispatcherBase):
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def dispatch(self, subscriber, _, record):
        self.scheduler.call(subscriber, record)

    def verify(self, subscriber) -> bool:
        return self.scheduler.verify(subscriber)
