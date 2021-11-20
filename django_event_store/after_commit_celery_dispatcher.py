from django.db import transaction

from event_store.dispatcher import DispatcherBase


class AfterCommitCeleryDispatcher(DispatcherBase):
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def dispatch(self, subscriber, _, record):
        transaction.on_commit(lambda: self.scheduler.call(subscriber, record))

    def verify(self, subscriber) -> bool:
        return self.scheduler.verify(subscriber)
