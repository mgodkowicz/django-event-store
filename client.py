import uuid
from collections import Iterable
from typing import Callable, List

from broker import Broker
from dispatcher import Dispatcher
from subscriptions import Subscriptions


GLOBAL_STREAM = ''


class Client:
    def __init__(self, repository=None, subscriptions=None, dispatcher=Dispatcher(), mapper=None):
        self.repository = repository
        self.subscriptions = subscriptions or Subscriptions()
        self.broker = Broker(self.subscriptions, dispatcher)
        self.correlation_id_generator = uuid.uuid4
        self.mapper = mapper

    def publish(self, *events, stream_name=GLOBAL_STREAM, expected_version=None):
        for event in events:
            self.broker.call(event, None)
        return self

    def subscribe(self, subscriber: Callable, to):
        if not isinstance(to, Iterable):
            to = [to]
        self.broker.add_subscription(subscriber, to)
        return self

    # def append_records_to_stream(self, records, stream_name, expected_version):
    #     self.repository.append_to_stream(records, Stream.new(stream_name), ExpectedVersion.new(expected_version))

    def transform(self, events):
        return
