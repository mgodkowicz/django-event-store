import uuid
from collections import Iterable
from typing import Callable, List

from broker import Broker
from dispatcher import Dispatcher
from specification import Specification
from specification_reader import SpecificationReader
from stream import GLOBAL_STREAM, Stream
from subscriptions import Subscriptions


class Client:
    def __init__(self, repository=None, subscriptions=None, dispatcher=Dispatcher(), mapper=None):
        self.repository = repository
        self.subscriptions = subscriptions or Subscriptions()
        self.broker = Broker(self.subscriptions, dispatcher)
        self.correlation_id_generator = uuid.uuid4
        self.mapper = mapper

    def publish(self, *events, stream_name=GLOBAL_STREAM, expected_version=None):
        records = self.transform(events)
        self.append_records_to_stream(list(records), stream_name, expected_version)
        for event in events:
            self.broker.call(event, None)
        return self

    def append(self, *events, stream_name=GLOBAL_STREAM, expected_version=None):
        records = self.transform(events)
        self.append_records_to_stream(records, stream_name, expected_version)
        return self

    def read(self):
        return Specification(SpecificationReader(self.repository, self.mapper))

    def subscribe(self, subscriber: Callable, to: List):
        if not isinstance(to, Iterable):
            to = [to]
        self.broker.add_subscription(subscriber, to)
        return self

    def append_records_to_stream(self, records, stream_name, expected_version):
        self.repository.append_to_stream(records, Stream.new(stream_name), None)#ExpectedVersion.new(expected_version))

    def transform(self, events):
        return [
            self.mapper.event_to_record(event) for event in events
        ]

