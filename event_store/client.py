import uuid
from collections import Iterable
from typing import Callable, List, Optional, Union

from broker import Broker
from dispatcher import Dispatcher
from event import Event
from record import Record
from repository import EventsRepository, Records
from specification import Specification
from specification_reader import SpecificationReader
from stream import GLOBAL_STREAM, Stream
from subscriptions import Subscriptions

Events = Union[Event, List[Event]]


class Client:
    def __init__(
        self,
        repository: Optional[EventsRepository] = None,
        subscriptions: Optional[Subscriptions] = None,
        dispatcher: Dispatcher = Dispatcher(),
        mapper=None,
    ):
        self.repository = repository
        self.subscriptions = subscriptions or Subscriptions()
        self.broker = Broker(self.subscriptions, dispatcher)
        self.correlation_id_generator = uuid.uuid4
        self.mapper = mapper

    def publish(
        self, events: Events, stream_name: str = GLOBAL_STREAM, expected_version=None
    ) -> "Client":
        if not isinstance(events, Iterable):
            events = [events]

        records = self._transform(events)
        self.append_records_to_stream(list(records), stream_name, expected_version)
        for event in events:
            self.broker.call(event, None)
        return self

    def append(
        self,
        events: Events,
        stream_name: str = GLOBAL_STREAM,
        expected_version=None,
    ) -> "Client":
        if not isinstance(events, Iterable):
            events = [events]
        records = self._transform(events)
        self.append_records_to_stream(records, stream_name, expected_version)
        return self

    def subscribe(self, subscriber: Callable, to: List) -> "Client":
        if not isinstance(to, Iterable):
            to = [to]
        self.broker.add_subscription(subscriber, to)
        return self

    def read(self) -> Specification:
        return Specification(SpecificationReader(self.repository, self.mapper))

    def append_records_to_stream(
        self, records: Records, stream_name: str, expected_version
    ) -> None:
        self.repository.append_to_stream(
            records, Stream.new(stream_name), None
        )  # ExpectedVersion.new(expected_version))

    def delete_stream(self, stream_name: str) -> "Client":
        self.repository.delete_stream(Stream(stream_name))
        return self

    def _transform(self, events: Events) -> List[Record]:
        return [self.mapper.event_to_record(event) for event in events]
