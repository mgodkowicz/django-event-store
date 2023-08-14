import uuid
from datetime import datetime
from typing import Optional

from frozendict import frozendict

from event_store.metadata import Metadata


class Event:
    """
    Data structure representing an event

    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/event.rb
    """

    def __init__(
        self,
        event_id: Optional[str] = None,
        metadata: Optional[dict] = None,
        data: Optional[dict] = None,
    ):
        self._event_id = str(event_id or uuid.uuid4())
        self._metadata = Metadata.new(metadata or {})
        self._data = frozendict(data or {})

    def __str__(self):
        return f"{self.__class__.__name__} ID: {self.event_id}"

    def __repr__(self):
        return f"{self.__class__.__name__} ID: {self.event_id}"

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and other.event_type == self.event_type
            and other.event_id == self.event_id
            and other.data == self.data
        )

    def __hash__(self):
        return hash((self.event_type, self.event_id, self.data)) ^ hash(self.__class__)

    @classmethod
    def new(
        cls,
        event_id: Optional[str] = None,
        metadata: Optional[dict] = None,
        data: Optional[dict] = None,
    ):
        return cls(event_id=event_id, metadata=metadata, data=data)

    @property
    def event_id(self) -> str:
        return self._event_id

    @property
    def metadata(self):
        return self._metadata

    @property
    def data(self):
        return self._data

    @property
    def message_id(self) -> str:
        return self._event_id

    @property
    def event_type(self) -> str:
        return self.metadata.get("event_type") or self.__class__.__name__

    @property
    def timestamp(self) -> datetime | None:
        return self.metadata.get("timestamp")

    @property
    def valid_at(self) -> datetime | None:
        return self.metadata.get("valid_at")

    @property
    def correlation_id(self):
        # TODO
        return
