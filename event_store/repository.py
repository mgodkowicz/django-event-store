from abc import ABC, abstractmethod
from typing import List, Optional

from event_store.expected_version import ExpectedVersion
from event_store.record import Record
from event_store.specification import SpecificationResult
from event_store.stream import Stream

Records = List[Record]


class EventsRepository(ABC):
    @abstractmethod
    def append_to_stream(
        self,
        records: Records,
        stream: Stream,
        expected_version: Optional[ExpectedVersion] = None,
    ) -> "EventsRepository":
        pass

    @abstractmethod
    def link_to_stream(
        self,
        event_ids: List[str],
        stream: Stream,
        expected_version: Optional[ExpectedVersion] = None,
    ) -> "EventsRepository":
        pass

    @abstractmethod
    def read(self, spec: SpecificationResult) -> List[Records]:
        pass

    @abstractmethod
    def has_event(self, event_id: str) -> bool:
        pass

    @abstractmethod
    def delete_stream(self, stream: Stream) -> "EventsRepository":
        pass

    @abstractmethod
    def count(self, spec: SpecificationResult) -> int:
        pass

    @abstractmethod
    def streams_of(self, event_id: str) -> list:
        pass

    def position_in_stream(self, event_id: str, stream: Stream):
        pass
