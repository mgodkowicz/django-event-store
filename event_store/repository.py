from abc import ABC, abstractmethod
from typing import List

from record import Record
from specification import SpecificationResult
from stream import Stream

Records = List[Record]


class EventsRepository(ABC):
    @abstractmethod
    def append_to_stream(
        self, records: Records, stream: Stream, expected_version=None
    ) -> "EventsRepository":
        pass

    @abstractmethod
    def link_to_stream(
        self, event_ids: List[str], stream: Stream, expected_version
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
