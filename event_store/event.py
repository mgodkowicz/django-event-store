import uuid

from typing import Optional, Any


class Event:
    def __init__(
        self,
        event_id: Optional[str] = None,
        metadata: Optional[dict] = None,
        data: Optional[dict] = None,
    ):
        self.event_id = event_id or str(uuid.uuid4())
        self.metadata = metadata or {}
        self.data = data or {}

    def __str__(self):
        return f"{self.__class__.__name__} ID: {self.event_id}"

    def __repr__(self):
        return f"{self.__class__.__name__} ID: {self.event_id}"

    def __eq__(self, other):
        return (
            isinstance(other, Event)
            and self.event_type == other.event_type
            and self.event_id == other.event_id
            and self.data == other.data
        )

    @property
    def event_type(self) -> str:
        return self.metadata.get("event_type", self.__class__.__name__)

    @property
    def timestamp(self) -> Any:
        return self.metadata.get("timestamp")

    @property
    def valid_at(self) -> Any:
        return self.metadata.get("valid_at")
