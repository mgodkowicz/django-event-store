import uuid
from abc import ABC

from typing import Optional


class Event():
    def __init__(self, event_id: Optional[str] = None, metadata: Optional[dict] = None, data: Optional[dict] = None):
        self.event_id = event_id or str(uuid.uuid4())
        self.metadata = metadata or {}
        self.data = data or {}

    def __eq__(self, other):
        return (
            isinstance(other, Event)
            and self.event_type == other.event_type
            and self.event_id == other.event_id
            and self.data == other.data
        )

    @property
    def event_type(self):
        return self.metadata.get("event_type", self.__class__.__name__)

    @property
    def timestamp(self):
        return self.metadata.get("timestamp")

    @property
    def valid_at(self):
        return self.metadata.get("valid_at")