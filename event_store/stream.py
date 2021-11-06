from dataclasses import dataclass
from typing import Optional

from event_store.exceptions import IncorrectStreamData

GLOBAL_STREAM = "global_stream"


@dataclass
class Stream:
    name: str

    def __post_init__(self):
        if not self.name:
            raise IncorrectStreamData

    @property
    def is_global(self):
        return self.name == GLOBAL_STREAM

    @classmethod
    def new(cls, name: Optional[str] = None):
        return cls(name or GLOBAL_STREAM)
