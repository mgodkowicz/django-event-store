from dataclasses import dataclass
from typing import Optional

GLOBAL_STREAM = 'global_stream'


@dataclass
class Stream:
    name: str

    @property
    def is_global(self):
        return self.name == GLOBAL_STREAM

    @classmethod
    def new(cls, name: Optional[str] = None):
        return cls(name or GLOBAL_STREAM)
