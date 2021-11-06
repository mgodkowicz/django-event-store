from dataclasses import dataclass
from enum import Enum
from typing import Callable, Union

from event_store.stream import Stream


class Version(Enum):
    ANY = "any"
    AUTO = "auto"
    NONE = None


POSITION_DEFAULT = -1


@dataclass(frozen=True)
class ExpectedVersion:
    version: Union[Version, int]

    # def __post_init__(self):
    #
    #     raise ValueError

    @classmethod
    def none(cls) -> "ExpectedVersion":
        return cls(Version.NONE)

    @classmethod
    def any(cls) -> "ExpectedVersion":
        return cls(Version.ANY)

    @classmethod
    def auto(cls) -> "ExpectedVersion":
        return cls(Version.AUTO)

    @classmethod
    def new(cls, version: int) -> "ExpectedVersion":
        return cls(version)

    def is_none(self) -> bool:
        return self.version == Version.NONE

    def is_auto(self) -> bool:
        return self.version == Version.AUTO

    def is_any(self) -> bool:
        return self.version == Version.ANY

    def resolve_for(self, stream: Stream, resolver: Callable):
        if isinstance(self.version, int):
            return self.version
        elif self.is_none():
            return POSITION_DEFAULT
        elif self.is_auto():
            new_version = resolver(stream)
            return new_version.position if new_version else POSITION_DEFAULT
