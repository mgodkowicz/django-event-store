import datetime
import uuid
from typing import Optional

import pytest

from event_store import InMemoryRepository, Record
from event_store.mappers.default import Default
from event_store.specification import Specification, SpecificationResult
from event_store.specification_reader import SpecificationReader
from event_store.stream import Stream


@pytest.fixture
def record():
    def _record_inner(
        event_id: Optional[str] = None,
        data: Optional[dict] = None,
        metadata: Optional[dict] = None,
        event_type: str = "FakeRecordTestEvent",
        timestamp: Optional[datetime.datetime.timestamp] = None,
        valid_at: Optional[datetime.datetime.timestamp] = None,
    ) -> Record:
        _now = datetime.datetime.now().timestamp()
        return Record(
            event_id=event_id or uuid.uuid4(),
            data=data or {},
            metadata=metadata or {},
            event_type=event_type,
            timestamp=timestamp or _now,
            valid_at=valid_at or _now,
        )

    return _record_inner


@pytest.fixture
def repository():
    return InMemoryRepository()


@pytest.fixture
def mapper():
    return Default()


@pytest.fixture
def specification(repository, mapper):
    return Specification(
        SpecificationReader(repository, mapper), SpecificationResult(Stream.new())
    )
