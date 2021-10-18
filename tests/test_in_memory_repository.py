import datetime
import uuid
from typing import Optional

import pytest

from in_memory_repository import EventDuplicatedInStream
from record import Record
from stream import Stream


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
        return Record(
            event_id=event_id or uuid.uuid4(),
            data=data or {},
            metadata=metadata or {},
            event_type=event_type,
            timestamp=timestamp or datetime.datetime.now().timestamp(),
            valid_at=timestamp or datetime.datetime.now().timestamp(),
        )
    return _record_inner


def test_does_not_allow_for_duplicated_events_in_the_stream(repository, record):
    event_id = str(uuid.uuid4())
    repository.append_to_stream(
        [
            record()
        ],
        Stream.new("other"),
        # ExpectedVersion
    )
    repository.append_to_stream(
        [
            record(event_id=event_id)
        ],
        Stream.new("stream"),
    )
    with pytest.raises(EventDuplicatedInStream):
        repository.append_to_stream(
            [
                record(event_id=event_id)
            ],
            Stream.new("stream"),
        )
