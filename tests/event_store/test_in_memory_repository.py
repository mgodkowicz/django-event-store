import uuid

import pytest

from event_store.exceptions import EventDuplicatedInStream
from event_store.stream import Stream


def test_does_not_allow_for_duplicated_events_in_the_stream(repository, record):
    event_id = str(uuid.uuid4())
    repository.append_to_stream(
        [record()],
        Stream.new("other"),
        # ExpectedVersion
    )
    repository.append_to_stream(
        [record(event_id=event_id)],
        Stream.new("stream"),
    )
    with pytest.raises(EventDuplicatedInStream):
        repository.append_to_stream(
            [record(event_id=event_id)],
            Stream.new("stream"),
        )
