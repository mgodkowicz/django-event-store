import uuid
from datetime import datetime

import pytest

from django_event_store.models import Event


@pytest.mark.django_db
def test_django_tests():
    Event.objects.create(
        event_id=str(uuid.uuid4()),
        event_type="Event1",
        data={},
        metadata={},
        created_at=datetime.now(),
    )
