import uuid

import pytest

from django_event_store.models import Event


@pytest.mark.django_db
def test_django_tests():
    Event.objects.create(id=str(uuid.uuid4()))
