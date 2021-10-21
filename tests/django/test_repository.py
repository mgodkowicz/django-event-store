import pytest
from stream import Stream

from django_event_store.event_repository import DjangoEventRepository


@pytest.fixture
def django_repository():
    return DjangoEventRepository()


@pytest.mark.django_db
def test_read_in_batches_forward(django_repository, record, specification):
    records = [record() for _ in range(20)]
    django_repository.append_to_stream(
        records,
        Stream.new(),
    )

    batches = django_repository.read(
        specification.forward().limit(11).in_batches(10).result
    )

    assert len(batches) == 2
