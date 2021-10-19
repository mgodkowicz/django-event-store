import pytest

from event_store.in_memory_repository import InMemoryRepository
from event_store.mappers.default import Default
from event_store.specification import Specification, SpecificationResult
from event_store.specification_reader import SpecificationReader
from event_store.stream import Stream


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
