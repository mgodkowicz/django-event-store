import pytest
from client import Client
from subscriptions import Subscriptions

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


@pytest.fixture()
def subscription():
    return Subscriptions()


@pytest.fixture
def client(event_store) -> Client:
    return event_store


@pytest.fixture
def event_store(repository, mapper) -> Client:
    return Client(
        repository=repository,
        mapper=mapper,
    )
