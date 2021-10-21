import pytest
from client import Client
from subscriptions import Subscriptions


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
