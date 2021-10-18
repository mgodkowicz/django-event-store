import pytest

from in_memory_repository import InMemoryRepository
from mappers.default import Default
from specification import Specification, SpecificationResult
from specification_reader import SpecificationReader
from stream import Stream


@pytest.fixture
def repository():
    return InMemoryRepository()


@pytest.fixture
def mapper():
    return Default()


@pytest.fixture
def specification(repository, mapper):
    return Specification(SpecificationReader(repository, mapper), SpecificationResult(Stream.new()))
