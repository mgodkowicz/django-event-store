from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from in_memory_repository import InMemoryRepository


class SpecificationReader:
    def __init__(self, repository: "InMemoryRepository", mapper):
        self.repository = repository
        self.mapper = mapper

    def one(self, specification_result):
        record = self.repository.read(specification_result)
        return self.mapper.record_to_event(record) if record else None

    def each(self, specification_result):
        for batch in self.repository.read(specification_result):
            yield [self.mapper.record_to_event(record) for record in batch]

    def count(self, specification_result):
        self.repository.count(specification_result)

    def has_event(self, event_id):
        return self.repository.has_event(event_id)
