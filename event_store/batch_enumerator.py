from dataclasses import dataclass
from typing import Any, Callable, Iterable


@dataclass
class BatchIterator:
    batch_size: int
    total_limit: int
    reader: Callable[[int, int], Iterable]
    _batch_offset: int = 0

    def __iter__(self):
        return self

    def __next__(self):
        batch_limit = min([self.batch_size, self.total_limit - self._batch_offset])
        result = self.reader(self._batch_offset, batch_limit)

        if not result or self._batch_offset > self.total_limit - 1:
            raise StopIteration

        self._batch_offset += self.batch_size
        return result
