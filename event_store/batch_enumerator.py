from typing import Callable, Iterable


class BatchEnumerator:
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/batch_enumerator.rb
    """

    def __init__(
        self,
        batch_size: int,
        total_limit: int,
        reader: Callable[[int, int], Iterable],
    ):
        self._batch_size = batch_size
        self._total_limit = total_limit
        self._reader = reader

    @classmethod
    def new(
        cls,
        batch_size: int,
        total_limit: int,
        reader: Callable[[int, int], Iterable],
    ):
        return cls(batch_size, total_limit, reader)

    def each(self):
        batch_offset = 0
        while batch_offset < self._total_limit:
            batch_limit = min(self._batch_size, self._total_limit - batch_offset)
            result = self._reader(batch_offset, batch_limit)

            batch_offset += self._batch_size

            if not result:
                break
            yield result

    def first(self):
        return next(self.each(), None)

    def to_a(self):
        return list(self.each())
