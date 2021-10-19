import math
from copy import copy
from dataclasses import dataclass
from typing import Optional

from exceptions import EventNotFound, InvalidPageSize
from specification_reader import SpecificationReader
from stream import Stream


@dataclass
class SpecificationResult:
    stream: Stream = Stream.new()
    start: Optional[str] = None
    stop: Optional[str] = None
    direction: str = "forward"  # FIXME enum?
    count: int = math.inf
    read_as: str = "all"
    batch_size: int = 100
    # count: Optional[None] = None

    @property
    def limited(self):
        return self.count != math.inf

    @property
    def limit(self):
        return self.count

    @property
    def forward(self):
        return self.direction == "forward"

    @property
    def backward(self):
        return self.direction == "backward"

    @property
    def all(self):
        return self.read_as == "all"

    @property
    def batched(self):
        return self.read_as == "batched"

    @property
    def first(self):
        return self.read_as == "first"

    @property
    def last(self):
        return self.read_as == "last"

    # @property
    # def limit(self):
    #     return self.limit or math.inf
    # start
    # stop
    # older_than
    # older_than_or_equal
    # newer_than
    # newer_than_or_equal
    # time_sort_by
    # count
    # stream
    # read_as
    # batch_size
    # with_ids
    # with_types


class InvalidPageStart(Exception):
    pass


class Specification:
    DEFAULT_BATCH_SIZE = 100

    def __init__(
        self, reader: SpecificationReader, result: Optional[SpecificationResult] = None
    ):
        self.reader = reader
        self.result = result or SpecificationResult()

    def _new(self, **kwargs):
        new_result = copy(self.result)
        for key, value in kwargs.items():
            setattr(new_result, key, value)
        return Specification(self.reader, new_result)

    def stream(self, stream_name: str) -> "Specification":
        """
        Limits the query to certain stream.
        """
        new_result = copy(self.result)
        new_result.stream = Stream(stream_name)
        return Specification(self.reader, new_result)

    def start_from(self, start: str) -> "Specification":
        """
        Limits the query to events before or after another event.

        :return:
        """
        if not start:
            raise InvalidPageStart

        if not self.reader.has_event(start):
            raise EventNotFound

        return self._new(start=start)

    def to(self, stop: str) -> "Specification":
        """
         Limits the query to events before or after another event.

        :return:
        """
        if not stop:
            raise InvalidPageStart

        if not self.reader.has_event(stop):
            raise EventNotFound

        return self._new(stop=stop)

    def limit(self, count: int) -> "Specification":
        try:
            if not count or count < 0:
                raise InvalidPageSize
        except Exception:
            raise InvalidPageSize("Page size has to be integer bigger than 0.")

        return self._new(count=count)

    def count(self) -> int:
        return self.reader.count(self.result)

    def each_batch(self):
        # return to_enum(:each_batch) unless block_given?
        for batch in self.reader.each(self.in_batches(self.result.batch_size).result):
            yield batch

    def each(self):
        for batch in self.each_batch():
            for event in batch:
                yield event

    def in_batches(self, batch_size=DEFAULT_BATCH_SIZE):
        return self._new(read_as="batch", batch_size=batch_size)

    def forward(self):
        return self._new(direction="forward")

    def backward(self):
        return self._new(direction="backward")

    def to_list(self) -> list:
        return list(self.each())
