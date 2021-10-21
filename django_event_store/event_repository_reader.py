from batch_enumerator import BatchIterator
from specification import SpecificationResult


class DjangoEventRepositoryReader:
    def __init__(self, event_class, stream_class):
        self.event_class = event_class
        self.stream_class = stream_class

    def read(self, spec: SpecificationResult):
        stream = self._read_scope(spec)
        if spec.batched:

            def batch_reader(offset: int, limit: int):
                return stream[offset : offset + limit]

            return [
                batch
                for batch in BatchIterator(spec.batch_size, spec.limit, batch_reader)
            ]
        return stream

    def _read_scope(self, spec: SpecificationResult):
        stream = self.event_class.objects
        if spec.with_ids is not None:
            stream = stream.filter(event_id__in=spec.with_ids)
        if spec.with_types is not None:
            stream = stream.filter(event_type__in=spec.with_types)

        stream = self._ordered(stream, spec)
        if spec.limit:
            stream = stream.all()[: spec.limit]

        return self._order(stream, spec)

    def _ordered(self, stream, spec):
        return stream

    def _order(self, stream, spec):
        if spec.backward:
            return stream.reverse()
        return stream
