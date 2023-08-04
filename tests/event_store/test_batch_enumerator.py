from collections.abc import Iterator
from unittest import TestCase
from unittest.mock import MagicMock

from event_store.batch_enumerator import BatchEnumerator

INFINITY = 10000000000


class TestBatchEnumerator(TestCase):
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/spec/batch_enumerator_spec.rb
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection = list(range(1, 10001))
        self.reader = lambda offset, limit: self.collection[offset:offset+limit]

    def test_each_should_return_iterator(self):
        self.assertIsInstance(
            BatchEnumerator.new(100, 900, self.reader).each(),
            Iterator
        )

    def test_batching(self):
        self.assertEqual(len(BatchEnumerator.new(100, 900, self.reader).to_a()), 9)
        self.assertEqual(len(BatchEnumerator.new(100, 901, self.reader).to_a()), 10)

        self.assertEqual(len(BatchEnumerator.new(100, 1000, self.reader).to_a()), 10)
        self.assertEqual(len(BatchEnumerator.new(100, 1000, self.reader).first()), 100)
        self.assertEqual(BatchEnumerator.new(100, 1000, self.reader).first(), list(range(1, 101)))
        self.assertEqual(len(BatchEnumerator.new(100, 1001, self.reader).to_a()), 11)

        self.assertEqual(len(BatchEnumerator.new(100, 10, self.reader).to_a()), 1)
        self.assertEqual(len(BatchEnumerator.new(100, 10, self.reader).first()), 10)
        self.assertEqual(BatchEnumerator.new(100, 10, self.reader).first(), list(range(1, 11)))

        self.assertEqual(len(BatchEnumerator.new(1, 1000, self.reader).to_a()), 1000)
        self.assertEqual(len(BatchEnumerator.new(1, 1000, self.reader).first()), 1)
        self.assertEqual(BatchEnumerator.new(1, 1000, self.reader).first(), [1])

        self.assertEqual(len(BatchEnumerator.new(100, INFINITY, self.reader).to_a()), 100)

        self.assertEqual(len(BatchEnumerator.new(100, 99, self.reader).to_a()), 1)
        self.assertEqual(len(BatchEnumerator.new(100, 99, self.reader).first()), 99)

        self.assertEqual(len(BatchEnumerator.new(100, 199, self.reader).to_a()), 2)
        self.assertEqual(len(BatchEnumerator.new(100, 199, self.reader).first()), 100)
        self.assertEqual(BatchEnumerator.new(100, 199, self.reader).first(), self.collection[:100])
        self.assertEqual(len(BatchEnumerator.new(100, 199, self.reader).to_a()[1]), 99)
        self.assertEqual(BatchEnumerator.new(100, 199, self.reader).to_a()[1], self.collection[100:199])

    def test_successive_args(self):
        self.assertEqual(
            list(BatchEnumerator.new(1000, INFINITY, self.reader).each()),
            [
                self.collection[0:1000],
                self.collection[1000:2000],
                self.collection[2000:3000],
                self.collection[3000:4000],
                self.collection[4000:5000],
                self.collection[5000:6000],
                self.collection[6000:7000],
                self.collection[7000:8000],
                self.collection[8000:9000],
                self.collection[9000:10000],
            ],
        )

    def test_ensure_minimal_required_number_of_iterations(self):
        reader = MagicMock(return_value=self.collection)
        BatchEnumerator.new(100, 100, reader).to_a()
        reader.assert_called_once()

    def test_ensure_one_call_for_empty_reader(self):
        reader = MagicMock(return_value=[])
        BatchEnumerator.new(100, INFINITY, reader).to_a()
        reader.assert_called_once()
