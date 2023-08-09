from datetime import datetime
from decimal import Decimal
from itertools import permutations
from unittest import TestCase

from event_store.record import Record
from event_store.serialized_record import SerializedRecord
from event_store.serializers import YAML


class TestSerializedRecord(TestCase):
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/spec/serialized_record_spec.rb
    """

    def setUp(self):
        self.iso8601 = "2019-10-03T22:25:22+00:00"
        self.datetime = datetime.fromisoformat(self.iso8601)

    def test_constructor_accept_all_arguments_and_returns_frozen_instance(self):
        event_id = "event_id"
        data = "data"
        metadata = "metadata"
        event_type = "event_type"
        record = SerializedRecord.new(
            event_id=event_id,
            data=data,
            metadata=metadata,
            event_type=event_type,
            timestamp=self.iso8601,
            valid_at=self.iso8601,
        )
        self.assertEqual(record.event_id, event_id)
        self.assertEqual(record.metadata, metadata)
        self.assertEqual(record.data, data)
        self.assertEqual(record.event_type, event_type)
        self.assertTrue(record.__dataclass_params__.frozen)

    def test_constructor_raises_stringsrequired_when_argument_is_not_a_string(self):
        a = "---\norder_id: '12345'\n"
        b = "---\nrequest_id: '12345'\n"
        samples = [
            [1, a, b, 1],
            [1, a, b, "string"],
            ["string", a, b, 1],
        ]
        for sample in samples:
            event_id, data, metadata, event_type = sample
            with self.assertRaises(SerializedRecord.StringsRequired):
                SerializedRecord.new(
                    event_id=event_id,
                    data=data,
                    metadata=metadata,
                    event_type=event_type,
                    timestamp=self.iso8601,
                    valid_at=self.iso8601,
                )

    def test_inequality(self):
        a = "---\norder_id: '12345'\n"
        b = "---\nrequest_id: '12345'\n"
        for one, two in permutations(
            [
                ["a", a, a, "a", "a", "a"],
                ["b", a, a, "a", "a", "a"],
                ["a", b, a, "a", "a", "a"],
                ["a", a, b, "a", "a", "a"],
                ["a", a, a, "b", "a", "a"],
                ["a", a, a, "a", "b", "a"],
                ["a", a, a, "a", "a", "b"],
            ],
            2,
        ):
            a = SerializedRecord.new(
                event_id=one[0],
                data=one[1],
                metadata=one[2],
                event_type=one[3],
                timestamp=one[4],
                valid_at=one[5],
            )
            b = SerializedRecord.new(
                event_id=two[0],
                data=two[1],
                metadata=two[2],
                event_type=two[3],
                timestamp=two[4],
                valid_at=two[5],
            )
            self.assertNotEqual(a, b)
            self.assertNotEqual(hash(a), hash(b))
            h = {a: "val"}
            self.assertEqual(h.get(b), None)

    def test_equality(self):
        a = SerializedRecord.new(
            event_id="a",
            data="---\norder_id: '12345'\n",
            metadata="---\nrequest_id: '12345'\n",
            event_type="d",
            timestamp=self.iso8601,
            valid_at=self.iso8601,
        )
        b = SerializedRecord.new(
            event_id="a",
            data="---\norder_id: '12345'\n",
            metadata="---\nrequest_id: '12345'\n",
            event_type="d",
            timestamp=self.iso8601,
            valid_at=self.iso8601,
        )
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        h = {a: "val"}
        self.assertEqual(h.get(b), "val")

    def test_to_dict(self):
        a = SerializedRecord.new(
            event_id="a",
            data="---\norder_id: '12345'\n",
            metadata="---\nrequest_id: '12345'\n",
            event_type="d",
            timestamp=self.iso8601,
            valid_at=self.iso8601,
        )
        self.assertEqual(
            a.to_dict(),
            {
                "event_id": "a",
                "data": "---\norder_id: '12345'\n",
                "metadata": "---\nrequest_id: '12345'\n",
                "event_type": "d",
                "timestamp": self.iso8601,
                "valid_at": self.iso8601,
            },
        )

    def test_constructor_raised_when_required_args_are_missing(self):
        with self.assertRaises(TypeError):
            SerializedRecord()

    def test_deserialize(self):
        actual = SerializedRecord.new(
            event_id="a",
            data="---\norder_id: '12345'\n",
            metadata="---\nrequest_id: '12345'\n",
            event_type="d",
            timestamp=self.iso8601,
            valid_at=self.iso8601,
        )
        expected = Record.new(
            event_id="a",
            data={"order_id": "12345"},
            metadata={"request_id": "12345"},
            event_type="d",
            timestamp=self.datetime,
            valid_at=self.datetime,
        )
        self.assertEqual(actual.deserialize(YAML), expected)

    def test_deserialization_works_for_non_primitive_values(self):
        actual = SerializedRecord.new(
            event_id="a",
            data="---\ntotal_price: !!python/object/apply:decimal.Decimal\n- '12.99'\n",
            metadata="--- {}\n",
            event_type="b",
            timestamp=self.iso8601,
            valid_at=self.iso8601,
        )
        expected = Record.new(
            data={"total_price": Decimal("12.99")},
            event_id="a",
            event_type="b",
            metadata={},
            timestamp=self.datetime,
            valid_at=self.datetime,
        )
        self.assertEqual(actual.deserialize(YAML), expected)
