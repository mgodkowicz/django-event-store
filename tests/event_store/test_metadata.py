import unittest
from datetime import date, datetime, time

from event_store.metadata import Metadata


class TestMetadata(unittest.TestCase):
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/spec/metadata_spec.rb
    """

    def test_default_values(self):
        self.assertEqual(list(Metadata.new().each(lambda k, v: (k, v))), [])
        self.assertEqual(
            list(Metadata.new({"a": "b"}).each(lambda k, v: (k, v))), [("a", "b")]
        )

    def test_allowed_values(self):
        m = Metadata.new()
        m["key"] = "string"
        self.assertEqual(m["key"], "string")

        m["key"] = 1
        self.assertEqual(m["key"], 1)

        m["key"] = 2 ** 40
        self.assertEqual(m["key"], 2 ** 40)

        m["key"] = 5.5
        self.assertEqual(m["key"], 5.5)

        m["key"] = date(2019, 1, 1)
        self.assertEqual(m["key"], date(2019, 1, 1))

        m["key"] = time(12, 0)
        self.assertEqual(m["key"], time(12, 0))

        m["key"] = dt = datetime.utcnow()
        self.assertEqual(m["key"], dt)

        m["key"] = True
        self.assertEqual(m["key"], True)

        m["key"] = False
        self.assertEqual(m["key"], False)

        m["key"] = None
        self.assertEqual(m["key"], None)

        m["key"] = {"some": "hash", "with": {"nested": "values"}}
        self.assertEqual(m["key"], {"some": "hash", "with": {"nested": "values"}})

        m["key"] = [1, 2, 3]
        self.assertEqual(m["key"], [1, 2, 3])

        with self.assertRaises(TypeError):
            m["key"] = object()

    def test_allowed_keys(self):
        m = Metadata.new({"key": "value", "key2": "value2"})

        # Testing that this does not raise an error
        _ = m["key"]
        _ = m.get("key")
        _ = "key" in m
        _ = m.pop("key")
        del m["key2"]

        with self.assertRaises(TypeError):
            _ = m[object()]

        with self.assertRaises(TypeError):
            _ = m.get(object())

        with self.assertRaises(TypeError):
            _ = object() in m

        with self.assertRaises(TypeError):
            _ = m.pop(object())

        with self.assertRaises(TypeError):
            del m[object()]

        with self.assertRaises(TypeError):
            m.setdefault(object(), "value")

    def test_each(self):
        m = Metadata.new()
        m["a"] = 1
        m["b"] = "2"
        self.assertEqual(list(m.each(lambda k, v: (k, v))), [("a", 1), ("b", "2")])

    def test_to_dict(self):
        m = Metadata.new()
        m["a"] = 1
        m["b"] = "2"
        self.assertEqual(m.to_dict(), {"a": 1, "b": "2"})

        d = m.to_dict()
        d["x"] = "leaked?"
        self.assertNotIn("x", m)

    def test_enumerable(self):
        m = Metadata.new()
        m["a"] = 1
        self.assertEqual(list(map(lambda k: (k, m[k]), m)), [("a", 1)])
