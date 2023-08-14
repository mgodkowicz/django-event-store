from collections.abc import MutableMapping
from datetime import date, datetime, time


MetadataValue = (
    str | int | float | date | time | datetime | bool | type(None) | dict | list
)


def assert_key_is_str(value):
    if not isinstance(value, str):
        raise TypeError(f"Key must be a string, {type(value)} given")


def assert_value_is_allowed(value):
    if not isinstance(value, MetadataValue):
        raise TypeError(f"Value type not allowed: {type(value)}")


class Metadata(MutableMapping[str, MetadataValue]):
    """
    Metadata class providing a restricted interface to a dictionary.

    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/metadata.rb
    """

    def __init__(self, data=None):
        self._dict = {}
        for k, v in (data or {}).items():
            self[k] = v

    def __getitem__(self, key):
        assert_key_is_str(key)
        return self._dict[key]

    def __setitem__(self, key, val):
        assert_key_is_str(key)
        assert_value_is_allowed(val)
        self._dict[key] = val

    def __delitem__(self, key):
        assert_key_is_str(key)
        del self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    @classmethod
    def new(cls, data=None):
        return cls(data)

    def each(self, function):
        for k, v in self._dict.items():
            yield function(k, v)

    def to_dict(self):
        return dict(self._dict.items())
