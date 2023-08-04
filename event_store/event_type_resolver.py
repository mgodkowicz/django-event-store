class EventTypeResolver:
    """
    Resolves the event type name from the event class.

    Port of: https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/event_type_resolver.rb
    """

    @classmethod
    def new(cls):
        return cls()

    def call(self, value) -> str:
        if isinstance(value, str):
            return value
        return str(value.__name__)
