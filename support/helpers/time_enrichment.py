from datetime import datetime


class TimeEnrichment:
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/support/helpers/time_enrichment.rb
    """

    @staticmethod
    def apply_to(event, timestamp=None, valid_at=None):
        timestamp = timestamp or datetime.utcnow()
        event.metadata.setdefault("timestamp", timestamp)
        event.metadata.setdefault("valid_at", valid_at or timestamp)
        return event
