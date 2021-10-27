from django.db import models


class EventsInStreams(models.Model):
    stream = models.TextField()
    position = models.IntegerField(null=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    event = models.ForeignKey(
        "Event", on_delete=models.CASCADE, related_name="stream_position"
    )

    class Meta:
        unique_together = [
            ["stream", "event"],
            ["stream", "position"],
        ]

    @property
    def metadata(self):
        return self.event.metadata

    @property
    def data(self):
        return self.event.data

    @property
    def event_type(self):
        return self.event.event_type

    @property
    def valid_at(self):
        return self.event.valid_at


class Event(models.Model):
    event_id = models.UUIDField(primary_key=True, unique=True, db_index=True)
    event_type = models.TextField(db_index=True)
    data = models.JSONField()
    metadata = models.JSONField()
    created_at = models.DateTimeField(null=False, db_index=True)
    valid_at = models.DateTimeField(null=True, db_index=True)
