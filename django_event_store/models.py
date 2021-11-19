from django.db import models


class EventsInStreams(models.Model):
    stream = models.TextField()
    position = models.IntegerField(null=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    event = models.ForeignKey(
        "Event",
        on_delete=models.CASCADE,
        related_name="stream_position",
        to_field="event_id",
    )

    class Meta:
        unique_together = [
            ["stream", "event"],
            ["stream", "position"],
        ]

    def __str__(self):
        return f"{self.stream} ({self.event_id}) (position: {self.position})"


class Event(models.Model):
    event_id = models.UUIDField(unique=True, db_index=True)
    event_type = models.TextField(db_index=True)
    data = models.JSONField()
    metadata = models.JSONField()
    created_at = models.DateTimeField(null=False, db_index=True)
    valid_at = models.DateTimeField(null=True, db_index=True)

    def __str__(self):
        return f"{self.event_type} ({self.event_id})"
