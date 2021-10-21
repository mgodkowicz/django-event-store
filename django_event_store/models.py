from django.db import models


class EventsInStreams(models.Model):
    stream = models.TextField()
    position = models.IntegerField(null=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    event = models.ForeignKey("Event", on_delete=models.CASCADE)

    class Meta:
        unique_together = [
            ["stream", "event"],
            ["stream", "position"],
        ]


class Event(models.Model):
    event_id = models.UUIDField(primary_key=True, unique=True, db_index=True)
    event_type = models.TextField(db_index=True)
    data = models.JSONField()
    metadata = models.JSONField()
    created_at = models.DateTimeField(null=False, db_index=True)
    valid_at = models.DateTimeField(null=True, db_index=True)
    # event = models.ForeignKey("Event", on_delete=models.CASCADE, related_name="related_events")
