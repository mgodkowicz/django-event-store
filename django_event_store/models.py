from django.db import models


class Event(models.Model):
    id = models.UUIDField(primary_key=True, unique=True, db_index=True)
