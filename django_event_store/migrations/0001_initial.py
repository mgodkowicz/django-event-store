# Generated by Django 3.2.8 on 2021-11-19 13:35

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Event",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("event_id", models.UUIDField(db_index=True, unique=True)),
                ("event_type", models.TextField(db_index=True)),
                ("data", models.JSONField()),
                ("metadata", models.JSONField()),
                ("created_at", models.DateTimeField(db_index=True)),
                ("valid_at", models.DateTimeField(db_index=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name="EventsInStreams",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("stream", models.TextField()),
                ("position", models.IntegerField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "event",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="stream_position",
                        to="django_event_store.event",
                        to_field="event_id",
                    ),
                ),
            ],
            options={
                "unique_together": {("stream", "position"), ("stream", "event")},
            },
        ),
    ]
