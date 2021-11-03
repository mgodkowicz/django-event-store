# Generated by Django 3.2.8 on 2021-10-27 15:17

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Event",
            fields=[
                (
                    "event_id",
                    models.UUIDField(
                        db_index=True, primary_key=True, serialize=False, unique=True
                    ),
                ),
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
                    ),
                ),
            ],
            options={
                "unique_together": {("stream", "position"), ("stream", "event")},
            },
        ),
    ]
