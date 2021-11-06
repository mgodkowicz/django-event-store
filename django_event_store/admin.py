from django.contrib import admin

from django_event_store.models import Event, EventsInStreams


class EventAdmin(admin.ModelAdmin):
    ordering = ("-created_at",)
    list_filter = ("event_type",)
    list_display = (
        "event_id",
        "event_type",
        "created_at",
        "valid_at",
    )


class EventsInStreamsAdmin(admin.ModelAdmin):
    ordering = ("-created_at",)
    list_filter = ("stream",)
    list_display = (
        "id",
        "stream",
        "event_id",
        "position",
        "created_at",
    )


admin.site.register(Event, EventAdmin)
admin.site.register(EventsInStreams, EventsInStreamsAdmin)
