from abc import abstractmethod

from celery import Task

from event_store import Event


class EventHandlerBaseTask(Task):
    def deserialize(self, payload):
        return type(payload.get("event_type"), (Event,), {})(
            event_id=payload.get("event_id"),
            data=payload.get("data"),
            metadata=payload.get("metadata"),
        )

    @abstractmethod
    def handle_event(self, event):
        pass

    def run(self, payload):
        event = self.deserialize(payload)
        return self.handle_event(event)
