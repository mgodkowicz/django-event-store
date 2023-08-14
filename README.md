# Django Event Store

Based on RailsEventStore v2.11.1

Requirements

How to install?

How to setup?

How to use?

```python
from event_store.event import Event
from event_store.subscriptions import Subscriptions

class MyEvent(Event):
    pass

class MyEventHandler:
    pass

subscriptions = Subscriptions()
subscriptions.add_subscription(MyEventHandler, [MyEvent])

subscriptions.all_for("MyEvent")
```
