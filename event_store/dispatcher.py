class Dispatcher:
    def __call__(self, subscriber, event, _) -> None:
        subscriber = subscriber() if isinstance(subscriber, type) else subscriber
        subscriber(event)
