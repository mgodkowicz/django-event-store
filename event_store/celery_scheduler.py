class CeleryScheduler:
    def call(self, task, record):
        task.apply_async(args=(record.to_dict(),))

    def verify(self, subscriber) -> bool:
        if not getattr(subscriber, "apply_async", None):
            return False
        return True
