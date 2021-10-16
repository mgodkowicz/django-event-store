from mappers.pipeline import Pipeline


class PipelineMapper:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def event_to_record(self, domain_event):
        return self.pipeline.dump(domain_event)

    def record_to_event(self, record):
        return self.pipeline.load(record)
