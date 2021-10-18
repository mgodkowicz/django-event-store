from event_store.mappers.pipeline import Pipeline
from event_store.mappers.pipeline_mapper import PipelineMapper


class EmptyMapper(PipelineMapper):
    def __init__(self):
        super().__init__(Pipeline())
