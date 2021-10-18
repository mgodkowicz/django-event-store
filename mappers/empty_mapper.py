from mappers.pipeline import Pipeline
from mappers.pipeline_mapper import PipelineMapper


class EmptyMapper(PipelineMapper):
    def __init__(self):
        super().__init__(Pipeline())
