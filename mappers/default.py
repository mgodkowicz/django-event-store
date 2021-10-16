from mappers.pipeline import Pipeline
from mappers.pipeline_mapper import PipelineMapper
from mappers.transformations.event_class_remapper import EventClassRemapper
from mappers.transformations.symbolize_metadata_keys import SymbolizeMetadataKeys


class Default(PipelineMapper):
    def __init__(self):
        super().__init__(Pipeline(
            EventClassRemapper({}),
            SymbolizeMetadataKeys(),
        ))
