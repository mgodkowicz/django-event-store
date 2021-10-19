from event_store.mappers.pipeline import Pipeline
from event_store.mappers.pipeline_mapper import PipelineMapper
from event_store.mappers.transformations.event_class_remapper import EventClassRemapper
from event_store.mappers.transformations.symbolize_metadata_keys import (
    SymbolizeMetadataKeys,
)


class Default(PipelineMapper):
    def __init__(self):
        super().__init__(
            Pipeline(
                EventClassRemapper({}),
                SymbolizeMetadataKeys(),
            )
        )
