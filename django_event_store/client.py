from datetime import datetime
from typing import Callable, Optional

from django_event_store.event_repository import DjangoEventRepository
from event_store import Client as EsClient
from event_store import Dispatcher, EventsRepository, Subscriptions
from event_store.dispatcher import DispatcherBase
from event_store.mappers.default import Default
from event_store.mappers.pipeline_mapper import PipelineMapper


class Client(EsClient):
    def __init__(
        self,
        repository: Optional[EventsRepository] = None,
        subscriptions: Optional[Subscriptions] = None,
        dispatcher: DispatcherBase = Dispatcher(),
        mapper: Optional[PipelineMapper] = None,
        clock: Callable = datetime.now,
    ):
        super().__init__(
            repository=repository or DjangoEventRepository(),
            subscriptions=subscriptions or Subscriptions(),
            dispatcher=dispatcher,
            mapper=mapper or Default(),
            clock=clock,
        )
