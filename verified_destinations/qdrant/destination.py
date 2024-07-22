import os
from typing import Any, Tuple, Optional, Mapping, Iterable
from dat_core.connectors.destinations.base import DestinationBase
from dat_core.connectors.destinations.data_processor import DataProcessor
from dat_core.pydantic_models import DatCatalog, DatMessage
from verified_destinations.qdrant.loader import QdrantLoader
from verified_destinations.qdrant.specs import QdrantSpecification


BATCH_SIZE = 1000

class Qdrant(DestinationBase):
    
    _spec_class = QdrantSpecification

    def _init_loader(self, config: Mapping[str, Any]) -> None:
        self.loader = QdrantLoader(config, config.connection_specification.embedding_dimensions)

    def check_connection(self, config: QdrantSpecification) -> Tuple[bool, Optional[Any]]:
        self._init_loader(config)
        try:
            check, desc = self.loader.check()
            return (check, desc)
        except Exception as e:
            return (False, e)

    def write(self, config: Mapping[str, Any], configured_catalog: DatCatalog, input_messages: Iterable[DatMessage]) -> Iterable[DatMessage]:
        self._init_loader(config)
        processor = DataProcessor(config, self.loader, BATCH_SIZE)
        yield from processor.processor(configured_catalog, input_messages)
