import os
from typing import Any, Tuple, Optional, Mapping, Iterable
from dat_core.connectors.destinations.base import DestinationBase
from dat_core.connectors.destinations.data_processor import DataProcessor
from dat_core.pydantic_models import DatCatalog, DatMessage
from verified_destinations.qdrant.seeder import QdrantSeeder
from verified_destinations.qdrant.specs import QdrantSpecification


BATCH_SIZE = 1000

class Qdrant(DestinationBase):
    
    _spec_class = QdrantSpecification

    def _init_seeder(self, config: Mapping[str, Any]) -> None:
        self.seeder = QdrantSeeder(config, config.connection_specification.embedding_dimensions)

    def check_connection(self, config: QdrantSpecification) -> Tuple[bool, Optional[Any]]:
        self._init_seeder(config)
        try:
            check, desc = self.seeder.check()
            return (check, desc)
        except Exception as e:
            return (False, e)

    def write(self, config: Mapping[str, Any], configured_catalog: DatCatalog, input_messages: Iterable[DatMessage]) -> Iterable[DatMessage]:
        self._init_seeder(config)
        processor = DataProcessor(config, self.seeder, BATCH_SIZE)
        yield from processor.processor(configured_catalog, input_messages)
