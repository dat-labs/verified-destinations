import os
from typing import Any, Tuple, Optional, Mapping, Iterable
from dat_core.connectors.destinations.base import DestinationBase
from dat_core.connectors.destinations.data_processor import DataProcessor
from dat_core.pydantic_models import DatCatalog, DatMessage, ConnectorSpecification
from verified_destinations.qdrant.seeder import QdrantSeeder


BATCH_SIZE = 1000

class Qdrant(DestinationBase):

    _spec_file = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'specs.yml')

    def _init_seeder(self, config: Mapping[str, Any]) -> None:
        self.seeder = QdrantSeeder(config, config.connection_specification.get('embedding_dimensions'))

    def check_connection(self, config: ConnectorSpecification) -> Tuple[bool, Optional[Any]]:
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
