import os
from dat_core.connectors.destinations.destination import Destination
from typing import Any, Iterable, Mapping, Tuple, Optional
from dat_core.pydantic_models.connector_specification import ConnectorSpecification, DestinationSyncMode
from dat_core.pydantic_models.dat_catalog import DatCatalog
from dat_core.pydantic_models.dat_message import DatMessage, Type, DatDocumentMessage, Data
from verified_destinations.pinecone.seeder import PineconeSeeder
from dat_core.connectors.destinations.vector_db_helpers.data_processor import DataProcessor
from dat_core.pydantic_models.dat_document_stream import DatDocumentStream, SyncMode


BATCH_SIZE = 1000


class Pinecone(Destination):

    _spec_file = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'specs.yml')

    def _init_seeder(self, config: Mapping[str, Any]) -> None:
        self.seeder = PineconeSeeder(config, config.connectionSpecification.get('embedding_dimensions'))

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
