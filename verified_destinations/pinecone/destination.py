import os
from typing import Any, Iterable, Mapping, Tuple, Optional
from dat_core.pydantic_models.dat_catalog import DatCatalog
from dat_core.pydantic_models.dat_message import DatMessage
from dat_core.connectors.destinations.base import DestinationBase
from dat_core.connectors.destinations.data_processor import DataProcessor
from verified_destinations.pinecone.loader import PineconeLoader
from verified_destinations.pinecone.specs import PineconeSpecification


BATCH_SIZE = 1000


class Pinecone(DestinationBase):

    _spec_class = PineconeSpecification

    def _init_loader(self, config: Mapping[str, Any]) -> None:
        self.loader = PineconeLoader(config, config.connection_specification.embedding_dimensions)

    def check_connection(self, config: PineconeSpecification) -> Tuple[bool, Optional[Any]]:
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
