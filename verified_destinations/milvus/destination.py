import os
from typing import Any, Optional, Tuple, Iterable, Mapping
from dat_core.connectors.destinations.base import DestinationBase
from dat_core.pydantic_models import ConnectorSpecification
from dat_core.pydantic_models import DatCatalog, DatMessage
from dat_core.connectors.destinations.data_processor import DataProcessor
from verified_destinations.milvus.specs import MilvusSpecification
from verified_destinations.milvus.loader import MilvusLoader


BATCH_SIZE = 1000

class Milvus(DestinationBase):

    _spec_class = MilvusSpecification
    
    def _init_loader(self, config: Mapping[str, Any]) -> None:
        self.loader = MilvusLoader(config)

    def check_connection(self, config: ConnectorSpecification) -> Tuple[bool, Optional[Any]]:
        self._init_loader(config)
        try:
            check, desc = self.loader.check()
            return (check, desc)
        except Exception as e:
            return (False, e)

    def write(
        self,
        config: Mapping[str, Any], configured_catalog: DatCatalog,
        input_messages: Iterable[DatMessage]
    ) -> Iterable[DatMessage]:
        self._init_loader(config)
        processor = DataProcessor(config, self.loader, BATCH_SIZE)
        yield from processor.processor(configured_catalog, input_messages)
