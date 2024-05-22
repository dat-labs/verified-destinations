import uuid
from dat_core.pydantic_models import StreamMetadata
import weaviate as weaviate_client
from typing import Any, List, Optional, Tuple
from dat_core.pydantic_models.dat_message import DatDocumentMessage
from dat_core.connectors.destinations.seeder import Seeder
from dat_core.connectors.destinations.utils import create_chunks

WEVIATE_BATCH_SIZE = 100

class WeaviateSeeder(Seeder):
    def __init__(self, config: Any):
        super().__init__(config)
        self._create_client()

    def seed(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
        chunks = create_chunks(document_chunks, batch_size=WEVIATE_BATCH_SIZE)
        for chunk in chunks:
            for document_chunk in chunk:
                metadata = document_chunk.data.metadata.model_dump()
                # metadata = self.metadata_filter(metadata)
                vector = document_chunk.data.vectors
                class_name = stream
                object_id= str(uuid.uuid4())
                self.client.batch.add_data_object(metadata, class_name, object_id, vector=chunk.embedding)
                


    def delete(self, filter, namespace=None):
        pass

    def check(self) -> Tuple[bool, Optional[str]]:
        # Check if the connection is valid by validating the client
        try:
            self._create_client()
            return True, None
        except Exception as e:
            return False, str(e)

    def metadata_filter(self, metadata: StreamMetadata) -> Any:
        return super().metadata_filter(metadata)

    def _create_client(self, ):
        self.client = weaviate_client.Client(
            url=self.config.connection_specification.get('cluster_url'),
        )
