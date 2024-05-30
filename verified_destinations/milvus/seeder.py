from pymilvus import MilvusClient
from typing import Any, List, Optional, Tuple
from dat_core.pydantic_models import DatCatalog, StreamMetadata
from dat_core.pydantic_models.dat_message import DatDocumentMessage
from dat_core.connectors.destinations.seeder import Seeder
from dat_core.connectors.destinations.utils import create_chunks
from dat_core.pydantic_models import WriteSyncMode

MILVUS_BATCH_SIZE = 100

class MilvusSeeder(Seeder):
    def __init__(self, config: Any):
        super().__init__(config)

    def seed(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
        pass

    def delete(self, filter, namespace=None):
        pass

    def check(self) -> Tuple[bool, Optional[str]]:
        import pdb; pdb.set_trace()
        try:
            self._create_or_use_collection()
            collection_info = self.client.describe_collection(self.config.connection_specification.collection_name)
            # Check if enable_dynamic_field is True
            if not collection_info.get("enable_dynamic_field", False):
                return False, "Dynamic fields are not enabled."
            
            if not collection_info.get("auto_id", False):
                return False, "Auto ID is not enabled."

            # Check if the required fields exist and have the correct properties
            id_field = None
            vector_field = None
            for field in collection_info.get("fields", []):
                if field["name"] == "id":
                    id_field = field
                elif field["name"] == "vector":
                    vector_field = field

            if id_field is None or vector_field is None:
                return (False,
                        f"Required fields are missing. id_field: {id_field}, vector_field: {vector_field}")

            if not id_field.get("is_primary", False):
                return False, "ID field is not primary"
            return True, None
        except Exception as e:
            print(e)
            return False, str(e)

    def metadata_filter(self, metadata: StreamMetadata) -> Any:
        pass

    def initiate_sync(self, configured_catalog: DatCatalog) -> None:
        pass

    def _create_client(self):
        return MilvusClient(
            uri=self.config.connection_specification.uri,
            db_name=self.config.connection_specification.authentication.get('db_name', ""),
            user=self.config.connection_specification.authentication.get('user', ""),
            password=self.config.connection_specification.authentication.get('password', ""),
            token=self.config.connection_specification.authentication.get('token', ""),
        )

    def _create_or_use_collection(self, ):
        collection_name = self.config.connection_specification.collection_name
        self.client = self._create_client()

        if not self.client.has_collection(collection_name=collection_name):
            self.client.create_collection(
                collection_name,
                dimension=self.config.connection_specification.embedding_dimension,
                auto_id=True
            )
