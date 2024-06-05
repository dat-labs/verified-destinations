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
        self._create_or_use_collection()
        chunks = create_chunks(document_chunks, batch_size=MILVUS_BATCH_SIZE)
        data = []
        for chunk in chunks:
            for document_chunk in chunk:
                metadata = document_chunk.data.metadata.model_dump()
                # metadata = self._normalize_metadata(metadata)
                vectors = document_chunk.data.vectors
                data.append({"vector": vectors, **metadata})
        # if not self.client.has_partition(collection_name=self.config.connection_specification.collection_name,
        #                                  partition_name=namespace):
        self.client.create_partition(collection_name=self.config.connection_specification.collection_name,
                                        partition_name=namespace)
        self.client.insert(collection_name=self.config.connection_specification.collection_name,
                           data=data,
                           partition_name=namespace
                           )

    def delete(self, filter, namespace=None):
        client = self._create_client()
        metadata_filter = self.metadata_filter(filter)
        # ids = self._get_entity_ids(client, metadata_filter, namespace)
        # if not ids:
        #     return
        print(f"Deleting entities with filter: {metadata_filter}")
        res = client.delete(
            collection_name=self.config.connection_specification.collection_name,
            filter=metadata_filter,
            partition_names=[namespace]
        )
        print(f"Deleted {res['delete_count']} entities")


    def check(self) -> Tuple[bool, Optional[str]]:
        try:
            self._create_or_use_collection()
            collection_info = self.client.describe_collection(
                self.config.connection_specification.collection_name)
            print(f"Collection info: {collection_info}")
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
            if collection_info.get("dimension") != self.config.connection_specification.embedding_dimension:
                return False, "Dimension of the embeddings does not match"
            return True, None
        except Exception as e:
            print(e)
            return False, str(e)

    def metadata_filter(self, metadata: StreamMetadata) -> Any:
        filter_str = ''
        for field in self.METADATA_FILTER_FIELDS:
            if field in metadata.model_dump().keys():
                filter_str += f'{field} == "{metadata.model_dump()[field]}" and '
        # Remove the last ' and ' from the string
        return filter_str[:-5]

    def _get_object_ids(self, client, filter: str, namespace: str) -> List[str]:
        response = client.query(
            collection_name=self.config.connection_specification.collection_name,
            filter=filter,
            partition_names=[namespace]
        )
        return [item["id"] for item in response]

    def initiate_sync(self, configured_catalog: DatCatalog) -> None:
        self._create_or_use_collection()
        for stream in configured_catalog.document_streams:
            if stream.write_sync_mode == WriteSyncMode.REPLACE:
                self.client.drop_partition(
                    collection_name=self.config.connection_specification.collection_name,
                    partition_name=stream.namespace
                )

    def _create_client(self):
        return MilvusClient(
            uri=self.config.connection_specification.uri,
            db_name=self.config.connection_specification.authentication.get(
                'db_name', ""),
            user=self.config.connection_specification.authentication.get(
                'user', ""),
            password=self.config.connection_specification.authentication.get(
                'password', ""),
            token=self.config.connection_specification.authentication.get(
                'token', ""),
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
