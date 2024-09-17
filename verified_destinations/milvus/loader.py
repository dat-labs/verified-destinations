from pymilvus import MilvusClient
from typing import Any, List, Optional, Tuple, Dict
from dat_core.pydantic_models import DatCatalog, StreamMetadata
from dat_core.pydantic_models.dat_message import DatDocumentMessage
from dat_core.connectors.destinations.loader import Loader
from dat_core.connectors.destinations.utils import create_chunks
from dat_core.pydantic_models import WriteSyncMode
from dat_core.loggers import logger

MILVUS_BATCH_SIZE = 100


class MilvusLoader(Loader):
    def __init__(self, config: Any):
        super().__init__(config)

    def load(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
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
        res = client.delete(
            collection_name=self.config.connection_specification.collection_name,
            filter=filter,
            partition_names=[namespace]
        )
        logger.info(f"Deleted {res['delete_count']} entities")


    def check(self) -> Tuple[bool, Optional[str]]:
        try:
            self._create_or_use_collection()
            collection_info = self.client.describe_collection(
                self.config.connection_specification.collection_name)
            logger.debug(f"Collection info: {collection_info}")
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

            if vector_field.get("params", {}).get("dim", None) != self.config.connection_specification.embedding_dimensions:
                return False, "Dimension of the embeddings does not match"

            return True, None
        except Exception as e:
            logger.error(f"Error while checking connection: {e}")
            return False, str(e)

    def prepare_metadata_filter(self, filter: Dict[str, Any]) -> str:
        filter_conditions = []

        for key, value in filter.items():
            if key == self.METADATA_DAT_RUN_ID_FIELD:
                filter_conditions.append(f"({key} != {repr(value)})")
            elif isinstance(value, str):
                filter_conditions.append(f"({key} == {repr(value)})")
            elif isinstance(value, list):
                filter_conditions.append(f"({key} in {value})")

        filter_statement = " and ".join(filter_conditions)

        return filter_statement

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
        if self.config.connection_specification.authentication.authentication == "basic_authentication":
            return MilvusClient(
                uri=self.config.connection_specification.uri,
                username=self.config.connection_specification.authentication.username,
                password=self.config.connection_specification.authentication.password
            )
        elif self.config.connection_specification.authentication.authentication == "token_authentication":
            return MilvusClient(
                uri=self.config.connection_specification.uri,
                token=self.config.connection_specification.authentication.token
            )
        else:
            return MilvusClient(
                uri=self.config.connection_specification.uri
            )


    def _create_or_use_collection(self, ):
        collection_name = self.config.connection_specification.collection_name
        self.client = self._create_client()
        logger.debug(f"client: {self.client}")

        if not self.client.has_collection(collection_name=collection_name):
            self.client.create_collection(
                collection_name,
                dimension=self.config.connection_specification.embedding_dimensions,
                auto_id=True
            )
