import re
import uuid
from dat_core.pydantic_models import DatCatalog, StreamMetadata
import weaviate
from weaviate.exceptions import UnexpectedStatusCodeException
from typing import Any, List, Optional, Tuple
from dat_core.pydantic_models.dat_message import DatDocumentMessage
from dat_core.connectors.destinations.seeder import Seeder
from dat_core.connectors.destinations.utils import create_chunks
from dat_core.pydantic_models import WriteSyncMode

WEVIATE_BATCH_SIZE = 100

class WeaviateSeeder(Seeder):
    def __init__(self, config: Any):
        super().__init__(config)

    def seed(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
        chunks = create_chunks(document_chunks, batch_size=WEVIATE_BATCH_SIZE)
        # self.client.batch.configure(batch_size=WEVIATE_BATCH_SIZE)
        with self.client.batch as batch:
            for data in chunks:
                for document_chunk in data:
                    metadata = document_chunk.data.metadata.model_dump()
                    metadata = self._normalize_metadata(metadata)
                    vectors = document_chunk.data.vectors
                    class_name = self._namespace_to_class_name(namespace)
                    object_id= str(uuid.uuid4())
                    batch.add_data_object(
                        data_object=metadata, class_name=class_name,
                        uuid=object_id, vector=vectors
                    )

    def delete(self, filter, namespace=None):
        self._create_client()
        metadata_filters = self.metadata_filter(filter)
        combined_filter = {
            "operator": "And",
            "operands": metadata_filters
        }
        object_ids = self._get_object_ids(combined_filter, namespace)
        if not object_ids:
            return
        where_filter = {"path": ["id"], "operator": "ContainsAny", "valueStringArray": object_ids}
        self.client.batch.delete_objects(
            class_name=self._namespace_to_class_name(namespace),
            where=where_filter
        )

    def check(self) -> Tuple[bool, Optional[str]]:
        # Check if the connection is valid by validating the client
        try:
            self._create_client()
            return True, None
        except Exception as e:
            return False, str(e)

    def metadata_filter(self, metadata: StreamMetadata) -> Any:
        filters = []
        for field in self.METADATA_FILTER_FIELDS:
            if field in metadata.model_dump().keys():
                filters.append({
                    "path": field,
                    "operator": "Equal",
                    "valueString": metadata.model_dump()[field]
                })
        return filters

    def _get_object_ids(self, combined_filter: dict, namespace: str) -> List[str]:
        class_name = self._namespace_to_class_name(namespace)
        query = (
            self.client.query.get(
                class_name,
            )
            .with_additional(["id"])
            .with_where(combined_filter)
        )
        try:
            result = query.do()
            object_ids = [item["_additional"]["id"] for item in result["data"]["Get"][class_name]]
            return object_ids
        except UnexpectedStatusCodeException as e:
            return []

    def initiate_sync(self, configured_catalog: DatCatalog) -> None:
        self._create_client()
        for stream in configured_catalog.document_streams:
            if stream.write_sync_mode == WriteSyncMode.REPLACE:
                self.client.schema.delete_class(
                    self._namespace_to_class_name(stream.namespace)
                )

    def _create_client(self, ):
        if self.config.connection_specification.authentication.get("authentication") == "basic_authentication":
            auth = weaviate.AuthClientPassword(
                username=self.config.connection_specification.authentication.get("username"),
                password=self.config.connection_specification.authentication.get("password")
            )
            self.client = weaviate.Client(
                url=self.config.connection_specification.cluster_url,
                auth_client_secret=auth
            )
        elif self.config.connection_specification.authentication.get("authentication") == "api_key_authentication":
            auth = weaviate.AuthApiKey(
                api_key=self.config.connection_specification.authentication.get("api_key")
            )
            self.client = weaviate.Client(
                url=self.config.connection_specification.cluster_url,
                auth_client_secret=auth
            )
        else:
            self.client = weaviate.Client(
                url=self.config.connection_specification.cluster_url
            )

    def _normalize_metadata(self, metadata: dict) -> dict:
        for key, value in metadata.items():
            if value is None:
                metadata[key] = ""  # Set value to empty string if None
        return metadata

    def _namespace_to_class_name(self, namespace: str) -> str:
        """
        Converts a namespace string to a class name by removing spaces,
        replacing dashes with underscores, and capitalizing the first 
        letter of each word.

        Example:
            'my-class name' -> 'My_ClassName'
        """
        # Remove leading/trailing whitespace and replace multiple spaces with a single space
        cleaned_namespace = re.sub(r'\s+', ' ', namespace.strip())

        # Split the cleaned namespace by spaces and dashes, capitalize the first letter of each word
        words = re.split(r'[-\s]', cleaned_namespace)
        capitalized_words = [word.capitalize() for word in words]

        # Join the words with underscores
        class_name = '_'.join(capitalized_words)

        return class_name

"""
TODO: Complete create_client to support all type of authentications
TODO: Test _namespace_to_class_name method with different inputs
TODO: Figure out Multi-tenancy support

"""