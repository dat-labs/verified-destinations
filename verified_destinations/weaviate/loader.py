import re
import uuid
from dat_core.pydantic_models import DatCatalog, StreamMetadata
import weaviate
from weaviate.exceptions import UnexpectedStatusCodeException
from typing import Any, List, Optional, Tuple, Dict
from dat_core.pydantic_models.dat_message import DatDocumentMessage
from dat_core.connectors.destinations.loader import Loader
from dat_core.connectors.destinations.utils import create_chunks
from dat_core.pydantic_models import WriteSyncMode
from dat_core.loggers import logger

WEVIATE_BATCH_SIZE = 100


class WeaviateLoader(Loader):
    def __init__(self, config: Any):
        super().__init__(config)

    def load(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
        chunks = create_chunks(document_chunks, batch_size=WEVIATE_BATCH_SIZE)
        with self.client.batch as batch:
            for data in chunks:
                for document_chunk in data:
                    metadata = document_chunk.data.metadata.model_dump()
                    metadata = self._normalize_metadata(metadata)
                    vectors = document_chunk.data.vectors
                    class_name = self._namespace_to_class_name(namespace)
                    object_id = str(uuid.uuid4())
                    batch.add_data_object(
                        data_object=metadata, class_name=class_name,
                        uuid=object_id, vector=vectors
                    )

    def delete(self, filter, namespace=None):
        self._create_client()
        object_ids = self._get_object_ids(filter, namespace)
        if not object_ids:
            return
        where_filter = {
            "path": ["id"], "operator": "ContainsAny", "valueStringArray": object_ids}
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
            logger.error(f"Error checking connection: {e}")
            return False, str(e)

    def prepare_metadata_filter(self, filter: Dict[str, Any]) -> Dict[str, Any]:
        filter_expr = {
            "operator": "And",
            "operands": []
        }

        for key, value in filter.items():
            if key == self.METADATA_DAT_RUN_ID_FIELD:
                filter_expr["operands"].append({
                    "operator": "NotEqual",
                    "path": f"{key}",
                    "valueString": str(value)
                })
            elif isinstance(value, str):
                filter_expr["operands"].append({
                    "operator": "Equal",
                    "path": f"{key}",
                    "valueString": value
                })
            elif isinstance(value, list):
                filter_expr["operands"].append({
                    "operator": "ContainsAny",
                    "path": f"{key}",
                    "valueStringList": value
                })

        return filter_expr

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
            object_ids = [item["_additional"]["id"]
                          for item in result["data"]["Get"][class_name]]
            return object_ids
        except (UnexpectedStatusCodeException, KeyError) as e:
            logger.error(f"Error while fetching object ids: {e}")
            return []

    def initiate_sync(self, configured_catalog: DatCatalog) -> None:
        self._create_client()
        for stream in configured_catalog.document_streams:
            if stream.write_sync_mode == WriteSyncMode.REPLACE:
                _filter = self.prepare_metadata_filter(
                    {self.METADATA_DAT_STREAM_FIELD: stream.name}
                )
                logger.info(f"Upsert mode set to 'REPLACE' for stream {stream.name}. "
                            f"Deleting with filter: {_filter}")
                self.delete(filter=_filter, namespace=stream.namespace)

    def _create_client(self, ):
        if self.config.connection_specification.authentication.authentication == "basic_authentication":
            auth = weaviate.AuthClientPassword(
                username=self.config.connection_specification.authentication.username,
                password=self.config.connection_specification.authentication.password
            )
            self.client = weaviate.Client(
                url=self.config.connection_specification.cluster_url,
                auth_client_secret=auth
            )
        elif self.config.connection_specification.authentication.authentication == "api_key_authentication":
            auth = weaviate.AuthApiKey(
                api_key=self.config.connection_specification.authentication.api_key
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
