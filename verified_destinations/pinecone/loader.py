import time
import uuid
from pinecone import Pinecone
from typing import Any, List, Optional, Dict
from dat_core.loggers import logger
from dat_core.connectors.destinations.loader import Loader
from dat_core.connectors.destinations.utils import create_chunks
from dat_core.pydantic_models import (
    DatDocumentMessage, StreamMetadata,
    WriteSyncMode, DatCatalog
)

PINECONE_BATCH_SIZE = 100

PARALLELISM_LIMIT = 4

METADATA_SIZE_LIMIT = 40  # 40KB

MAX_IDS_PER_DELETE = 1000


class PineconeLoader(Loader):
    def __init__(self, config: Any, embedding_dimensions: int):
        super().__init__(config)
        self.embedding_dimensions = int(embedding_dimensions)
        self.pine = Pinecone(
            api_key=config.connection_specification.pinecone_api_key)

    def load(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
        pinecone_index = self.pine.Index(
            self.config.connection_specification.pinecone_index)
        pinecone_docs = []
        for document_chunk in document_chunks:
            metadata = document_chunk.data.metadata.model_dump()
            metadata = self._normalize_metadata(metadata)
            chunk = document_chunk.data.vectors
            pinecone_docs.append((str(uuid.uuid4()), chunk, metadata))
        serial_batches = create_chunks(
            pinecone_docs, batch_size=PINECONE_BATCH_SIZE * PARALLELISM_LIMIT)
        for batch in serial_batches:
            async_results = [
                pinecone_index.upsert(
                    vectors=ids_vectors_chunk, async_req=True, show_progress=False, namespace=namespace)
                for ids_vectors_chunk in create_chunks(batch, batch_size=PINECONE_BATCH_SIZE)
            ]
            [async_result.get() for async_result in async_results]

    def determine_capacity_mode(self, index_name: str) -> str:
        describe_index = self.pine.describe_index(index_name)
        spec_keys = describe_index.get("spec", {})
        if "pod" in spec_keys:
            return "pod"
        elif "serverless" in spec_keys:
            return "serverless"
        else:
            raise ValueError("Unknown capacity mode")

    def delete(self, filter: Dict[str, Any], namespace: str):
        pinecone_index = self.pine.Index(
            self.config.connection_specification.pinecone_index)
        capacity_mode = self.determine_capacity_mode(
            self.config.connection_specification.pinecone_index)
        if capacity_mode in ["pod", "serverless"]:
            top_k = 10000
            self.delete_by_metadata(filter, top_k, namespace)
        else:
            pinecone_index.delete(filter=filter, namespace=namespace)

    def delete_by_metadata(self, filter, top_k, namespace=None):
        pinecone_index = self.pine.Index(
            self.config.connection_specification.pinecone_index)
        zero_vector = [0.0] * self.embedding_dimensions
        query_result = pinecone_index.query(
            vector=zero_vector, filter=filter, top_k=top_k, namespace=namespace)
        while len(query_result.matches) > 0:
            vector_ids = [doc.id for doc in query_result.matches]
            if len(vector_ids) > 0:
                batches = create_chunks(vector_ids, batch_size=MAX_IDS_PER_DELETE)
                for batch in batches:
                    pinecone_index.delete(ids=list(batch), namespace=namespace)
                    print(f"Sleeping for 20 seconds")
                    time.sleep(20)
            query_result = pinecone_index.query(vector=zero_vector, filter=filter, top_k=top_k, namespace=namespace)

    def check(self) -> Optional[str]:
        try:
            indexes = self.pine.list_indexes()
            index = self.config.connection_specification.pinecone_index
            if index not in [i.name for i in indexes]:
                return False, f"Index {index} does not exist in environment {self.config.connection_specification.pinecone_environment}."
            description = self.pine.describe_index(index)
            if description.dimension != self.embedding_dimensions:
                return (False,
                        (f"Index {index} has dimension {description.dimension} "
                         f"but configured dimension is {self.embedding_dimensions}."))
        except Exception as e:
            return False, str(e)
        return True, description

    def initiate_sync(self, configured_catalog: DatCatalog):
        for stream in configured_catalog.document_streams:
            if stream.write_sync_mode == WriteSyncMode.REPLACE:
                _filter = self.prepare_metadata_filter(
                    {self.METADATA_DAT_STREAM_FIELD: stream.name})
                logger.info(f"Upsert mode set to 'REPLACE' for stream {stream.name}."
                            f" Deleting with filter: {_filter}")
                self.delete(
                    filter=_filter, namespace=stream.namespace)

    def prepare_metadata_filter(self, filter: Dict[str, Any]) -> Dict[str, Any]:
        filter_dict = {}

        for key, value in filter.items():
            if key == self.METADATA_DAT_RUN_ID_FIELD:
                filter_dict[key] = {"$ne": value}
            elif isinstance(value, str):
                filter_dict[key] = {"$eq": value}
            elif isinstance(value, list):
                filter_dict[key] = {"$in": value}

        return filter_dict

    def _normalize_metadata(self, metadata: dict) -> dict:
        # Remove any key-value pairs with None values
        normalized_metadata = {
            key: value for key, value in metadata.items() if value is not None
        }

        # Filter out unsupported metadata types and normalize lists of strings
        for key, value in list(normalized_metadata.items()):
            if isinstance(value, (str, int, float, bool)):
                # Supported types: keep them as-is
                continue
            elif isinstance(value, list):
                # Ensure the list contains only strings
                if all(isinstance(item, str) for item in value):
                    normalized_metadata[key] = value
                else:
                    # Remove list if it contains non-string elements
                    del normalized_metadata[key]
            else:
                # Remove unsupported metadata types
                del normalized_metadata[key]

        metadata_size = sum(len(str(k)) + len(str(v)) for k, v in normalized_metadata.items())
        if metadata_size > METADATA_SIZE_LIMIT * 1024:
            raise ValueError(f"Metadata exceeds the 40KB size limit, current size: {metadata_size} bytes")

        return normalized_metadata
