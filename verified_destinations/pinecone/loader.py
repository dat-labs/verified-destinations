import uuid
from pinecone import Pinecone
from typing import Any, List, Optional
from dat_core.connectors.destinations.loader import Loader
from dat_core.connectors.destinations.utils import create_chunks
from dat_core.pydantic_models import (
    DatDocumentMessage, StreamMetadata,
    WriteSyncMode, DatCatalog
)

PINECONE_BATCH_SIZE = 40

PARALLELISM_LIMIT = 4

MAX_METADATA_SIZE = 40960 - 10000

MAX_IDS_PER_DELETE = 1000

class PineconeLoader(Loader):
    def __init__(self, config: Any, embedding_dimensions: int):
        super().__init__(config)
        self.embedding_dimensions = int(embedding_dimensions)
        self.pine = Pinecone(api_key=config.connection_specification.pinecone_api_key)

    def load(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
        pinecone_index = self.pine.Index(self.config.connection_specification.pinecone_index)
        pinecone_docs = []
        for document_chunk in document_chunks:
            metadata = document_chunk.data.metadata.model_dump()
            metadata = self._normalize_metadata(metadata)
            chunk = document_chunk.data.vectors
            pinecone_docs.append((str(uuid.uuid4()), chunk, metadata))
        serial_batches = create_chunks(pinecone_docs, batch_size=PINECONE_BATCH_SIZE * PARALLELISM_LIMIT)
        for batch in serial_batches:
            async_results = [
                pinecone_index.upsert(vectors=ids_vectors_chunk, async_req=True, show_progress=False, namespace=namespace)
                for ids_vectors_chunk in create_chunks(batch, batch_size=PINECONE_BATCH_SIZE)
            ]
            [async_result.get() for async_result in async_results]

    def delete(self, filter, namespace=None):
        pinecone_index = self.pine.Index(self.config.connection_specification.pinecone_index)
        meta_filter = self.metadata_filter(filter)
        pod_type = self.pine.describe_index(self.config.connection_specification.pinecone_index).spec.pod.pod_type
        if pod_type == "starter":
            top_k = 10000
            self.delete_by_metadata(meta_filter, top_k, namespace)
        else:
            pinecone_index.delete(filter=meta_filter, namespace=namespace)

    def delete_by_metadata(self, filter, top_k, namespace=None):
        pinecone_index = self.pine.Index(self.config.connection_specification.pinecone_index)
        zero_vector = [0.0] * self.embedding_dimensions
        query_result = self.pinecone_index.query(
            vector=zero_vector, filter=filter, top_k=top_k, namespace=namespace)
        while len(query_result.matches) > 0:
            vector_ids = [doc.id for doc in query_result.matches]
            if len(vector_ids) > 0:
                # split into chunks of 1000 ids to avoid id limit
                batches = create_chunks(vector_ids, batch_size=MAX_IDS_PER_DELETE)
                for batch in batches:
                    pinecone_index.delete(ids=list(batch), namespace=namespace)
            query_result = pinecone_index.query(
                vector=zero_vector, filter=filter, top_k=top_k, namespace=namespace)

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
                self.delete(filter={self.METADATA_DAT_STREAM_FIELD: stream.name}, namespace=stream.namespace)

    def metadata_filter(self, metadata: StreamMetadata) -> Any:
        meta_filter_fields = {}
        for field in self.METADATA_FILTER_FIELDS:
            if field in metadata.model_dump().keys():
                meta_filter_fields[field] = {"$eq": getattr(metadata, field)}

        return meta_filter_fields

    def _normalize_metadata(self, metadata: dict) -> dict:
        for key, value in metadata.items():
            if value is None:
                metadata[key] = ""  # Set value to empty string if None
        return metadata
