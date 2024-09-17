import uuid
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, MatchAny, MatchExcept
from typing import Any, List, Optional, Dict
from dat_core.connectors.destinations.loader import Loader
from qdrant_openapi_client.models import VectorParams, Distance
from dat_core.pydantic_models import (
    DatDocumentMessage, DatCatalog,
    WriteSyncMode, StreamMetadata
)
from dat_core.loggers import logger


DISTANCE_MAP = {
    "dot": Distance.DOT,
    "cos": Distance.COSINE,
    "euc": Distance.EUCLID,
}


class QdrantLoader(Loader):
    def __init__(self, config: Any, embedding_dimensions: int):
        super().__init__(config)
        self.embedding_dimensions = int(embedding_dimensions)
        self._create_client()

    def load(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
        points = []
        for document_chunk in document_chunks:
            metadata = document_chunk.data.metadata.model_dump()
            chunk = document_chunk.data.vectors
            points.append(
                models.PointStruct(
                    id=str(uuid.uuid4()),
                    vector=chunk,
                    payload=metadata
                )
            )
        self._client.upload_points(
            collection_name=self.config.connection_specification.collection_name,
            points=points
        )

    def delete(self, filter, namespace=None):
        logger.info(f"Deleting points with filter: {filter}")
        self._client.delete(
            collection_name=self.config.connection_specification.collection_name,
            points_selector=models.FilterSelector(
                filter=filter
            ),
        )

    def check(self) -> Optional[str]:
        try:
            if not self._client:
                return (False, "Qdrant is not alive")
            available_collections = [
                collection.name for collection in self._client.get_collections().collections]
            distance = DISTANCE_MAP.get(
                self.config.connection_specification.distance.distance)
            collection_name = self.config.connection_specification.collection_name
            if collection_name in available_collections:
                description = self._client.get_collection(
                    collection_name=collection_name)
                if description.config.params.vectors.size != self.embedding_dimensions:
                    return (False,
                            f"Collection {collection_name} has dimension {description.config.params.size}, "
                            f"but the configured dimension is {self.embedding_dimensions}.")
            else:
                self._client.create_collection(
                    collection_name=self.config.connection_specification.collection_name,
                    vectors_config=VectorParams(
                        size=self.embedding_dimensions, distance=distance)
                )
        except Exception as e:
            logger.error(f"Could not connect to Qdrant: {e}")
            raise e
        return True, None

    def _create_client(self):
        url = self.config.connection_specification.url
        self._client = QdrantClient(url)

    def initiate_sync(self, configured_catalog: DatCatalog):
        for stream in configured_catalog.document_streams:
            if stream.write_sync_mode == WriteSyncMode.REPLACE:
                self.delete(
                    filter={self.METADATA_DAT_STREAM_FIELD: stream.name}, namespace=stream.namespace)

    def prepare_metadata_filter(self, filter: Dict[str, Any]) -> Filter:
        conditions = []

        for key, value in filter.items():
            if key == Loader.METADATA_DAT_RUN_ID_FIELD:
                conditions.append(
                    FieldCondition(
                        key=key,
                        match=MatchExcept(**{"except": [value]})
                    )
                )
            elif isinstance(value, str):
                conditions.append(
                    FieldCondition(
                        key=key,
                        match=MatchValue(value=value)
                    )
                )
            elif isinstance(value, list):
                conditions.append(
                    FieldCondition(
                        key=key,
                        match=MatchAny(any=value)
                    )
                )

        return Filter(must=conditions)
