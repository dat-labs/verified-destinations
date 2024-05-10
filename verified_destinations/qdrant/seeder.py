import uuid
from qdrant_client import QdrantClient, models
from typing import Any, List, Optional
from dat_core.connectors.destinations.seeder import Seeder
from qdrant_openapi_client.models import VectorParams, Distance
from dat_core.pydantic_models import (
    DatDocumentMessage, DatCatalog,
    WriteSyncMode, StreamMetadata
)


DISTANCE_MAP = {
    "dot": Distance.DOT,
    "cos": Distance.COSINE,
    "euc": Distance.EUCLID,
}

class QdrantSeeder(Seeder):
    def __init__(self, config: Any, embedding_dimensions: int):
        super().__init__(config)
        self.embedding_dimensions = embedding_dimensions
        self._create_client()

    def seed(self, document_chunks: List[DatDocumentMessage], namespace: str, stream: str) -> None:
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
            collection_name=self.config.connection_specification.collection,
            points=points
        )

    def delete(self, filter, namespace=None):
        should_filter = self.metadata_filter(filter)
        self._client.delete(
            collection_name=self.config.connection_specification.collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    should=should_filter
                ),
            )
        )
        # scroll_records = self.scroll(scroll_filter=should_filter)
        # for point in scroll_records:
        #     delete_ids.append(point.id)


    def check(self) -> Optional[str]:
        try:
            if not self._client:
                return (False, "Qdrant is not alive")
            available_collections = [collection.name for collection in self._client.get_collections().collections]
            distance = DISTANCE_MAP.get(self.config.connection_specification.distance)
            collection_name = self.config.connection_specification.collection
            if collection_name in available_collections:
                description = self._client.get_collection(collection_name=collection_name)
                if description.config.params.vectors.size != self.embedding_dimensions:
                    return (False,
                            f"Collection {collection_name} has dimension {description.config.params.size}, "
                            f"but the configured dimension is {self.embedding_dimensions}.")
            else:
                self._client.create_collection(
                    collection_name=self.config.connection_specification.collection,
                    vectors_config=VectorParams(size=self.embedding_dimensions, distance=distance)
                )
        except Exception as e:
            raise e
        return True, description

    def _create_client(self):
        url = self.config.connection_specification.url
        print(f"Qdrant url: {url}")
        self._client = QdrantClient(url)
    
    def scroll(self, scroll_filter: List[models.FieldCondition]):
        scroll_records = self._client.scroll(
            collection_name=self.config.connection_specification.collection,
            scroll_filter=models.Filter(
                should=scroll_filter
            ),
        )
        return scroll_records[0]

    def initiate_sync(self, configured_catalog: DatCatalog):
        should_fields = []
        for stream in configured_catalog.document_streams:
            if stream.write_sync_mode == WriteSyncMode.REPLACE:
                should_fields.append(models.FieldCondition(
                    key=self.METADATA_DAT_STREAM_FIELD,
                    match=models.MatchValue(value=stream.name)
                ))
        self._client.delete(
            collection_name=self.config.connection_specification.collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    should=should_fields
                ),
            )
        )

    def metadata_filter(self, metadata: StreamMetadata) -> Any:
        should_fields = []
        for field in self.METADATA_FILTER_FIELDS:
            if field in metadata.model_dump().keys():
                should_fields.append(models.FieldCondition(
                    key=field,
                    match=models.MatchValue(value=metadata[field])
                ))
        return should_fields
