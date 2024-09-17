import os
import json
import pytest
from dat_core.pydantic_models import (
    ConnectorSpecification, DatCatalog,
    DatDocumentStream, ReadSyncMode, WriteSyncMode,
    Type,
)


@pytest.fixture(scope="class")
def valid_connection_object(request):
    yield {
        "url": os.getenv("QDRANT_URL"),
        "collection_name": os.getenv("QDRANT_COLLECTION_NAME"),
        "distance": json.loads(os.getenv("QDRANT_DISTANCE")),
        "embedding_dimensions": os.getenv("EMBEDDING_DIMENSIONS"),
    }


@pytest.fixture(scope="class")
def conf_catalog(request):
    conf_catalog = DatCatalog(
        document_streams=[
            DatDocumentStream(
                name="actor_instances",
                namespace="pytest_actor_instances",
                read_sync_mode=ReadSyncMode.INCREMENTAL,
                write_sync_mode=WriteSyncMode.UPSERT,
            ),
            DatDocumentStream(
                name="PDF",
                namespace="pytest_unstructured_document",
                read_sync_mode=ReadSyncMode.INCREMENTAL,
                write_sync_mode=WriteSyncMode.UPSERT,
            ),
        ]
    )
    yield conf_catalog

@pytest.fixture(scope="class")
def records(request):
    yield {
        "actor_instances": [
            {
                "type": Type.STATE,
                "stream_state": {
                    "data": {},
                    "stream_status": "STARTED"
                },
            },
            {
                "type": Type.RECORD,
                "document_chunk": "id: c57cf7fa-9013-4ddb-91b6-1f85e5b588d1",
                "vectors": [0.4] * 1536,
                "metadata": {
                    "dat_source": "postgres",
                    "dat_document_chunk": "id: c57cf7fa-9013-4ddb-91b6-1f85e5b588d1",
                    "dat_stream": "actor_instances",
                    "dat_document_entity": "public_actor_instances",
                    "dat_record_id": "public_actor_instances_c57cf7fa-9013-4ddb-91b6-1f85e5b588d1",
                    "dat_run_id": "7c3f04fafccc4d6090e5c2ec94bd6c827"
                }
            },
            # {
            #    "type": Type.RECORD,
            #     "document_chunk": "id: 1a2d2b0d-1d0f-4e7b-8e7e-0c3f5b4d7c2d",
            #     "vectors": [0.2] * 1536,
            #     "metadata": {
            #         "dat_source": "postgres",
            #         "dat_document_chunk": "id: 1a2d2b0d-1d0f-4e7b-8e7e-0c3f5b4d7c2d",
            #         "dat_stream": "actor_instances",
            #         "dat_document_entity": "public_actor_instances",
            #         "dat_record_id": "public_actor_instances_1a2d2b0d-1d0f-4e7b-8e7e-0c3f5b4d7c2d2",
            #         "dat_run_id": "7c3f04fafccc4d6090e5c2ec94bd6c827"
            #     }
            # },
            {"type": Type.RECORD,
                "document_chunk": "id: 1a2d2b0d-1d0f-4e7b-8e7e-0c3f5b4d7c2ck",
                "vectors": [0.5] * 1536,
                "metadata": {
                    "dat_source": "postgres",
                    "dat_document_chunk": "id: 1a2d2b0d-1d0f-4e7b-8e7e-0c3f5b4d7c2ck",
                    "dat_stream": "actor_instances",
                    "dat_document_entity": "public_actor_instances",
                    "dat_record_id": "public_actor_instances_1a2d2b0d-1d0f-4e7b-8e7e-0c3f5b4d7c2ck",
                    "dat_run_id": "7c3f04fafccc4d6090e5c2ec94bd6c827"
                }
             },
             {
                 "type": Type.STATE,
                    "stream_state": {
                        "data": {"last_emitted_at": 2},
                        "stream_status": "COMPLETED"
                    },
             }
        ],
        "PDF": [
           {
                "type": Type.STATE,
                "stream_state": {
                    "data": {},
                    "stream_status": "STARTED"
                },
            },
            {
                "type": Type.RECORD,
                "document_chunk": "An Orange PDF first chunk",
                "vectors": [0.1] * 1536,
                "metadata": {
                    "dat_source": "GoogleDrive",
                    "dat_document_chunk": "An Orange PDF first chunk",
                    "dat_stream": "PDF",
                    "dat_document_entity": "/Apple/Orange.pdf",
                    "dat_record_id": "/Apple/Orange.pdf",
                    "dat_run_id": "7c3f04fafccc4d6090e5c2ec94bd6c827"
                }
            },
            {
                "type": Type.RECORD,
                "document_chunk": "An Orange PDF second chunk",
                "vectors": [0.1] * 1536,
                "metadata": {
                    "dat_source": "GoogleDrive",
                    "dat_document_chunk": "An Orange PDF second chunk",
                    "dat_stream": "PDF",
                    "dat_document_entity": "/Apple/Orange.pdf",
                    "dat_record_id": "/Apple/Orange.pdf",
                    "dat_run_id": "7c3f04fafccc4d6090e5c2ec94bd6c827"
                }
            },
            {
                "type": Type.RECORD,
                "document_chunk": "An Orange PDF third chunk",
                "vectors": [0.1] * 1536,
                "metadata": {
                    "dat_source": "GoogleDrive",
                    "dat_document_chunk": "An Orange PDF third chunk",
                    "dat_stream": "PDF",
                    "dat_document_entity": "/Apple/Orange.pdf",
                    "dat_record_id": "/Apple/Orange.pdf",
                    "dat_run_id": "7c3f04fafccc4d6090e5c2ec94bd6c827"
                }
            },
            {
                 "type": Type.STATE,
                    "stream_state": {
                        "data": {"last_emitted_at": 2},
                        "stream_status": "COMPLETED"
                    },
             }
        ],
    }
