import os
import pytest
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, ReadSyncMode, WriteSyncMode


@pytest.fixture()
def valid_connection_specification():
    yield {
        'pinecone_index': os.getenv('PINECONE_INDEX'),
        'pinecone_api_key': os.getenv('PINECONE_API_KEY'),
        'pinecone_environment': os.getenv('PINECONE_ENVIRONMENT'),
        'embedding_dimensions': os.getenv('PINECONE_EMBEDDING_DIMENSIONS'),
    }

@pytest.fixture()
def valid_document_streams():
    yield {
        'document_streams': [
            {
                'name': 'PDF',
                'namespace': 'pytest_pdf',
                'read_sync_mode': 'INCREMENTAL',
                'write_sync_mode': 'UPSERT',
            },
            {
                'name': 'CSV',
                'namespace': 'pytest_csv',
                'read_sync_mode': 'INCREMENTAL',
                'write_sync_mode': 'APPEND',
            }
        ]
    }
