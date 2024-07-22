import os
import pytest
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, ReadSyncMode, WriteSyncMode


@pytest.fixture(scope="class")
def config(request):
    yield {
        "clster_url": os.getenv("WEAVIATE_CLUSTER_URL"),
        "authentication": {
            "authentication": "api_key_authentication",
            "api_key": os.getenv("WEAVIATE_API_KEY")
        },
    }


@pytest.fixture(scope="class")
def conf_catalog(request):
    conf_catalog = DatCatalog(
        document_streams=[
            DatDocumentStream(
                name="PDF",
                namespace="pytest_pdf",
                read_sync_mode=ReadSyncMode.INCREMENTAL,
                write_sync_mode=WriteSyncMode.UPSERT,
            ),
            DatDocumentStream(
                name="CSV",
                namespace="pytest_csv",
                read_sync_mode=ReadSyncMode.INCREMENTAL,
                write_sync_mode=WriteSyncMode.APPEND,
            )
        ]
    )
    yield conf_catalog
