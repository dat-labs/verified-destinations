import os
import pytest
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, ReadSyncMode, WriteSyncMode


@pytest.fixture(scope="class")
def config(request):
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the JSON file
    json_path = os.path.join(current_dir, "..", "secrets", "config.json")
    # Read the JSON file and set the configuration value
    config_data = ConnectorSpecification.model_validate_json(
        open(json_path).read(), )
    yield config_data


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
