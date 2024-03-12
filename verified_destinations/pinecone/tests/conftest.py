import os
import pytest
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models.configured_dat_catalog import ConfiguredDatCatalog
from dat_core.pydantic_models.configured_document_stream import ConfiguredDocumentStream, DestinationSyncMode
from dat_core.pydantic_models.dat_document_stream import DatDocumentStream, SyncMode


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
    conf_catalog = ConfiguredDatCatalog(
        document_streams=[
            ConfiguredDocumentStream(
                stream=DatDocumentStream(
                    name="PDF",
                    namespace="pytest_pdf",
                    sync_mode=SyncMode.INCREMENTAL
                ),
                namespace="pytest_pdf",
                sync_mode=SyncMode.INCREMENTAL,
                destination_sync_mode=DestinationSyncMode.UPSERT
            ),
            ConfiguredDocumentStream(
                stream=DatDocumentStream(
                    name="CSV",
                    namespace="pytest_csv",
                    sync_mode=SyncMode.INCREMENTAL
                ),
                namespace="pytest_csv",
                sync_mode=SyncMode.INCREMENTAL,
                destination_sync_mode=DestinationSyncMode.APPEND
            )
        ]
    )
    yield conf_catalog
