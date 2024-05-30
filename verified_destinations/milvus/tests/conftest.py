import os
from pytest import fixture
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, ReadSyncMode, WriteSyncMode
from verified_destinations.milvus.specs import MilvusConnection
from verified_destinations.milvus.specs import MilvusSpecification


@fixture()
def valid_connection_object():
    yield {"uri": "http://localhost:19530","collection_name": "test_collection","embedding_dimension": 1536}
    # yield MilvusConnection(
    #     uri="http://localhost:19530",
    #     collection_name="test_collection",
    #     embedding_dimension=1536
    # )

@fixture()
def valid_catalog_object():
    yield {'document_streams': []}

@fixture()
def valid_dat_record_message():
    yield '{"type":"STATE","log":null,"spec":null,"connectionStatus":null,"catalog":null,"record":null,"state":{"stream":{"name":"pdf","namespace":"foobarqux","json_schema":{},"read_sync_mode":"INCREMENTAL","write_sync_mode":"APPEND","cursor_field":"string","dir_uris":["bak/MySQL/STAGING/for-dat-gdrive-test"]},"stream_state":{"data":{"string":null},"stream_status":"STARTED"}}}' # TODO; fix this

@fixture(scope="class")
def config(request):
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the JSON file
    json_path = os.path.join(current_dir, "..", "secrets", "config.json")
    # Read the JSON file and set the configuration value
    config_data = ConnectorSpecification.model_validate_json(
        open(json_path).read(), )
    yield config_data


@fixture(scope="class")
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
