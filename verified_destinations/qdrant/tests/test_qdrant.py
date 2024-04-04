import os
import yaml
from typing import List
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models.dat_connection_status import Status
from verified_destinations.qdrant.destination import Qdrant
from dat_core.pydantic_models import (
    DatMessage, DatDocumentMessage,
    Data, DatStateMessage, StreamState,
    StreamStatus, DatDocumentStream,
    Type, DatCatalog
)


class TestQdrant:

    def test_spec(self, ):
        """
        GIVEN None
        WHEN spec() is called on a valid Destination class
        THEN spec stated in ./specs/ConnectorSpecification.yml is returned
        """
        spec = Qdrant().spec()
        current_dir = os.path.dirname(os.path.abspath(__file__))   
        yaml_path = os.path.join(current_dir, "..", "specs.yml")
        with open(yaml_path) as yaml_in:
            schema = yaml.safe_load(yaml_in)
            assert schema == spec

    def test_check(self, config):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN check() is called on a valid Destination class
        THEN no error is raised
        """
        check = Qdrant().check(
            config=config)
        print(check)
        assert check.status == Status.SUCCEEDED

    def test_write(self, config, conf_catalog):
        """
        Given a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class
        THEN no error is raised
        """
        configured_catalog = DatCatalog.model_validate_json(conf_catalog.json())
        first_record = DatMessage(
                type=Type.RECORD,
                record=DatDocumentMessage(
                    data=Data(
                        document_chunk='foo',
                        vectors=[0.1] * 1536,
                        metadata={"meta": "Objective", "dat_source": "S3",
                                  "dat_stream": "PDF", "dat_document_entity": "DBT/DBT Overview.pdf"},
                    ),
                    emitted_at=1,
                    namespace=configured_catalog.document_streams[0].namespace,
                    stream=DatDocumentStream(
                        name=configured_catalog.document_streams[0].name,
                        namespace=configured_catalog.document_streams[0].namespace,
                        read_sync_mode="INCREMENTAL",
                    ),
                ),
            )
        second_record = DatMessage(
                type=Type.RECORD,
                record=DatDocumentMessage(
                    data=Data(
                        document_chunk='bar',
                        vectors=[1.0] * 1536,
                        metadata={"meta": "Arbitrary", "dat_source": "S3",
                                  "dat_stream": "CSV", "dat_document_entity": "Apple/DBT/DBT Overview.pdf"},
                    ),
                    emitted_at=2,
                    namespace=configured_catalog.document_streams[1].namespace,
                    stream=DatDocumentStream(
                        name=configured_catalog.document_streams[1].name,
                        namespace=configured_catalog.document_streams[1].namespace,
                        read_sync_mode="INCREMENTAL",
                    )
                ),
            )
        mocked_input: List[DatMessage] = [
            first_record,
            second_record,
        ]
        docs = Qdrant().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        for doc in docs:
            print(f"doc: {doc}")
            assert isinstance(doc, DatMessage)

    def test_write_multiple_streams(self, config, conf_catalog):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class
        THEN no error is raised
        """
        comp_state_msgs = []
        configured_catalog = DatCatalog.model_validate_json(
            conf_catalog.json())
        first_record = DatMessage(
            type=Type.RECORD,
            record=DatDocumentMessage(
                data=Data(
                    document_chunk='foo',
                    vectors=[1.0] * 1536,
                    metadata={"meta": "Objective", "dat_source": "S3",
                                "dat_stream": "PDF", "dat_document_entity": "DBT/DBT Overview.pdf"},
                ),
                emitted_at=1,
                namespace=configured_catalog.document_streams[0].namespace,
                stream=DatDocumentStream(
                    name=configured_catalog.document_streams[0].name,
                    namespace=configured_catalog.document_streams[0].namespace,
                    read_sync_mode="INCREMENTAL",
                ),
            ),
        )
        second_record = DatMessage(
            type=Type.RECORD,
            record=DatDocumentMessage(
                data=Data(
                    document_chunk='bar',
                    vectors=[1.1] * 1536,
                    metadata={"meta": "Arbitrary", "dat_source": "S3",
                                "dat_stream": "CSV", "dat_document_entity": "Apple/DBT/DBT Overview.pdf"},
                ),
                emitted_at=2,
                namespace=configured_catalog.document_streams[1].namespace,
                stream=DatDocumentStream(
                    name=configured_catalog.document_streams[1].name,
                    namespace=configured_catalog.document_streams[1].namespace,
                    read_sync_mode="INCREMENTAL",
                )
            ),
        )
        mocked_input: List[DatMessage] = [
            DatMessage(
                type=Type.STATE,
                state=DatStateMessage(
                    stream=DatDocumentStream(
                        name=configured_catalog.document_streams[0].name,
                        namespace=configured_catalog.document_streams[0].namespace,
                        read_sync_mode="INCREMENTAL"
                    ),
                    stream_state=StreamState(
                        data={},
                        stream_status=StreamStatus.STARTED
                    )
                ),
                record=first_record.record
            ),
            first_record,
            DatMessage(
                type=Type.STATE,
                state=DatStateMessage(
                    stream=DatDocumentStream(
                        name=configured_catalog.document_streams[1].name,
                        namespace=configured_catalog.document_streams[1].namespace,
                        read_sync_mode="INCREMENTAL",
                    ),
                    stream_state=StreamState(
                        data={},
                        stream_status=StreamStatus.STARTED
                    )
                ),
                record=second_record.record
            ),
            second_record,
            DatMessage(
                type=Type.STATE,
                state=DatStateMessage(
                    stream=DatDocumentStream(
                        name=configured_catalog.document_streams[0].name,
                        namespace=configured_catalog.document_streams[0].namespace,
                        read_sync_mode="INCREMENTAL",
                    ),
                    stream_state=StreamState(
                        data={"last_emitted_at": 2},
                        stream_status=StreamStatus.COMPLETED
                    )
                ),
            ),
            DatMessage(
                type=Type.STATE,
                state=DatStateMessage(
                    stream=DatDocumentStream(
                        name=configured_catalog.document_streams[1].name,
                        namespace=configured_catalog.document_streams[1].namespace,
                        read_sync_mode="INCREMENTAL",
                    ),
                    stream_state=StreamState(
                        data={"last_emitted_at": 2},
                        stream_status=StreamStatus.COMPLETED
                    )
                ),
            ),
        ]
        docs = Qdrant().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        for doc in docs:
            print(f"doc: {doc}")
            if doc.state.stream_state.stream_status == StreamStatus.COMPLETED:
                comp_state_msgs.append(doc)
            assert isinstance(doc, DatMessage)
        assert len(comp_state_msgs) == 2
        assert comp_state_msgs[0].state.stream_state.stream_status == StreamStatus.COMPLETED
        assert comp_state_msgs[1].state.stream_state.stream_status == StreamStatus.COMPLETED
