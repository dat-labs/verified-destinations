import yaml
from typing import List
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models.dat_connection_status import Status
from conftest import *
from verified_destinations.qdrant.destination import Qdrant
from dat_core.pydantic_models.dat_message import (DatMessage, DatDocumentMessage,
                                         Data, DatStateMessage, StreamState,
                                         StreamStatus, DatDocumentStream,
                                         Type)
from dat_core.pydantic_models.dat_catalog import DatCatalog


class TestQdrant:

    DESTINATION_CONFIG_FILE = "./verified_destinations/qdrant/destination_config.json"

    def test_spec(self, ):
        """
        GIVEN None
        WHEN spec() is called on a valid Destination class
        THEN spec stated in ./specs/ConnectorSpecification.yml is returned
        """
        spec = Qdrant().spec()
        with open('./verified_destinations/qdrant/specs.yml') as yaml_in:
            schema = yaml.safe_load(yaml_in)
            assert schema == spec

    def test_check(self, ):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN check() is called on a valid Destination class
        THEN no error is raised
        """
        config = ConnectorSpecification.model_validate_json(
            open(self.DESTINATION_CONFIG_FILE).read(), )
        check = Qdrant().check(
            config=config)
        print(check)
        assert check.status == Status.SUCCEEDED
    
    def test_write(self, ):
        """
        Given a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class
        THEN no error is raised
        """
        config = ConnectorSpecification.model_validate_json(
            open(self.DESTINATION_CONFIG_FILE).read(), )
        configured_catalog = DatCatalog.model_validate_json(
            open('./verified_destinations/qdrant/configured_catalog.json').read(), )
        first_record = DatMessage(
                type=Type.RECORD,
                record=DatDocumentMessage(
                    data=Data(
                        document_chunk='foo',
                        vectors=[0.0] * 1536,
                        metadata={"meta": "Objective", "dat_source": "S3",
                                  "dat_stream": "PDF", "dat_document_entity": "DBT/DBT Overview.pdf"},
                    ),
                    emitted_at=1,
                    namespace="pytest_seeder",
                    stream=DatDocumentStream(
                        name="S3",
                        namespace="pytest_seeder",
                        sync_mode="incremental",
                    ),
                ),
            )
        second_record = DatMessage(
                type=Type.RECORD,
                record=DatDocumentMessage(
                    data=Data(
                        document_chunk='bar',
                        vectors=[0.0] * 1536,
                        metadata={"meta": "Arbitrary", "dat_source": "S3",
                                  "dat_stream": "PDF", "dat_document_entity": "Apple/DBT/DBT Overview.pdf"},
                    ),
                    emitted_at=2,
                    namespace="pytest_seeder",
                    stream=DatDocumentStream(
                        name="S3",
                        namespace="pytest_seeder",
                        sync_mode="incremental",
                    )
                ),
            )
        mocked_input: List[DatMessage] = [
            first_record,
            second_record,
        ]
        check  = Qdrant().check(
            config=config)
        # import pdb;pdb.set_trace()
        assert check.status == Status.SUCCEEDED

        docs = Qdrant().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        for doc in docs:
            print(f"doc: {doc}")
            assert isinstance(doc, DatMessage)

    def test_write_state_started(self, ):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class
        THEN no error is raised
        """
        config = ConnectorSpecification.model_validate_json(
            open(self.DESTINATION_CONFIG_FILE).read(), )
        configured_catalog = DatCatalog.model_validate_json(
            open('./verified_destinations/qdrant/configured_catalog.json').read(), )
        first_record = DatMessage(
                type=Type.RECORD,
                record=DatDocumentMessage(
                    data=Data(
                        document_chunk='foo',
                        vectors=[0.0] * 1536,
                        metadata={"meta": "Objective", "dat_source": "S3",
                                  "dat_stream": "PDF", "dat_document_entity": "DBT/DBT Overview.pdf"},
                    ),
                    emitted_at=1,
                    namespace="pytest_seeder",
                    stream=DatDocumentStream(
                        name="S3",
                        namespace="pytest_seeder",
                        sync_mode="incremental",
                    ),
                ),
            )
        second_record = DatMessage(
                type=Type.RECORD,
                record=DatDocumentMessage(
                    data=Data(
                        document_chunk='bar',
                        vectors=[0.0] * 1536,
                        metadata={"meta": "Arbitrary", "dat_source": "S3",
                                  "dat_stream": "PDF", "dat_document_entity": "Apple/DBT/DBT Overview.pdf"},
                    ),
                    emitted_at=2,
                    namespace="pytest_seeder",
                    stream=DatDocumentStream(
                        name="S3",
                        namespace="pytest_seeder",
                        sync_mode="incremental",
                    )
                ),
            )
        mocked_input: List[DatMessage] = [
            DatMessage(
                type=Type.STATE,
                state=DatStateMessage(
                    stream=DatDocumentStream(
                        name="S3",
                        namespace="pytest_seeder",
                        sync_mode="incremental",
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
                        name="S3",
                        namespace="pytest_seeder",
                        sync_mode="incremental",
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
                        name="S3",
                        namespace="pytest_seeder",
                        sync_mode="incremental",
                    ),
                    stream_state=StreamState(
                        data={"last_emitted_at": 2},
                        stream_status=StreamStatus.COMPLETED
                    )
                ),
            ),
        ]
        check  = Qdrant().check(
            config=config)
        # import pdb;pdb.set_trace()
        assert check.status == Status.SUCCEEDED

        docs = Qdrant().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )

        for doc in docs:
            print(f"doc: {doc}")
            assert isinstance(doc, DatMessage)