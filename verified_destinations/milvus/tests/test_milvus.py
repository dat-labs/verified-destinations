from typing import List
from dat_core.pydantic_models import (
    DatConnectionStatus, DatDocumentStream,
    DatMessage, Data, Type,
    DatCatalog, DatDocumentMessage
)
from verified_destinations.milvus.destination import Milvus
# from verified_destinations.milvus.catalog import MilvusCatalog
from verified_destinations.milvus.specs import MilvusSpecification


class TestMilvus:
    def test_check(self, config):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN check() is called on a valid Destination class
        THEN no error is raised
        """
        check_connection_tpl = Milvus().check(
            config=config)
        print(check_connection_tpl)
        assert isinstance(check_connection_tpl, DatConnectionStatus)
        assert check_connection_tpl.status.name == 'SUCCEEDED'
    
    def test_write(self, config, conf_catalog):
        """
        GIVEN a valid connectionSpecification JSON config
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
                        write_sync_mode="REPLACE",
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
                        write_sync_mode="REPLACE",
                    )
                ),
            )
        mocked_input: List[DatMessage] = [
            first_record,
            second_record,
        ]
        print(f"mocked_input: {mocked_input}")
        docs = Milvus().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        for doc in docs:
            print(f"doc: {doc}")
            assert isinstance(doc, DatMessage)


    # def test_write(valid_connection_object, valid_catalog_object, valid_dat_record_message):
    #     config = MilvusSpecification(
    #         name='Milvus',
    #         connection_specification=valid_connection_object,
    #         module_name='milvus'
    #     )

    #     milvus = Milvus()
    #     messages = milvus.write(
    #         config=config,
    #         catalog=MilvusCatalog(**valid_catalog_object),
    #         input_messages=[valid_dat_record_message],
    #     )
    #     for message in messages:
    #         assert DatDocumentStream.model_validate(message)
