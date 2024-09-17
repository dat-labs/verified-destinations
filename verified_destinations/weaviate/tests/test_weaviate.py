import yaml
import os
from typing import List
from dat_core.pydantic_models import (
    DatMessage, DatDocumentMessage,
    Data, DatStateMessage,
    StreamState, StreamStatus,
    DatDocumentStream, Type,
    DatCatalog, DatConnectionStatus,
)
from verified_destinations.weaviate.destination import Weaviate
from verified_destinations.weaviate.specs import WeaviateSpecification


class TestWeaviate:


    def test_check(self, valid_connection_object):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN check() is called on a valid Destination class
        THEN no error is raised
        """
        check_connection_tpl = Weaviate().check(
            config=WeaviateSpecification(
                name='Weaviate',
                connection_specification=valid_connection_object,
                module_name='weaviate'
            )
        )
        assert isinstance(check_connection_tpl, DatConnectionStatus)
        assert check_connection_tpl.status.name == 'SUCCEEDED'

    def test_write(self, valid_connection_object, conf_catalog, records):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class
        THEN no error is raised
        """
        config = WeaviateSpecification(
            name='Weaviate',
            connection_specification=valid_connection_object,
            module_name='weaviate',
        )
        configured_catalog = DatCatalog.model_validate_json(
            conf_catalog.json())
        mocked_input: List[DatMessage] = []
        for document_stream in configured_catalog.document_streams:
            for record in records.get(document_stream.name, []):
                if record["type"] == Type.STATE:
                    mocked_input.append(
                        DatMessage(
                            type=Type.STATE,
                            state=DatStateMessage(
                                stream=document_stream,
                                stream_state=StreamState(
                                    data=record["stream_state"]["data"],
                                    stream_status=StreamStatus[record["stream_state"]["stream_status"]]
                                )
                            )
                        )
                    )
                elif record["type"] == Type.RECORD:
                    mocked_input.append(
                        DatMessage(
                            type=Type.RECORD,
                            record=DatDocumentMessage(
                                data=Data(
                                    document_chunk=record["document_chunk"],
                                    vectors=record["vectors"],
                                    metadata=record["metadata"],
                                ),
                                namespace=document_stream.namespace,
                                stream=document_stream
                            ),
                            namespace=document_stream.namespace,
                            stream=document_stream
                        )
                    )
        docs = Weaviate().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        for doc in docs:
            assert isinstance(doc, DatMessage)
        assert False
