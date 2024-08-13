import yaml
import os
from typing import List
from dat_core.pydantic_models import (
    DatMessage, DatDocumentMessage,
    Data, DatStateMessage,
    StreamState, StreamStatus,
    DatDocumentStream, Type,
    DatCatalog, Status,
    StreamMetadata,
)
from verified_destinations.pinecone.destination import Pinecone
from verified_destinations.pinecone.specs import PineconeSpecification
from verified_destinations.pinecone.tests.conftest import record_list, NAMESPACE_TEST
from pinecone import Pinecone as PC

from time import sleep

class TestPinecone:
    def test_env(self, valid_connection_specification):
        """
        Checks if all the values in the `valid_connection_specification` dictionary are not empty.
        If any value is empty, raises a `ValueError` with a message indicating which key is missing.
        
        :param valid_connection_specification: A dictionary containing the connection specification.
        :type valid_connection_specification: dict
        
        :raises ValueError: If any value in the `valid_connection_specification` dictionary is empty.
        """
        for key,value in valid_connection_specification.items():
            if not value:
                raise ValueError(f"Missing {key}. Please set it as an environment variable and try again.")
            
            
    def test_spec(self, ):
        """
        GIVEN None
        WHEN spec() is called on a valid Destination class
        THEN spec stated in ./specs/ConnectorSpecification.yml is returned
        """
        spec = Pinecone().spec()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.join(current_dir, "..", "specs.yml")
        with open(yaml_path) as yaml_in:
            schema = yaml.safe_load(yaml_in)
            assert schema == spec

    def test_check(self, valid_connection_specification):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN check() is called on a valid Destination class
        THEN no error is raised
        """
        check = Pinecone().check(
            config=PineconeSpecification(
                name='Pinecone',
                module_name='pinecone',
                connection_specification=valid_connection_specification
            ))
        assert check.status == Status.SUCCEEDED

    def test_write(self, valid_connection_specification, valid_catalog_object):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class
        THEN no error is raised
        """
        clear_namespaces(valid_connection_specification)

        config = PineconeSpecification(
            name='Pinecone',
            module_name='pinecone',
            connection_specification=valid_connection_specification
        )
        
        records = record_list(category='NEW', write_sync_mode='APPEND')
        mocked_input: List[DatMessage] = [DatMessage(**record) for record in records]
        
        configured_catalog = DatCatalog(**valid_catalog_object)
        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        sleep(3)
        
        for doc in docs: assert isinstance(doc, DatMessage)
        
        # Checking if the records stored in Pinecone is same 
        # as the records passed in the write() call
        query_records = get_query(
                valid_connection_specification=valid_connection_specification,
                namespace=NAMESPACE_TEST, 
                top_k=len(records)+1, 
                filter={},
            )
        assert len(query_records['matches']) == len(records)

    def test_write_multiple_streams(self, valid_connection_specification, valid_catalog_object):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class
        THEN no error is raised
        """
        comp_state_msgs = []
        configured_catalog = DatCatalog(**valid_catalog_object)
        records = record_list()
        first_record = DatMessage(**records[0])
        second_record = DatMessage(**records[1])
        mocked_input: List[DatMessage] = [
            DatMessage(
                type=Type.STATE,
                state=DatStateMessage(
                    stream=configured_catalog.document_streams[0],
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
                    stream=configured_catalog.document_streams[1],
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
                    stream=configured_catalog.document_streams[0],
                    stream_state=StreamState(
                        data={"last_emitted_at": 2},
                        stream_status=StreamStatus.COMPLETED
                    )
                ),
            ),
            DatMessage(
                type=Type.STATE,
                state=DatStateMessage(
                    stream=configured_catalog.document_streams[1],
                    stream_state=StreamState(
                        data={"last_emitted_at": 2},
                        stream_status=StreamStatus.COMPLETED
                    )
                ),
            ),
        ]

        config = PineconeSpecification(
            name='Pinecone',
            module_name='pinecone',
            connection_specification=valid_connection_specification
        )

        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        for doc in docs:
            print(f"doc: {doc}")
            if doc.state:
                if doc.state.stream_state.stream_status == StreamStatus.COMPLETED:
                    comp_state_msgs.append(doc)
            assert isinstance(doc, DatMessage)
        assert len(comp_state_msgs) == 2
        assert comp_state_msgs[0].state.stream_state.stream_status == StreamStatus.COMPLETED
        assert comp_state_msgs[1].state.stream_state.stream_status == StreamStatus.COMPLETED

    def test_write_replace(self, valid_connection_specification, valid_catalog_object):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class with replace mode
        THEN no error is raised and the document is replaced
        """
        clear_namespaces(valid_connection_specification)
        config = PineconeSpecification(
            name='Pinecone',
            module_name='pinecone',
            connection_specification=valid_connection_specification
        )

        configured_catalog = DatCatalog(**valid_catalog_object)
        old_records = record_list(category='OLD', write_sync_mode='APPEND')
        mocked_input: List[DatMessage] = [DatMessage(**record) for record in old_records]
        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        sleep(3)

        new_records = record_list(category='NEW', write_sync_mode='REPLACE')
        mocked_input: List[DatMessage] = [DatMessage(**record) for record in new_records]
        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        sleep(3)

        for doc in docs:
            assert isinstance(doc, DatMessage)
        
        query_result = get_query(
            valid_connection_specification=valid_connection_specification,
            namespace=NAMESPACE_TEST, 
            top_k=10,
            filter={},
        )
        
        assert len(query_result['matches']) == len(new_records)
        
        
    def test_write_append(self, valid_connection_specification, valid_catalog_object):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class with append mode
        THEN no error is raised and the document is appended
        """
        clear_namespaces(valid_connection_specification)
        config = PineconeSpecification(
            name='Pinecone',
            module_name='pinecone',
            connection_specification=valid_connection_specification
        )

        configured_catalog = DatCatalog(**valid_catalog_object)
        old_records = record_list(category='OLD', write_sync_mode='APPEND')
        mocked_input: List[DatMessage] = [DatMessage(**record) for record in old_records]
        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        sleep(3)

        new_records = record_list(category='NEW', write_sync_mode='APPEND')
        mocked_input: List[DatMessage] = [DatMessage(**record) for record in new_records]
        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        sleep(3)

        for doc in docs:
            assert isinstance(doc, DatMessage)
        
        query_result = get_query(
            valid_connection_specification=valid_connection_specification,
            namespace=NAMESPACE_TEST, 
            top_k=10,
            filter={},
        )
        
        assert len(query_result['matches']) == len(new_records) + len(old_records)
    
    def test_write_upsert(self, valid_connection_specification, valid_catalog_object):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN write() is called on a valid Destination class with upsert mode
        THEN no error is raised and the document is upserted
        """
        clear_namespaces(valid_connection_specification)
        config = PineconeSpecification(
            name='Pinecone',
            module_name='pinecone',
            connection_specification=valid_connection_specification
        )

        configured_catalog = DatCatalog(**valid_catalog_object)
        old_records = record_list(category='OLD', write_sync_mode='APPEND')
        mocked_input: List[DatMessage] = [DatMessage(**record) for record in old_records]
        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        sleep(3)

        new_records = record_list(category='NEW', write_sync_mode='UPSERT')
        mocked_input: List[DatMessage] = [DatMessage(**record) for record in new_records]
        docs = Pinecone().write(
            config=config,
            configured_catalog=configured_catalog,
            input_messages=mocked_input
        )
        sleep(3)

        for doc in docs:
            assert isinstance(doc, DatMessage)
        
        query_result = get_query(
            valid_connection_specification=valid_connection_specification,
            namespace=NAMESPACE_TEST, 
            top_k=10,
            filter={},
        )
        
        uniques = set()
        for record in old_records:
            uniques.add(record['record']['data']['metadata']['dat_document_entity'])
        for record in new_records:
            uniques.add(record['record']['data']['metadata']['dat_document_entity'])

        assert len(query_result['matches']) == len(uniques)
    


def pinecone_client(valid_connection_specification, connection_specification=None):
    if connection_specification:
        pinecone_client = PC(
            api_key=connection_specification['pinecone_api_key'],
            environment=connection_specification['pinecone_environment']
            )
    else:
        pinecone_client = PC(
            api_key=valid_connection_specification['pinecone_api_key'],
            environment=valid_connection_specification['pinecone_environment']
            )
    return pinecone_client  

def clear_namespaces(valid_connection_specification):
    client = pinecone_client(valid_connection_specification)
    index = client.Index(valid_connection_specification['pinecone_index'])
    index_namespaces = index.describe_index_stats()['namespaces'].keys()
    if NAMESPACE_TEST in index_namespaces:
        index.delete(deleteAll=True, namespace=NAMESPACE_TEST)
    sleep(3)

def get_metadata_filter(metadata):
    metadata_filter = {}
    for metadata_key, metadata_value in metadata.items():
        metadata_filter[metadata_key] = {"$eq": metadata_value}
    return metadata_filter

def get_query(valid_connection_specification, namespace, top_k, filter):
    client = pinecone_client(valid_connection_specification)
    index = client.Index(valid_connection_specification['pinecone_index'])
    response = index.query(
        namespace=namespace,
        filter=get_metadata_filter(filter),
        vector=[1]*int(valid_connection_specification['embedding_dimensions']),
        top_k=top_k,
        include_values=True,
        include_metadata=True
    )
    return response
