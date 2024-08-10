import os
import pytest
from time import sleep
from pinecone import Pinecone

NAMESPACE_TEST = 'pytest'

@pytest.fixture(scope='function')
def valid_connection_specification(request):
    pinecone_api_key = os.getenv('PINECONE_API_KEY')
    pinecone_index = os.getenv('PINECONE_INDEX')
    pinecone_environment = os.getenv('PINECONE_ENVIRONMENT')
    embedding_dimensions = os.getenv('EMBEDDING_DIMENSIONS')

    yield {
        'pinecone_index': pinecone_index,
        'pinecone_api_key': pinecone_api_key,
        'pinecone_environment': pinecone_environment,
        'embedding_dimensions': embedding_dimensions,
    }

@pytest.fixture(scope='function')
def valid_catalog_object(request):
    write_sync_modes = ['APPEND', 'REPLACE', 'UPSERT']
    document_streams = { 'document_streams': [] }
    
    for name in ['PDF', 'CSV']:
        for write_sync_mode in write_sync_modes:
            document_streams['document_streams'].append({
                'name': name,
                'namespace': NAMESPACE_TEST,
                'read_sync_mode': 'INCREMENTAL',
                'write_sync_mode': write_sync_mode,
            })

    yield document_streams

old_record_metadata = [
    {   
        'meta': 'Objective', 
        'dat_source': 'S3',
        'dat_stream': 'PDF',
        'dat_document_entity': 'DBT/DBT Overview.pdf',
    },
    {
        'meta': 'Arbitrary', 
        'dat_source': 'S3',
        'dat_stream': 'PDF',
        'dat_document_entity': 'Google/Tutorials search_algorithms.pdf',
    },
]

new_record_metadata = [
    {
        'meta': 'Objective', 
        'dat_source': 'S3',
        'dat_stream': 'PDF',
        'dat_document_entity': 'DBT/DBT Overview.pdf'
    },
    {
        'meta': 'Arbitrary', 
        'dat_source': 'S3',
        'dat_stream': 'PDF',
        'dat_document_entity': 'DAT/docs Introduction.pdf'
    },
    {
        'meta': 'Objective', 
        'dat_source': 'S3',
        'dat_stream': 'CSV',
        'dat_document_entity': 'Apple/DBT/DBT Employee.csv'
    },
]

def record_list(write_sync_mode='APPEND', category='NEW'):
    """
    Returns a list of valid records.

    Args:
        write_sync_mode (str, optional): Possible values: ['APPEND', 'REPLACE', 'UPSERT']. Defaults to 'APPEND'.
        category (str, optional): Possible values: ['NEW', 'OLD']. Defaults to 'NEW'.
    """
    embedding_dimensions = int(os.getenv('EMBEDDING_DIMENSIONS'))
    if not embedding_dimensions:
        raise ValueError('EMBEDDING_DIMENSIONS is not set')
    
    extra_metadata = {}

    if category == 'NEW':
        record_metadata = new_record_metadata
        extra_metadata['document_chunk'] = 'New Data'
    elif category == 'OLD':
        record_metadata = old_record_metadata
        extra_metadata['document_chunk'] = 'Old Data'

    vectors = [1.0] * embedding_dimensions

    records = list()
    for metadata in record_metadata:
        metadata.update(extra_metadata)
        records.append(
            {
                'type': 'RECORD',
                'record': {
                    'data': {
                        'document_chunk': metadata['document_chunk'],
                        'vectors': vectors,
                        'metadata': metadata
                    },
                    'emitted_at': 1,
                    'namespace': NAMESPACE_TEST,
                    'stream': {
                        'name': metadata['dat_stream'],
                        'namespace': NAMESPACE_TEST,
                        'read_sync_mode': 'INCREMENTAL',
                        'write_sync_mode': write_sync_mode
                    }
                }
            }
        )
    return records

def pytest_sessionfinish(session, exitstatus):
    pinecone_api_key = os.getenv('PINECONE_API_KEY')
    pinecone_index = os.getenv('PINECONE_INDEX')
    if pinecone_api_key and pinecone_index:
        client = Pinecone(api_key=pinecone_api_key)
        index = client.Index(pinecone_index)
        index_namespaces = index.describe_index_stats()['namespaces'].keys()
        if NAMESPACE_TEST in index_namespaces:
            index.delete(deleteAll=True, namespace=NAMESPACE_TEST)
    else:
        print('Pinecone API key or index not set. Skipping teardown.')