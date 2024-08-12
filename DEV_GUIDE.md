# Development guide

## Introduction

This is a detailed guide to help you develop your own connector for `verified-destinations` and ensure that all your tests pass.

### Generate stub files for your verified-destinations actor
```bash
dat-cli init
```

Follow the onscreen instructions and you will be met with these lines in your terminal output.

```text
...
verified_destinations/bar/specs.yml written.
verified_destinations/bar/specs.py written.
verified_destinations/bar/destination.py written.
verified_destinations/bar/tests/conftest.py written.
verified_destinations/bar/tests/test_bar.py written.
```

Your `verified-destinations` directory should look something like this:
```text
verified_destinations/
├── {your-destination-connector}/
│   ├── destination.py
│   ├── specs.py
│   ├── specs.yml
│   └── tests
│       ├── conftest.py
│       └── test_{your-destination-connector}.py
└── verified_destination_0/
    ├── ...
    └── ...
```

We have a detailed description for what each file does under our [docs](http://path/to/docs). Here we are providing a short description of them.

### `specs.yml`

Here, we can add any additional parameters (as keys) that we want our destination to have under `connection_specification`.

E.g. If you are developing a Pinecone destination actor, you might want to ask for the `pinecone_index`, `pinecone_environment`, `pinecone_api_key` and `embedding_dimensions` as part of `connection_specification`. You can add it like so:
Example:
```yml
description: Specification of an actor (source/embeddingsdestination/destination)
type: object
...
file truncated for brevity
...
  connection_specification:
    description: ConnectorDefinition specific blob. Must be a valid JSON string.
    type: object
    required:
      - pinecone_index
      - pinecone_environment
      - pinecone_api_key
      - embedding_dimensions
    properties:
      pinecone_index:
        type: string
        description: Name of the Pinecone index to use
        order: 0
      pinecone_environment:
        type: string
        description: Name of the Pinecone environment to use
        order: 1
      pinecone_api_key:
        type: string
        description: Pinecone API key
        order: 2
      embedding_dimensions:
        type: number
        description: Configured embedding_dimensions of the Pinecone index
        order: 3
```

### `specs.py`

This file contains the `specs.yml` file as a [Pydantic](https://docs.pydantic.dev/latest/concepts/models/) model. You can generate it using [`datamodel-codegen`](https://docs.pydantic.dev/latest/integrations/datamodel_code_destination/) or edit manually like so:
Example:
```python
'''
file truncated for brevity
'''
class ConnectionSpecificationModel(ConnectionSpecification):
    pinecone_index: str = Field(..., description='Name of the Pinecone index to use')
    pinecone_environment: str = Field(
        ..., description='Name of the Pinecone environment to use'
    )
    pinecone_api_key: str = Field(..., description='Pinecone API key')
    embedding_dimensions: int = Field(
        ..., description='Number of dimensions for the embeddings'
    )
'''
file truncated for brevity
'''
```

### `destination.py`

#### `check_connection()`
Implement your connection logic here.

Returns `(connections_status, message)`


## Running tests
```bash
pytest verified_destinations/{your-destination-connector}/tests/test_{your-destination-connector}.py 
```