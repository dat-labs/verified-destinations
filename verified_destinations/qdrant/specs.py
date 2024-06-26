# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-05-16T08:48:51+00:00

from __future__ import annotations

from typing import Union, Literal, Optional

from pydantic import BaseModel, Field
from dat_core.pydantic_models import ConnectionSpecification


class Dot(BaseModel):
    distance: str = Field(
        'dot',
        description='Dot product distance',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )


class Cos(BaseModel):
    distance: str = Field(
        'cos',
        description='Cosine distance',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )


class Euc(BaseModel):
    distance: str = Field(
        'euc',
        description='Euclidean distance',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )


class ConnectionSpecificationModel(ConnectionSpecification):
    url: str = Field(..., description='Url of the Qdrant server',
                     title='Qdrant URL')
    collection_name: str = Field(
        ..., description='Name of the collection to use', title='Collection name'
    )
    distance: Union[Euc, Cos, Dot] = Field(
        ..., description='Distance function to use', title='Distance function',
        json_schema_extra={
            'ui-opts': {
                'widget': 'singleDropdown',
            }
        }
    )
    embedding_dimensions: int = Field(
        ..., description='Number of dimensions for the embeddings'
    )


class QdrantSpecification(BaseModel):
    name: Literal['Qdrant']
    module_name: Literal['qdrant']
    documentation_url: Optional[str] = (
        'https://datlabs.gitbook.io/datlabs/integrations/destinations/qdrant'
    )
    connection_specification: ConnectionSpecificationModel = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )
