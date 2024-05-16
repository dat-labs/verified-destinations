# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-05-16T08:48:51+00:00

from __future__ import annotations

from typing import Union

from pydantic import BaseModel, Field
from dat_core.pydantic_models import ConnectionSpecification


class Dot(BaseModel):
    distance: str = Field('dot', const=True)


class Cos(BaseModel):
    distance: str = Field('cos', const=True)


class Euc(BaseModel):
    distance: str = Field('euc', const=True)


class ConnectionSpecificationModel(ConnectionSpecification):
    url: str = Field(..., description='Url of the Qdrant server', title='Qdrant URL')
    collection_name: str = Field(
        ..., description='Name of the collection to use', title='Collection name'
    )
    distance: Union[Euc, Cos, Dot] = Field(
        ..., description='Distance function to use', title='Distance function'
    )


class QdrantSpecification(BaseModel):

    connection_specification: ConnectionSpecificationModel = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )