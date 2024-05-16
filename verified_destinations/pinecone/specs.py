# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-05-16T08:46:31+00:00

from __future__ import annotations

from pydantic import BaseModel, Field
from dat_core.pydantic_models import ConnectionSpecification


class ConnectionSpecificationModel(ConnectionSpecification):
    pinecone_index: str = Field(..., description='Name of the Pinecone index to use')
    pinecone_environment: str = Field(
        ..., description='Name of the Pinecone environment to use'
    )
    pinecone_api_key: str = Field(..., description='Pinecone API key')


class PineconeSpecification(BaseModel):

    connection_specification: ConnectionSpecificationModel = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )
