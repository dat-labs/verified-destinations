# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-05-28T10:30:47+00:00

from __future__ import annotations

from typing import Optional, Union, Literal

from pydantic import BaseModel, Field
from dat_core.pydantic_models import ConnectionSpecification


class NoAuthentication(BaseModel):
    authentication: str = Field(
        'no_authentication',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )

    class Config:
        extra = "allow"


class BasicAuthentication(BaseModel):
    authentication: str = Field(
        'basic_authentication',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )
    username: str = Field(
        ..., description='Username for the Weaviate cluster', title='Username'
    )
    password: str = Field(
        ..., description='Password for the Weaviate cluster', title='Password'
    )


class APIKeyAuthentication(BaseModel):
    authentication: str = Field(
        'api_key_authentication',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )
    api_key: str = Field(
        ..., description='API Key of the Weaviate cluster', title='API key'
    )


class ConnectionSpecificationModel(ConnectionSpecification):
    cluster_url: str = Field(
        ..., description='URL of the Weaviate cluster', title='Weaviate cluster URL'
    )
    authentication: Union[
        NoAuthentication,
        BasicAuthentication,
        APIKeyAuthentication,
    ] = Field(...,
              description='Authentication method to use',
              title='Authentication',
              json_schema_extra={
                  'ui-opts': {
                          'widget': 'singleDropdown',
                  }
              }
              )


class WeaviateSpecification(BaseModel):
    name: Literal['Weaviate']
    module_name: Literal['weaviate']
    documentation_url: Optional[str] = (
        'https://datlabs.gitbook.io/datlabs/integrations/destinations/weaviate'
    )
    connection_specification: ConnectionSpecificationModel = Field(
        ...,
        description='connection_specification specific blob. Must be a valid JSON string.',
    )
