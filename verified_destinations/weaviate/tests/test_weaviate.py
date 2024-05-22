import yaml
import os
from typing import List
from dat_core.pydantic_models import (
    DatMessage, DatDocumentMessage,
    Data, DatStateMessage,
    StreamState, StreamStatus,
    DatDocumentStream, Type,
    DatCatalog, Status
)
from verified_destinations.weaviate.destination import Weaviate


class TestWeaviate:

    def test_spec(self, ):
        """
        GIVEN None
        WHEN spec() is called on a valid Destination class
        THEN spec stated in ./specs/ConnectorSpecification.yml is returned
        """
        spec = Weaviate().spec()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.join(current_dir, "..", "specs.yml")
        with open(yaml_path) as yaml_in:
            schema = yaml.safe_load(yaml_in)
            assert schema == spec

    def test_check(self, config):
        """
        GIVEN a valid connectionSpecification JSON config
        WHEN check() is called on a valid Destination class
        THEN no error is raised
        """
        check = Weaviate().check(
            config=config)
        print(check)
        assert check.status == Status.SUCCEEDED
