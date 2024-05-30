from verified_destinations.milvus.destination import Milvus
# from verified_destinations.milvus.catalog import MilvusCatalog
from verified_destinations.milvus.specs import MilvusSpecification
from dat_core.pydantic_models import DatConnectionStatus, DatDocumentStream

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
