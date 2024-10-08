from pymilvus import MilvusClient, connections, db
from dat_core.loggers import logger

# client = connections.connect(host="127.0.0.1", port=19530)

def _create_client():
    return MilvusClient(
        uri="http://localhost:19530",
    )

collection_name = "pytest_collection"

def load_data():
    client = _create_client()
    client.create_collection(collection_name, dimension=5, auto_id=True, enable_dynamic_field=True)
    data=[
    {"id": 0, "vector": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592], "color": "pink_8682"},
    {"id": 1, "vector": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104], "color": "red_7025"},
    {"id": 2, "vector": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592], "color": "orange_6781"},
    {"id": 3, "vector": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345], "color": "pink_9298"},
    {"id": 4, "vector": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106], "color": "red_4794"},
    {"id": 5, "vector": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955], "color": "yellow_4222"},
    {"id": 6, "vector": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987], "color": "red_9392"},
    {"id": 7, "vector": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052], "color": "grey_8510"},
    {"id": 8, "vector": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336], "color": "white_9381"},
    {"id": 9, "vector": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608], "color": "purple_4976"}
    ]
    data_with_uuid_id_and_nore_random_dynamic_fields = [
        { "vector": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592], "color": "pink_8682", "random_field_1": "random_value_1", "random_field_2": "random_value_2"},
        {"vector": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104], "color": "red_7025", "random_field_1": "random_value_1", "random_field_2": "random_value_2"},
        {"vector": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592], "color": "orange_6781", "random_field_1": "random_value_1", "random_field_2": "random_value_2"},
    ]

    res = client.insert(
        collection_name=collection_name,
        data=data_with_uuid_id_and_nore_random_dynamic_fields
    )

    logger.debug(res)


def check():
    client = _create_client()
    if not client.has_collection(collection_name):
        return False
    res = client.describe_collection(
        collection_name=collection_name
    )

    logger.debug(res)

def delete():
    client = _create_client()
    client.drop_collection(collection_name=collection_name)

    logger.debug("Collection dropped")

def _search_by_dynamic_fields_and_print_result():
    client = _create_client()
    # _filter = 'color in ["red_7025", "red_4794"] or random_field_1 == "random_value_1" and random_field_2 == "random_value_2"'
    _filter = 'stream == "actor_instances"'
    print(f"Searching for {_filter}")
    res = client.query(
        collection_name=collection_name,
        filter=_filter,
        # partition_names=["pytest_pdf", "pytest_csv"]
    )
    logger.debug(len(res))
    # import pdb;pdb.set_trace()
    for r in res:
        logger.debug(f"ID: {r.get('id')}, Meta: {r.get('meta')}")

def drop_partition():
    client = _create_client()
    client.drop_partition(collection_name="collection_name", partition_name="pytest_csv1")

    logger.debug("Partition dropped")


if __name__ == "__main__":
    # print(check())
    # load_data()
    # delete()
    _search_by_dynamic_fields_and_print_result()
    # drop_partition()