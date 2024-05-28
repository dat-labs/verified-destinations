import weaviate as weaviate_client
from weaviate.exceptions import UnexpectedStatusCodeException
import json
import requests

client = weaviate_client.Client(
    url = "http://localhost:8080",
)
class_name = "Pytest_csv"
class_obj = {
    "class": class_name,
}
# try:
#     client.schema.create_class(class_obj)
# except UnexpectedStatusCodeException:
#     pass
client.schema.delete_class(class_name)

resp = requests.get('https://raw.githubusercontent.com/weaviate-tutorials/quickstart/main/data/jeopardy_tiny.json')
data = json.loads(resp.text)  # Load data
vectors = [
    [0.25 + i/100] * 1536 for i in range(len(data))
]
client.batch.configure(batch_size=100)  # Configure batch
with client.batch as batch:  # Initialize a batch process
    for i, data_obj in enumerate(data):  # Batch import data
        batch.add_data_object(
            data_object=data_obj,
            class_name=class_name,
            vector=vectors[i]
        )