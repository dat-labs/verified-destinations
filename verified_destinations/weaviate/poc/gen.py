import weaviate as weaviate_client
import json

client = weaviate_client.Client(
    url = "http://localhost:8080",
)

class_name = "Question"

response = client.schema.get(class_name)

logger.debug(json.dumps(response, indent=2))