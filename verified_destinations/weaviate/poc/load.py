import weaviate as weaviate_client
import json
import requests

client = weaviate_client.Client(
    url = "http://localhost:8080",
)

class_obj = {
    "class": "Question",
    "vectorizer": "text2vec-openai",
}

client.schema.create_class(class_obj)

resp = requests.get('https://raw.githubusercontent.com/weaviate-tutorials/quickstart/main/data/jeopardy_tiny.json')
data = json.loads(resp.text)  # Load data
# import pdb;pdb.set_trace()

client.batch.configure(batch_size=100)  # Configure batch
with client.batch as batch:  # Initialize a batch process
    for i, d in enumerate(data):  # Batch import data
        print(f"importing question: {i+1}")
        properties = {
            "answer": d["Answer"],
            "question": d["Question"],
            "category": d["Category"],
        }
        batch.add_data_object(
            data_object=properties,
            class_name="Question"
        )