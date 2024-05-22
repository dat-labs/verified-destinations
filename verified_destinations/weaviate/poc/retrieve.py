import weaviate as weaviate_client
import json
import requests

client = weaviate_client.Client(
    url = "http://localhost:8080",
)

def get_batch_with_cursor(collection_name, batch_size, cursor=None):
    # First prepare the query to run through data
    query = (
        client.query.get(
            collection_name,         # update with your collection name
        )
        .with_additional(["id vector"])
        .with_limit(batch_size)
    )

    # Fetch the next set of results
    if cursor is not None:
        result = query.with_after(cursor).do()
    # Fetch the first set of results
    else:
        result = query.do()
    # import pdb;pdb.set_trace()

    return result["data"]["Get"][collection_name]

def print_batch(collection_name, batch, cursor=None):
    while True:
        # Get the next batch of objects
        next_batch = get_batch_with_cursor(collection_name, batch, cursor)

        # Break the loop if empty – we are done
        if len(next_batch) == 0:
            break

        # Here is your next batch of objects
        print(next_batch)

        # Move the cursor to the last returned uuid
        cursor=next_batch[-1]["_additional"]["id"]



if __name__ == "__main__":
    print_batch("Question", 100)
