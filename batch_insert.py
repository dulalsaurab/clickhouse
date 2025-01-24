import json
import clickhouse_connect
from config import *
import argparse
from concurrent.futures import ThreadPoolExecutor

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
)

def read_json_in_batches(file_path, batch_size = BATCH_SIZE):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)  # Load the entire JSON array into memory
            for b in range(0, len(data), batch_size):
                yield data[b:b + batch_size]
    except Exception as e:
        print(e)

def transform_to_clickhouse_format(batch):
    return [
        (row["bucket"], row["hostname"], row["clientip"], row["vtc"], row["cid"], row["path"], row["graphqlop"], row["headerorder"], row["cipherorder"], row["timestamp"] )  # Adjust field mapping
        for row in batch
    ]

def insert_batch(batch):
    client = get_client()
    try:
        transformed_data = transform_to_clickhouse_format(batch)        
        client.insert(CH_TABLE, transformed_data)
        return len(transformed_data)
    finally:
        client.close()

def main(json_file):
    with ThreadPoolExecutor (max_workers=4) as executor:
        futures = []
        for batch in read_json_in_batches(json_file):
            futures.append(executor.submit(insert_batch, batch))
        
        total_inserted = sum(f.result() for f in futures)
        print(f"Inserted {total_inserted} rows into {CH_TABLE}")

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True, type=str, help="json file to upload")
    args = parser.parse_args()

    main(args.file)
    