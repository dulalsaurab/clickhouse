import json
import clickhouse_connect
from config import *
import argparse
from concurrent.futures import ThreadPoolExecutor
import time
import cProfile
import logging
from datetime import datetime
import os

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class Timer:
    def __init__(self, name):
        self.name = name
    
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        logger.info(f"{self.name} took {self.elapsed:.2f} seconds")

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
        with Timer("Reading JSON file"):
            with open(file_path, "r") as f:
                data = json.load(f)
                for b in range(0, len(data), batch_size):
                    yield data[b:b + batch_size]
    except Exception as e:
        logger.error(f"Error reading JSON: {e}")
        raise

def transform_to_clickhouse_format(batch):
    with Timer("Transforming batch"):
        return [
            (row["bucket"], row["hostname"], row["clientip"], row["vtc"],
            row["cid"], row["path"], row["graphqlop"], row["headerorder"],
            row["cipherorder"], row["timestamp"])
            for row in batch
    ]

def insert_batch(batch, batch_num):
    client = get_client()
    try:
        with Timer(f"Processing batch {batch_num}"):
            transformed_data = transform_to_clickhouse_format(batch)   
            with Timer(f"DB insertion for batch {batch_num}"):     
                client.insert(CH_TABLE, transformed_data)
            return len(transformed_data)
    finally:
        client.close()

def main(json_file):
    start_time = datetime.now()
    logger.info(f"Starting import process at {start_time}")
    
    batch_times = []
    total_rows = 0

    with Timer("Total execution"):
        with ThreadPoolExecutor (max_workers=MAX_WORKERS) as executor:
            futures = []
            for batch_num, batch in enumerate(read_json_in_batches(json_file)):
                futures.append(executor.submit(insert_batch, batch, batch_num))
            
            for f in futures:
                try:
                    rows = f.result()
                    total_rows += rows
                except Exception as e:
                    logger.error(f"Future failed: {e}")
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"\nImport Summary:")
    logger.info(f"Total rows inserted: {total_rows}")
    logger.info(f"Total duration: {duration:.2f} seconds")
    logger.info(f"Average insertion rate: {total_rows/duration:.2f} rows/second")
    logger.info(f"Finished at: {end_time}")
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True, type=str, help="json file to upload")
    parser.add_argument("--profile", action="store_true", help="Enable detailed profiling")
    args = parser.parse_args()
    
    logger.info("Starting script execution")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Number of threads: {MAX_WORKERS}")
    
    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()
        main(args.file)
        profiler.disable()
        profiler.print_stats(sort='cumulative')
    else:
        main(args.file)
