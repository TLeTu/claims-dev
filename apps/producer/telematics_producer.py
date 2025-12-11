import sys
import os
import json
import time
import glob
import pandas as pd
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'telematics_topic'
SOURCE_PATH = "/opt/spark/raw_data/telematics/"
DELAY_SECONDS = 1

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Fatal: Could not connect to Kafka at {KAFKA_BROKER}. {e}")
        sys.exit(1)

def get_source_files(path):
    files = glob.glob(os.path.join(path, "*.parquet"))
    if not files:
        print(f"Warning: No .parquet files found in {path}")
    return files

def run_producer_loop(producer, topic):
    print(f"Starting continuous stream to {topic}...")
    
    try:
        while True:
            files = get_source_files(SOURCE_PATH)
            for filepath in files:
                try:
                    df = pd.read_parquet(filepath)
                    for _, row in df.iterrows():
                        # Convert to dict and ensure all values are strings
                        record = {k: str(v) for k, v in row.to_dict().items()}
                        key = record.get('chassis_no', 'default').encode('utf-8')
                        
                        producer.send(topic, value=record, key=key)
                        print(f"Sent: {key.decode('utf-8')}")
                        time.sleep(DELAY_SECONDS)
                        
                except Exception as e:
                    print(f"Error processing file {filepath}: {e}")
            
            print("Cycle complete. Restarting in 10s...")
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    if not os.path.exists(SOURCE_PATH):
        print(f"Error: Source path does not exist: {SOURCE_PATH}")
        sys.exit(1)
        
    producer = get_producer()
    run_producer_loop(producer, KAFKA_TOPIC)