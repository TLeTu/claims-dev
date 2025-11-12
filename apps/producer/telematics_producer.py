import pandas as pd
from kafka import KafkaProducer
import json
import time
import glob
import sys
import os

# --- CONFIGURATION ---
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'telematics_topic'
# Path to the folder containing your parquet files (relative to where you run this script)
PARQUET_FILE_PATH = "/opt/spark/raw_data/telematics/"
SIMULATION_SPEED_SECONDS = 1        # Seconds to wait between sending records
# ---------------------

def get_producer():
    """Initializes the KafkaProducer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"Error: Could not connect to Kafka broker at {KAFKA_BROKER}. Is it running?")
        print(f"Details: {e}")
        sys.exit(1)

def read_parquet_files(path):
    """
    Finds all .parquet files in the specified path and yields their DataFrames.
    """
    parquet_files = glob.glob(f"{path.rstrip('/')}/*.parquet")
    
    if not parquet_files:
        print(f"Error: No .parquet files found in directory: {path}")
        print("Please check the PARQUET_FILE_PATH variable.")
        sys.exit(1)
        
    print(f"Found {len(parquet_files)} parquet files to process:")
    for f in parquet_files:
        print(f"  - {f}")
        
    for filepath in parquet_files:
        try:
            yield pd.read_parquet(filepath)
        except Exception as e:
            print(f"Warning: Could not read file {filepath}. Skipping. Error: {e}")

def send_to_kafka(producer, topic):
    """
    Reads data from parquet files and sends it row-by-row to Kafka.
    """
    print(f"\nStarting to send data to Kafka topic: {topic}")
    print("Press Ctrl+C to stop the script.")
    
    record_count = 0
    
    # Loop indefinitely to simulate a continuous stream
    while True:
        print("\n--- Starting new loop through all files ---")
        for df in read_parquet_files(PARQUET_FILE_PATH):
            for _, row in df.iterrows():
                try:
                    record = row.to_dict()
                    
                    # Convert ALL values to strings to match the DLT schema
                    string_record = {k: str(v) for k, v in record.items()}
                    
                    # Use chassis_no as the partition key
                    partition_key = string_record.get('chassis_no', 'default_key').encode('utf-8')
                    
                    # Send the record to Kafka
                    producer.send(topic, value=string_record, key=partition_key)
                    
                    record_count += 1
                    print(f"Record {record_count} Sent (Key: {partition_key.decode('utf-8')})")
                    
                    time.sleep(SIMULATION_SPEED_SECONDS)
                    
                except KeyboardInterrupt:
                    print("\nScript stopped by user.")
                    return
                except Exception as e:
                    print(f"Error sending record: {e}")
                    time.sleep(5) # Wait before retrying

        print("--- Finished loop. Restarting in 10 seconds. ---")
        time.sleep(10)

if __name__ == "__main__":
    try:
        if not os.path.exists(PARQUET_FILE_PATH):
            print(f"Error: Path not found: {PARQUET_FILE_PATH}")
            print(f"Make sure you are running this script from your 'project_data' directory.")
            sys.exit(1)
            
        producer = get_producer()
        send_to_kafka(producer, KAFKA_TOPIC)
        
    except KeyboardInterrupt:
        print("\nSimulation stopped.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'producer' in locals() and producer:
            producer.flush()
            producer.close()
            print("Kafka producer closed.")