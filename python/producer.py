from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime
from faker import Faker
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

def create_health_data():
    return {
        "patient_id": str(uuid.uuid4()),
        "heart_rate": random.randint(60, 100),
        "temperature": round(random.uniform(36, 40), 1),
        "oxygen_saturation": random.randint(95, 100),
        "timestamp": int(time.time() * 1000)
    }

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def create_producer():
    # Added robust configuration
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer,
        acks='all',  # Wait for all replicas
        retries=5,   # Retry up to 5 times
        retry_backoff_ms=1000,  # Wait 1 second between retries
        request_timeout_ms=30000,  # 30 second timeout
        max_block_ms=30000,  # 30 second max block
        buffer_memory=33554432,  # 32MB buffer memory
        compression_type='gzip',  # Enable compression
        max_in_flight_requests_per_connection=1  # Ensure ordering
    )

def main():
    while True:  # Outer loop for connection retries
        try:
            producer = create_producer()
            topic = "health-data"
            
            logger.info("Starting health data producer...")
            
            while True:  # Inner loop for continuous data production
                try:
                    health_data = create_health_data()
                    logger.info(f"Preparing to send health data...")
                    
                    # Send data with explicit partition key (patient_id)
                    future = producer.send(
                        topic, 
                        value=health_data,
                        key=health_data['patient_id'].encode('utf-8')
                    )
                    
                    # Wait for the send to complete with a reasonable timeout
                    record_metadata = future.get(timeout=10)
                    
                    # Log detailed message persistence information
                    logger.info(
                        f"\nMessage successfully persisted:"
                        f"\n→ Topic: {record_metadata.topic}"
                        f"\n→ Partition: {record_metadata.partition}"
                        f"\n→ Offset: {record_metadata.offset}"
                        f"\n→ Timestamp: {datetime.fromtimestamp(record_metadata.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}"
                        f"\n→ Data: {json.dumps(health_data, indent=2)}"
                        f"\n{'-'*50}"
                    )
                    
                    # Flush to ensure all messages are sent
                    producer.flush()
                    
                    time.sleep(1)  # Generate data every second
                    
                except Exception as e:
                    logger.error(f"Error producing health data: {e}")
                    # Don't break on transient errors, let it retry
                    time.sleep(5)  # Wait 5 seconds before retrying
                    
        except Exception as e:
            logger.error(f"Critical error in producer: {e}")
            if producer:
                producer.close()
            logger.info("Waiting 10 seconds before attempting to reconnect...")
            time.sleep(10)
        finally:
            if producer:
                producer.close()

if __name__ == "__main__":
    main()
