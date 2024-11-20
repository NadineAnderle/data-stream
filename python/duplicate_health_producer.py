from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_health_data():
    # Generate a large string to increase the size of the data
    large_data = "x" * (50)  # 50MB of 'x'
    return {
        "patient_id": str(uuid.uuid4()),
        "heart_rate": random.randint(60, 100),
        "temperature": round(random.uniform(36, 40), 1),
        "oxygen_saturation": random.randint(95, 100),
        "timestamp": int(time.time() * 1000),
        "large_data": large_data
    }

def should_duplicate():
    """Randomly decide if message should be duplicated"""
    return random.random() < 0.3  # 30% chance of duplication

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def create_producer():
    # Using kafka:9093 for internal Docker network communication
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer,
        acks='all',
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        max_block_ms=30000,
        buffer_memory=33554432,
        compression_type='gzip',
        max_in_flight_requests_per_connection=1
    )

def send_data(producer, topic, health_data):
    try:
        future = producer.send(
            topic, 
            value=health_data,
            key=health_data['patient_id'].encode('utf-8')
        )
        
        record_metadata = future.get(timeout=10)
        
        #logger.info(
        #    f"\nMessage successfully persisted:"
       #     f"\n→ Topic: {record_metadata.topic}"
        #    f"\n→ Partition: {record_metadata.partition}"
         #   f"\n→ Offset: {record_metadata.offset}"
         #   f"\n→ Timestamp: {datetime.fromtimestamp(record_metadata.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}"
         #   f"\n→ Data: {json.dumps(health_data, indent=2)}"
         #   f"\n{'-'*50}"
        #)
        
        producer.flush()
        
    except Exception as e:
        logger.error(f"Error sending health data: {e}")

def main():
    while True:
        try:
            producer = create_producer()
            topic = "health-data"
            
            logger.info("Starting health data producer with duplication...")
            
            while True:
                try:
                    # Generate health data
                    health_data = create_health_data()
                    logger.info(f"Preparing to send health data...")
                    
                    # Send original data
                    send_data(producer, topic, health_data)
                    
                    # Decide if we should duplicate this message
                    if should_duplicate():
                        logger.info("Duplicating message...")
                        # Small delay between duplicates (50-200ms)
                        time.sleep(random.uniform(0.05, 0.2))
                        # Send the exact same data again
                        send_data(producer, topic, health_data)
                    
                    #time.sleep(1)  # Generate data every second
                    
                except Exception as e:
                    logger.error(f"Error producing health data: {e}")
                    time.sleep(5)
                    
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
