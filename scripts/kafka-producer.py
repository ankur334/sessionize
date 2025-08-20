#!/usr/bin/env python3
"""
Kafka Test Producer for Sessionize

This script produces sample JSON events to Kafka topics for testing
the Sessionize pipeline components.
"""

import json
import time
import random
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SessionizeKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer with JSON serialization."""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            retry_backoff_ms=1000
        )
        logger.info(f"Connected to Kafka at {bootstrap_servers}")
    
    def generate_user_event(self, event_id):
        """Generate a realistic user event."""
        event_types = ['click', 'view', 'purchase', 'search', 'add_to_cart', 'checkout']
        users = ['user_1', 'user_2', 'user_3', 'user_4', 'user_5', 'user_6', 'user_7', 'user_8', 'user_9', 'user_10']
        pages = [
            'https://example.com/home',
            'https://example.com/products',
            'https://example.com/product/123',
            'https://example.com/cart',
            'https://example.com/checkout',
            'https://example.com/profile',
            'https://example.com/search',
            'https://example.com/category/electronics',
            'https://example.com/category/books',
            'https://example.com/about'
        ]
        actions = ['button_click', 'scroll', 'form_submit', 'link_click', 'page_view']
        
        user_id = random.choice(users)
        session_id = f"session_{random.randint(1, 5)}"
        
        event = {
            'event_id': f'evt_{event_id:06d}',
            'event_type': random.choice(event_types),
            'user_id': user_id,
            'timestamp': datetime.now().isoformat() + 'Z',
            'session_id': session_id,
            'page_url': random.choice(pages),
            'action': random.choice(actions),
            'duration_ms': random.randint(100, 5000),
            'properties': {
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
                'country': random.choice(['US', 'UK', 'CA', 'DE', 'FR']),
                'user_agent': 'SessionizeTestAgent/1.0'
            }
        }
        
        return event
    
    def generate_clickstream_event(self, event_id):
        """Generate clickstream events for session analysis."""
        users = [f'user_{i}' for i in range(1, 21)]
        sessions = [f'session_{i}' for i in range(1, 11)]
        pages = [f'page_{i}' for i in range(1, 51)]
        
        event = {
            'user_id': random.choice(users),
            'session_id': random.choice(sessions),
            'timestamp': datetime.now().isoformat() + 'Z',
            'page': random.choice(pages),
            'action': random.choice(['view', 'click', 'scroll', 'hover']),
            'duration_seconds': random.randint(1, 300),
            'event_id': f'click_{event_id:06d}'
        }
        
        return event
    
    def produce_events(self, topic, num_events, event_type='user_event', rate=1.0):
        """Produce events to a Kafka topic."""
        logger.info(f"Producing {num_events} events to topic '{topic}' at {rate} events/second")
        
        sent_count = 0
        failed_count = 0
        
        try:
            for i in range(num_events):
                try:
                    # Generate event based on type
                    if event_type == 'clickstream':
                        event = self.generate_clickstream_event(i + 1)
                    else:
                        event = self.generate_user_event(i + 1)
                    
                    # Send to Kafka
                    future = self.producer.send(
                        topic,
                        key=event.get('user_id'),
                        value=event
                    )
                    
                    # Add callback for success/failure
                    future.add_callback(lambda metadata: self._on_send_success(metadata))
                    future.add_errback(lambda exception: self._on_send_error(exception))
                    
                    sent_count += 1
                    
                    # Rate limiting
                    if rate > 0:
                        time.sleep(1.0 / rate)
                    
                    # Progress update
                    if sent_count % 100 == 0:
                        logger.info(f"Sent {sent_count}/{num_events} events")
                        
                except Exception as e:
                    logger.error(f"Failed to send event {i}: {e}")
                    failed_count += 1
            
            # Flush remaining messages
            self.producer.flush()
            
            logger.info(f"✅ Completed: {sent_count} sent, {failed_count} failed")
            
        except KeyboardInterrupt:
            logger.info("🛑 Interrupted by user")
        finally:
            self.producer.close()
    
    def _on_send_success(self, metadata):
        """Callback for successful message send."""
        logger.debug(f"Message sent to {metadata.topic}[{metadata.partition}] at offset {metadata.offset}")
    
    def _on_send_error(self, exception):
        """Callback for failed message send."""
        logger.error(f"Failed to send message: {exception}")


def main():
    parser = argparse.ArgumentParser(description="Kafka Test Producer for Sessionize")
    parser.add_argument('--topic', default='events-topic', help='Kafka topic to produce to')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--num-events', type=int, default=100, help='Number of events to produce')
    parser.add_argument('--rate', type=float, default=5.0, help='Events per second')
    parser.add_argument('--event-type', choices=['user_event', 'clickstream'], default='user_event',
                       help='Type of events to generate')
    parser.add_argument('--continuous', action='store_true', help='Run continuously until interrupted')
    
    args = parser.parse_args()
    
    try:
        producer = SessionizeKafkaProducer(args.bootstrap_servers)
        
        if args.continuous:
            logger.info("🔄 Running in continuous mode. Press Ctrl+C to stop.")
            while True:
                producer.produce_events(args.topic, 50, args.event_type, args.rate)
                time.sleep(1)
        else:
            producer.produce_events(args.topic, args.num_events, args.event_type, args.rate)
            
    except KeyboardInterrupt:
        logger.info("👋 Shutting down producer")
    except Exception as e:
        logger.error(f"❌ Producer failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())