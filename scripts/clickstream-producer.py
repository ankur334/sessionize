#!/usr/bin/env python3
"""
Clickstream Data Producer for User Sessionization Testing

Generates realistic clickstream events to Kafka topic for testing the sessionization pipeline.
Based on the provided sample data format with realistic user behavior patterns.

Usage:
    python scripts/clickstream-producer.py --num-users 10 --rate 5
    python scripts/clickstream-producer.py --help
"""

import json
import time
import random
import argparse
from datetime import datetime, timedelta
from uuid import uuid4
from typing import List, Dict
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from kafka import KafkaProducer
except ImportError:
    print("kafka-python is required. Install it with: pip install kafka-python")
    sys.exit(1)


class ClickstreamProducer:
    """Generates realistic clickstream events for sessionization testing."""
    
    def __init__(self, kafka_servers="localhost:9092", topic="clickstream-events"):
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.producer = None
        
        # User behavior patterns
        self.pages = ["home", "selection", "review", "booking", "payment", "thankyou", "profile", "search"]
        self.event_types = ["user-action", "system-action"]
        self.event_names = [
            "page-view", "user-selection", "user-review", "user-booked", 
            "user-payment", "user-exited", "user-clicked", "user-searched"
        ]
        
        # Session patterns
        self.session_duration_minutes = [5, 10, 15, 30, 45, 60, 90, 120]  # Various session lengths
        self.inactivity_gaps_minutes = [1, 2, 5, 10, 15, 35, 45]  # Including gaps that trigger new sessions (35, 45 > 30min)
        
    def connect(self):
        """Connect to Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10
            )
            print(f"✅ Connected to Kafka at {self.kafka_servers}")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            sys.exit(1)
    
    def generate_user_events(self, user_id: str, num_sessions: int = None) -> List[Dict]:
        """Generate realistic clickstream events for a single user."""
        events = []
        current_time = int(time.time() * 1000)  # Current time in milliseconds
        
        # Generate 1-3 sessions per user if not specified
        if num_sessions is None:
            num_sessions = random.randint(1, 3)
        
        for session_num in range(num_sessions):
            session_events = self._generate_session_events(user_id, current_time)
            events.extend(session_events)
            
            # Add gap between sessions (some gaps will trigger new sessions)
            if session_num < num_sessions - 1:  # Not the last session
                gap_minutes = random.choice(self.inactivity_gaps_minutes)
                current_time += gap_minutes * 60 * 1000  # Convert to milliseconds
        
        return events
    
    def _generate_session_events(self, user_id: str, start_time_ms: int) -> List[Dict]:
        """Generate events for a single session."""
        events = []
        current_time = start_time_ms
        
        # Random session duration
        session_duration_ms = random.choice(self.session_duration_minutes) * 60 * 1000
        end_time = current_time + session_duration_ms
        
        # Generate 5-20 events per session
        num_events = random.randint(5, 20)
        
        # Create a realistic user journey
        user_journey = self._create_user_journey(num_events)
        
        for i, page in enumerate(user_journey):
            if current_time >= end_time:
                break
                
            event = self._create_event(user_id, page, current_time, i == len(user_journey) - 1)
            events.append(event)
            
            # Add realistic time gap between events (1-5 minutes)
            gap_seconds = random.randint(30, 300)  # 30 seconds to 5 minutes
            current_time += gap_seconds * 1000
        
        return events
    
    def _create_user_journey(self, num_events: int) -> List[str]:
        """Create a realistic user journey through pages."""
        # Common user journey patterns
        journey_patterns = [
            ["home", "selection", "review", "booking", "payment", "thankyou"],  # Complete purchase
            ["home", "search", "selection", "review", "selection", "review"],  # Browsing
            ["home", "profile", "booking", "payment", "thankyou"],  # Quick rebooking
            ["home", "selection", "home", "selection", "review"],  # Indecisive user
            ["search", "selection", "review", "booking"],  # Search-driven
        ]
        
        base_journey = random.choice(journey_patterns)
        
        # Extend or trim to match num_events
        if len(base_journey) < num_events:
            # Add more browsing events
            extra_pages = random.choices(["selection", "review", "search"], k=num_events - len(base_journey))
            base_journey.extend(extra_pages)
        elif len(base_journey) > num_events:
            base_journey = base_journey[:num_events]
        
        return base_journey
    
    def _create_event(self, user_id: str, page: str, timestamp_ms: int, is_exit: bool = False) -> Dict:
        """Create a single clickstream event."""
        event_id = str(uuid4())
        
        # Determine event details based on page and context
        if page == "booking" and not is_exit:
            event_name = "user-booked"
            event_value = f"booking-{random.randint(1000, 9999)}"
            booking_details = {"id": f"ZC{random.randint(10000000, 99999999)}"}
        elif page == "selection":
            event_name = "user-selection"
            event_value = f"card-{random.randint(1, 10)}-selected"
            booking_details = ""
        elif page == "review":
            event_name = "user-review"
            event_value = f"card-{random.randint(1, 10)}-reviewed"
            booking_details = {}
        elif is_exit or page == "thankyou":
            event_name = "user-exited"
            event_value = ""
            booking_details = {}
        else:
            event_name = "page-view"
            event_value = f"{page}-viewed"
            booking_details = ""
        
        event_type = "system-action" if is_exit else "user-action"
        
        event = {
            "event_id": event_id,
            "page_name": page,
            "event_timestamp": str(timestamp_ms),
            "booking_details": booking_details,
            "uuid": user_id,
            "event_details": {
                "event_name": event_name,
                "event_type": event_type,
                "event_value": event_value
            }
        }
        
        return event
    
    def produce_events(self, num_users: int, events_per_second: float, duration_minutes: int = None):
        """Produce clickstream events to Kafka topic."""
        if not self.producer:
            self.connect()
        
        print(f"🚀 Starting clickstream data production")
        print(f"   📊 Users: {num_users}")
        print(f"   ⚡ Rate: {events_per_second} events/second")
        print(f"   📍 Topic: {self.topic}")
        
        if duration_minutes:
            print(f"   ⏱️  Duration: {duration_minutes} minutes")
            end_time = time.time() + (duration_minutes * 60)
        else:
            print(f"   ⏱️  Duration: Infinite (use Ctrl+C to stop)")
            end_time = None
        
        # Generate user IDs
        user_ids = [f"user-{i:04d}-{uuid4().hex[:8]}" for i in range(num_users)]
        
        events_sent = 0
        try:
            while True:
                if end_time and time.time() >= end_time:
                    break
                
                # Select a random user
                user_id = random.choice(user_ids)
                
                # Generate events for this user (usually 1 event, sometimes a burst)
                if random.random() < 0.8:  # 80% single events
                    events = self.generate_user_events(user_id, num_sessions=1)[:1]
                else:  # 20% burst of events (user active session)
                    events = self.generate_user_events(user_id, num_sessions=1)[:random.randint(2, 5)]
                
                # Send events
                for event in events:
                    try:
                        future = self.producer.send(
                            self.topic,
                            value=event,
                            key=event['uuid']
                        )
                        # Optional: wait for delivery
                        # future.get(timeout=10)
                        
                        events_sent += 1
                        
                        # Print progress
                        if events_sent % 100 == 0:
                            print(f"📤 Sent {events_sent} events...")
                            
                    except Exception as e:
                        print(f"❌ Failed to send event: {e}")
                
                # Rate limiting
                time.sleep(1.0 / events_per_second)
        
        except KeyboardInterrupt:
            print(f"\n⏹️  Stopped by user after sending {events_sent} events")
        except Exception as e:
            print(f"❌ Production error: {e}")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                print(f"✅ Total events sent: {events_sent}")


def main():
    parser = argparse.ArgumentParser(description="Clickstream Data Producer for Sessionization Testing")
    
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", default="clickstream-events",
                       help="Kafka topic name (default: clickstream-events)")
    parser.add_argument("--num-users", type=int, default=5,
                       help="Number of unique users to simulate (default: 5)")
    parser.add_argument("--rate", type=float, default=2.0,
                       help="Events per second (default: 2.0)")
    parser.add_argument("--duration", type=int, default=None,
                       help="Duration in minutes (default: infinite)")
    parser.add_argument("--sample", action="store_true",
                       help="Generate and print sample events without sending to Kafka")
    
    args = parser.parse_args()
    
    producer = ClickstreamProducer(args.kafka_servers, args.topic)
    
    if args.sample:
        print("📋 Sample clickstream events:")
        print("=" * 50)
        sample_events = producer.generate_user_events("sample-user-123", num_sessions=2)
        for i, event in enumerate(sample_events[:5]):  # Show first 5 events
            print(f"{i+1}. {json.dumps(event, indent=2)}")
        print(f"... (showing 5 of {len(sample_events)} events)")
    else:
        producer.produce_events(args.num_users, args.rate, args.duration)


if __name__ == "__main__":
    main()