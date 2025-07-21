import os
import uuid
import random
import json
import time
from datetime import datetime
from faker import Faker

fake = Faker()

EVENT_TYPES = ["page_view", "click", "scroll", "add_to_cart", "purchase"]
PAGE_URLS = ["/", "/home", "/products", "/about", "/contact", "/cart", "/checkout"]
COUNTRIES = ["US", "IN", "DE", "FR", "JP", "BR", "AU"]

# Set to True to upload to S3, False to save locally
USE_S3 = False
S3_BUCKET = "your-bucket-name"
S3_PREFIX = "clickstream_data/"

OUTPUT_FOLDER = "data/raw/"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def generate_event():
    timestamp = datetime.utcnow()
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 3),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": timestamp.isoformat(),
        "event_date": timestamp.strftime("%Y-%m-%d"),
        "session_id": str(uuid.uuid4())[:8],
        "page_url": random.choice(PAGE_URLS),
        "country": random.choice(COUNTRIES),
    }

def write_batch_to_local(events, batch_id):
    output_path = os.path.join(OUTPUT_FOLDER, f"clickstream_batch_{batch_id}.json")
    with open(output_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    print(f"✅ Written {len(events)} events to {output_path}")

def generate_streaming_data(total_batches=10, batch_size=100, interval_seconds=5):
    for batch_id in range(total_batches):
        events = [generate_event() for _ in range(batch_size)]
        write_batch_to_local(events, batch_id)
        time.sleep(interval_seconds)

if __name__ == "__main__":
    generate_streaming_data(total_batches=5, batch_size=3, interval_seconds=5)
