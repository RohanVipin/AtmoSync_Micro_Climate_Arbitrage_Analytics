import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter

random.seed(42)

TOTAL_RECORDS = 1000

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_FILE = OUTPUT_DIR / "sensor_events.jsonl"

COMMODITIES = {
    "strawberries": (0, 2, 90, 95, 0.8),
    "vaccines": (-25, -15, 0, 60, 0.4),
    "salmon": (-1, 1.5, 85, 95, 0.6),
    "avocados": (5, 10, 85, 90, 1.2),
    "dairy": (1, 4, 70, 85, 1.0),
    "grapes": (-0.5, 1, 90, 95, 1.0),
    "biologics": (2, 8, 30, 60, 0.3)
}

def generate_event(index, commodity, timestamp):
    tmin, tmax, hmin, hmax, vmax = COMMODITIES[commodity]

    reading_type = random.choices(
        ["normal", "temperature", "humidity", "vibration"],
        weights=[80, 10, 5, 5],
        k=1
    )[0]

    temperature = random.uniform(tmin, tmax)
    humidity = random.uniform(hmin, hmax)
    vibration = random.uniform(0.05, vmax * 0.75)

    if reading_type == "temperature":
        temperature = random.choice([
            tmin - random.uniform(1, 8),
            tmax + random.uniform(1, 8)
        ])
    elif reading_type == "humidity":
        humidity = random.choice([
            max(0, hmin - random.uniform(5, 20)),
            min(100, hmax + random.uniform(5, 20))
        ])
    elif reading_type == "vibration":
        vibration = vmax * random.uniform(1.2, 3)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": timestamp.isoformat(),
        "container_id": f"CONT-{(index % 10) + 1:03d}",
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "vibration": round(vibration, 3),
        "latitude": round(random.uniform(10, 30), 6),
        "longitude": round(random.uniform(68, 88), 6),
        "commodity": commodity,
        "quantity_kg": round(random.uniform(100, 1000), 2)
    }

    return event, reading_type

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    commodities = list(COMMODITIES)
    counts = Counter()
    types = Counter()
    now = datetime.now(timezone.utc)

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        for i in range(TOTAL_RECORDS):
            commodity = commodities[i % len(commodities)]
            timestamp = now - timedelta(minutes=TOTAL_RECORDS - 1 - i)

            event, reading_type = generate_event(i, commodity, timestamp)
            file.write(json.dumps(event) + "\n")

            counts[commodity] += 1
            types[reading_type] += 1

    print("AtmoSync data generation successful!")
    print("Total records:", sum(counts.values()))
    print("Output:", OUTPUT_FILE)
    print("Commodity distribution:", dict(counts))
    print("Reading distribution:", dict(types))

if __name__ == "__main__":
    main()
