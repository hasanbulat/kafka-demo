import json
import math
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer


SAMPLES_PER_PERIOD = 64
AMPLITUDE = 1
ANGULAR_FREQUENCY = 2
VERTICAL_OFFSET = 1


def build_message(point, publisher_id, publisher_started_at):
    """Keep x/y numeric; encode UTC datetimes, fixed-point decimals and UUIDs as strings."""
    return {
        **point,
        "message_id": str(uuid4()),
        "publisher_id": str(publisher_id),
        "publisher_started_at": publisher_started_at.isoformat(),
        "published_at": datetime.now(timezone.utc).isoformat(),
        "x_decimal": f"{point['x']:.6f}",
        "y_decimal": f"{point['y']:.6f}",
        "amplitude": f"{AMPLITUDE:.6f}",
        "angular_frequency": f"{ANGULAR_FREQUENCY:.6f}",
        "vertical_offset": f"{VERTICAL_OFFSET:.6f}",
        "source": "sine-publisher",
        "signal": "sine",
        "formula": "sin(2*x) + 1",
        "x_unit": "radians",
        "y_unit": "unitless",
        "schema_version": "1",
    }


def main():
    topic = os.getenv("KAFKA_TOPIC", "sine_graph")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    interval_ms = int(os.getenv("PUBLISH_INTERVAL_MS", "100"))
    if interval_ms < 0:
        raise ValueError("PUBLISH_INTERVAL_MS must be 0 or greater")
    publisher_id = uuid4()
    publisher_started_at = datetime.now(timezone.utc)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda point: json.dumps(point).encode("utf-8"),
    )
    print(
        f"Sending y = sin(2*x) + 1 points to {topic} at {bootstrap_servers} "
        f"every {interval_ms} ms..."
    )

    sample_index = 0
    try:
        while True:
            x = math.tau * (sample_index % SAMPLES_PER_PERIOD) / SAMPLES_PER_PERIOD
            y = AMPLITUDE * math.sin(ANGULAR_FREQUENCY * x) + VERTICAL_OFFSET
            point = {"x": round(x, 6), "y": round(y, 6)}
            message = build_message(point, publisher_id, publisher_started_at)
            metadata = producer.send(topic, message).get(timeout=10)
            print(
                f"Sent to {metadata.topic}[{metadata.partition}]@{metadata.offset}: "
                f"{json.dumps(point)}",
                flush=True,
            )
            sample_index += 1
            time.sleep(interval_ms / 1000)
    except KeyboardInterrupt:
        print("\nStopping sine publisher.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
