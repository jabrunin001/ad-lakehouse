# generator/produce.py
import argparse
import json
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

from generator.stream import event_batch


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10_000, help="number of ad requests")
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--late-rate", type=float, default=0.05)
    ap.add_argument("--fill-prob", type=float, default=0.7,
                    help="fraction of ad requests that produce an impression (0-1)")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    topic = os.environ.get("KAFKA_TOPIC", "ad_events")
    producer = Producer({"bootstrap.servers": bootstrap})

    now = datetime.now(timezone.utc)
    count = 0
    for ev in event_batch(args.n, now, args.dup_rate, args.late_rate, args.seed, args.fill_prob):
        payload = ev.model_dump()
        payload["event_ts"] = payload["event_ts"].isoformat()
        producer.produce(topic, key=ev.user_id, value=json.dumps(payload))
        count += 1
        if count % 2000 == 0:
            producer.poll(0)
    producer.flush()
    print(f"produced {count} events from {args.n} requests to {topic}")


if __name__ == "__main__":
    main()
