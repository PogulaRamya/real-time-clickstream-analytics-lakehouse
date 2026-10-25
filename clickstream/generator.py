"""Deterministic synthetic journeys; no real customer data."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import random
import time
import uuid

KINDS = ("page_view", "product_view", "add_to_cart", "checkout", "purchase")

def generate(count, seed=42, start=None):
    rng = random.Random(seed)
    start = start or datetime.now(timezone.utc)
    emitted = 0
    journey = 0
    while emitted < count:
        user = f"visitor-{journey % 10000:05d}"
        product = f"SKU-{rng.randrange(1, 21):03d}"
        price = rng.randrange(1000, 20000)
        depth = rng.choices([1, 2, 3, 4, 5], [15, 30, 25, 15, 15])[0]
        # Journeys repeat for a visitor only after >30 minutes at large counts.
        base = start + timedelta(seconds=journey)
        for step, kind in enumerate(KINDS[:depth]):
            if emitted >= count:
                return
            yield {
                "schema_version": 1,
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{seed}:{start.isoformat()}:{journey}:{step}")),
                "event_time": (base + timedelta(seconds=step)).isoformat(),
                "user_id": user,
                "email": f"{user}@example.invalid",
                "event_type": kind,
                "product_id": product,
                "order_id": f"order-{seed}-{journey}" if kind == "purchase" else None,
                "quantity": 1 if kind == "purchase" else 0,
                "revenue_cents": price if kind == "purchase" else 0,
                "currency": "USD",
                "device": rng.choice(["mobile", "desktop", "tablet"]),
            }
            emitted += 1
        journey += 1


def put_batch(client, stream, events, retries=8):
    """Retry ONLY failed PutRecords entries; never silently lose exhausted records."""
    pending = [{"Data": (json.dumps(e) + "\n").encode(), "PartitionKey": e["user_id"]} for e in events]
    if len(pending) > 500:
        raise ValueError("PutRecords supports at most 500 records per request")
    for attempt in range(retries + 1):
        response = client.put_records(StreamName=stream, Records=pending)
        pending = [record for record, result in zip(pending, response["Records"])
                   if result.get("ErrorCode")]
        if not pending:
            return len(events)
        if attempt < retries:
            time.sleep(min(0.05 * 2 ** attempt, 5) * random.uniform(0.5, 1.5))
    raise RuntimeError(f"{len(pending)} records still failed after retries")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", default="build/events.jsonl")
    p.add_argument("--count", type=int, default=10000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--stream", help="Kinesis stream; omit for local JSONL")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--rate", type=int, default=5000, help="Target events/sec; measure actual achieved rate")
    p.add_argument("--seconds", type=int, default=60)
    a = p.parse_args()
    if min(a.count, a.rate, a.seconds) <= 0:
        p.error("count, rate, seconds must be positive")
    if not a.stream:
        from pathlib import Path
        target = Path(a.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w") as f:
            for e in generate(a.count, a.seed):
                f.write(json.dumps(e) + "\n")
        print(f"Wrote {a.count} events to {target}")
        return
    import boto3
    from botocore.config import Config
    client = boto3.client("kinesis", region_name=a.region,
                          config=Config(max_pool_connections=16, retries={"mode": "adaptive", "max_attempts": 10}))
    sent = 0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=16) as pool:
        for tick in range(a.seconds):
            # Timestamp each live burst at wall time: no synthetic future watermark jumps.
            events = list(generate(a.rate, a.seed + tick, datetime.now(timezone.utc)))
            now = datetime.now(timezone.utc)
            # Preserve journey ordering within this burst, with event times in the last five seconds.
            for e in events:
                step = KINDS.index(e["event_type"])
                e["event_time"] = (now - timedelta(seconds=5-step)).isoformat()
                e["user_id"] = f"{tick}-" + e["user_id"]
            jobs = [pool.submit(put_batch, client, a.stream, events[i:i+500])
                    for i in range(0, len(events), 500)]
            sent += sum(j.result() for j in jobs)
            time.sleep(max(0, started + tick + 1 - time.monotonic()))
    elapsed = time.monotonic() - started
    print(json.dumps({"acknowledged_events": sent, "elapsed_seconds": elapsed,
                      "achieved_events_per_second": sent / elapsed, "target": a.rate}))

if __name__ == "__main__":
    main()
