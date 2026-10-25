"""Dependency-free, finite-data reference; does not emulate Spark watermarks."""
from collections import defaultdict
from datetime import datetime
import hashlib
from .generator import KINDS


def validate(e):
    try:
        dt = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
        return (dt.tzinfo is not None and e["schema_version"] == 1
                and all(isinstance(e[k], str) and e[k] for k in ("event_id", "user_id", "product_id"))
                and e["event_type"] in KINDS and e["currency"] == "USD"
                and isinstance(e["quantity"], int) and e["quantity"] >= 0
                and isinstance(e["revenue_cents"], int) and e["revenue_cents"] >= 0
                and ((e["event_type"] == "purchase" and bool(e.get("order_id")) and e["quantity"] > 0)
                     or (e["event_type"] != "purchase" and e["quantity"] == 0 and e["revenue_cents"] == 0)))
    except (ValueError, TypeError, KeyError):
        return False


def analyze(events, gap_seconds=1800):
    seen, valid, rejected = set(), [], []
    for e in events:
        if not validate(e):
            rejected.append(e)
        elif e["event_id"] not in seen:
            valid.append(e)
            seen.add(e["event_id"])
    users, products = defaultdict(list), defaultdict(lambda: dict(views=0, carts=0, purchases=0, revenue_cents=0))
    for e in valid:
        users[e["user_id"]].append(e)
        prod = products[e["product_id"]]
        prod["views"] += e["event_type"] == "product_view"
        prod["carts"] += e["event_type"] == "add_to_cart"
        prod["purchases"] += e["event_type"] == "purchase"
        prod["revenue_cents"] += e["revenue_cents"]
    sessions = []
    def finish(user, group):
        stage = 0
        for e in group:
            if stage < 4 and e["event_type"] == KINDS[stage + 1]:
                stage += 1
        start, end = group[0]["event_time"], group[-1]["event_time"]
        sessions.append(dict(session_id=hashlib.sha256(f"{user}:{start}".encode()).hexdigest(),
                             user_id=user, start=start, end=end, events=len(group), funnel_stage=stage,
                             revenue_cents=sum(e["revenue_cents"] for e in group)))
    for user, items in users.items():
        items.sort(key=lambda e: (datetime.fromisoformat(e["event_time"]), e["event_id"]))
        group = []
        previous = None
        for e in items:
            current = datetime.fromisoformat(e["event_time"])
            if group and (current - previous).total_seconds() >= gap_seconds:
                finish(user, group)
                group = []
            group.append(e)
            previous = current
        if group:
            finish(user, group)
    return dict(valid_events=len(valid), rejected_events=len(rejected), sessions=sorted(sessions, key=lambda s: (s["user_id"], s["start"])),
                products=[dict(product_id=k, **v) for k, v in sorted(products.items())])
