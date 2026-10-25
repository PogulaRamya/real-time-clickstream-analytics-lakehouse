import csv
import json
from pathlib import Path
from .analytics import analyze
from .generator import generate

def main():
    out = Path("build")
    out.mkdir(exist_ok=True)
    events = list(generate(10000))
    # Deliberate replay and invalid event exercise deduplication and quarantine.
    events += events[:100] + [{"event_id": "bad"}]
    result = analyze(events)
    for kind in ("sessions", "products"):
        with (out / f"{kind}.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(result[kind][0]))
            writer.writeheader()
            writer.writerows(result[kind])
    summary = {k: result[k] for k in ("valid_events", "rejected_events")}
    summary["sessions"] = len(result["sessions"])
    summary["ordered_converted_sessions"] = sum(s["funnel_stage"] == 4 for s in result["sessions"])
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    print("CSV outputs: build/sessions.csv and build/products.csv")

if __name__ == "__main__":
    main()
