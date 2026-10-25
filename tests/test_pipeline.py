import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from clickstream.generator import generate, put_batch
from clickstream.analytics import analyze

class PipelineTests(unittest.TestCase):
    def events(self):
        return list(generate(100, start=datetime(2026, 1, 1, tzinfo=timezone.utc)))

    def test_generator_is_deterministic(self):
        self.assertEqual(self.events(), self.events())

    def test_replay_does_not_double_count_money(self):
        events = self.events()
        self.assertEqual(analyze(events), analyze(events + events))

    def test_out_of_order_arrival_has_same_results(self):
        events = self.events()
        self.assertEqual(analyze(events), analyze(list(reversed(events))))

    def test_quarantine(self):
        events = self.events()
        result = analyze(events + [{"event_id": "broken"}, dict(events[0], revenue_cents=-1)])
        self.assertEqual(result["valid_events"], 100)
        self.assertEqual(result["rejected_events"], 2)

    def test_session_boundary_and_ordered_funnel(self):
        base = self.events()[0]
        def e(i, minute, kind):
            return dict(base, event_id=str(i), event_time=f"2026-01-01T00:{minute:02d}:00+00:00",
                        event_type=kind, quantity=int(kind == "purchase"),
                        order_id="order" if kind == "purchase" else None)
        result = analyze([e(1, 0, "purchase"), e(2, 1, "product_view"),
                          e(3, 2, "add_to_cart"), e(4, 3, "checkout"), e(5, 33, "product_view")])
        self.assertEqual(len(result["sessions"]), 2)
        self.assertEqual(result["sessions"][0]["funnel_stage"], 3)

    @patch("clickstream.generator.time.sleep")
    def test_retry_only_failed_records(self, sleep):
        class Client:
            sizes = []
            def put_records(self, **kw):
                self.sizes.append(len(kw["Records"]))
                return {"Records": [{}, {"ErrorCode": "throttled"}]} if len(self.sizes) == 1 else {"Records": [{}]}
        c = Client()
        self.assertEqual(put_batch(c, "demo", self.events()[:2]), 2)
        self.assertEqual(c.sizes, [2, 1])

    @patch("clickstream.generator.time.sleep")
    def test_exhausted_retry_fails(self, sleep):
        class Client:
            def put_records(self, **kw):
                return {"Records": [{"ErrorCode": "throttled"}]}
        with self.assertRaises(RuntimeError):
            put_batch(Client(), "demo", self.events()[:1], retries=1)

if __name__ == "__main__":
    unittest.main()
