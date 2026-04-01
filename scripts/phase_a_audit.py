"""
Phase A: Production Audit Report

Generates a report of event distribution across top 5 airlines
and total revenue processed. Designed for restricted bastion hosts
with ≤30MB available RAM.
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def extract_price(raw):
    """
    Extract a numeric price from the raw value.

    Handles two formats found in the stream:
      - Plain numeric:  811.63
      - Nested object:  {"amount": 724.8, "currency": "USD"}

    Returns None if the value cannot be interpreted as a price.
    """
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, dict):
        amt = raw.get("amount")
        if isinstance(amt, (int, float)):
            return float(amt)
        return None
    if isinstance(raw, str):
        try:
            return float(raw)
        except ValueError:
            return None
    return None


class StreamingAuditor:
    """Single-pass streaming auditor for JSONL event files."""

    def __init__(self, filepath):
        self.filepath = Path(filepath)
        self.airline_stats = defaultdict(
            lambda: {"count": 0, "revenue": 0.0}
        )
        self.total_events = 0
        self.total_revenue = 0.0
        self.parse_errors = 0
        self.validation_errors = 0
        self.nested_price_count = 0
        self.missing_booking_id = 0

    def run(self):
        """Stream through the file, aggregate, return report dict."""
        try:
            with open(self.filepath, "r", encoding="utf-8") as fh:
                for line_num, line in enumerate(fh, 1):
                    stripped = line.strip()
                    if not stripped:
                        continue
                    self._process_line(stripped, line_num)
        except FileNotFoundError:
            print(f"ERROR: File not found: {self.filepath}", file=sys.stderr)
            sys.exit(1)

        return self._build_report()

    def _process_line(self, line, line_num):
        # Parse JSON
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            self.parse_errors += 1
            return

        # Validate required fields
        event_id = event.get("event_id")
        airline = event.get("airline")
        action = event.get("action")
        timestamp = event.get("timestamp")
        raw_price = event.get("price")

        if not all([event_id, airline, action, timestamp]):
            self.validation_errors += 1
            return

        if raw_price is None:
            self.validation_errors += 1
            return

        # Extract price 
        price = extract_price(raw_price)
        if price is None or price < 0:
            self.validation_errors += 1
            return

        if isinstance(raw_price, dict):
            self.nested_price_count += 1

        if "booking_id" not in event:
            self.missing_booking_id += 1

        # Aggregate
        self.total_events += 1
        self.total_revenue += price
        self.airline_stats[airline]["count"] += 1
        self.airline_stats[airline]["revenue"] += price

        # Progress indicator every 500k events
        if self.total_events % 500_000 == 0:
            print(
                f"  ... {self.total_events:,} events processed", file=sys.stderr
            )

    def _build_report(self):
        top5 = sorted(
            self.airline_stats.items(),
            key=lambda x: x[1]["count"],
            reverse=True,
        )[:5]

        return {
            "report_timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_events": self.total_events,
                "total_revenue": round(self.total_revenue, 2),
                "unique_airlines": len(self.airline_stats),
                "parse_errors": self.parse_errors,
                "validation_errors": self.validation_errors,
                "nested_price_events": self.nested_price_count,
                "missing_booking_id": self.missing_booking_id,
            },
            "top_5_airlines": [
                {
                    "rank": i + 1,
                    "airline": airline,
                    "event_count": stats["count"],
                    "total_revenue": round(stats["revenue"], 2),
                    "avg_value": round(stats["revenue"] / stats["count"], 2),
                }
                for i, (airline, stats) in enumerate(top5)
            ],
        }


def main():
    args = dict(
        input="data/raw/stream_logs.jsonl",
        output="data/processed/phase_a_report.json"
    )

    auditor = StreamingAuditor(args["input"])
    report = auditor.run()
    report_json = json.dumps(report, indent=2)

    Path(args["output"]).parent.mkdir(parents=True, exist_ok=True)
    with open(args["output"], "w", encoding="utf-8") as f:
        f.write(report_json)
    print(f"Report written to {args['output']}", file=sys.stderr)

if __name__ == "__main__":
    main()
