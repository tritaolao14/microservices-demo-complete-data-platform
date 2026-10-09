import json

from lakehouse.application.options import WriterOptions
from lakehouse.application.writer import BronzeOrdersWriter
from lakehouse.infrastructure.sinks import InMemorySink
from lakehouse.runner import run_jsonl
from lakehouse.tests.fakes import FakeEncoder

DATASET = "bronze/orders"


def make_writer(sink):
    options = WriterOptions(prefix=DATASET, flush_rows=100, flush_interval_seconds=0)
    return BronzeOrdersWriter(options, sink, encoder=FakeEncoder())


def test_run_jsonl_lands_records(tmp_path):
    path = tmp_path / "orders.jsonl"
    path.write_text(
        "\n".join(
            json.dumps({"order_id": f"o{i}", "timestamp": "2026-10-09T10:00:00Z"})
            for i in range(3)
        ),
        encoding="utf-8",
    )

    sink = InMemorySink()
    count = run_jsonl(str(path), make_writer(sink))

    assert count == 3
    assert sink.list_keys(f"{DATASET}/2026/10/09/part-")
