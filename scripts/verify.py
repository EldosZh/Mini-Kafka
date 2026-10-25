"""Reproducible small benchmark and restart smoke check against temporary storage."""

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

from event_streaming import Consumer, Producer
from event_streaming.benchmark import run
from event_streaming.broker import Broker


async def verify(root: Path):
    broker = await Broker(root, port=0, segment_size=1024, index_interval=2).start()
    result = {}
    try:
        broker.registry.create("orders", 3)

        def exercise():
            with Producer(port=broker.port) as producer:
                keyed = [
                    producer.send("orders", str(i).encode(), key=b"same", acks="fsync")
                    for i in range(6)
                ]
                rr = producer.send_batch("orders", [b"round-robin"] * 6)
                assert len({r.partition for r in keyed}) == 1
                assert [r.partition for r in rr] == [0, 1, 2, 0, 1, 2]
            with Consumer(
                f"127.0.0.1:{broker.port}", "orders", "analytics", True
            ) as consumer:
                records = consumer.poll(0)
                assert len(records) == 12
                consumer.commit()
            return {
                partition: broker.registry.partition("orders", partition).end
                for partition in range(3)
            }

        ends = await asyncio.to_thread(exercise)
        broker.registry.create("benchmark", 3)
        result["producer_benchmark"] = await asyncio.to_thread(
            run, "produce", "127.0.0.1", broker.port, "benchmark", 300, 256, 30
        )
        result["consumer_benchmark"] = await asyncio.to_thread(
            run, "consume", "127.0.0.1", broker.port, "benchmark", 300, 256, 30
        )
    finally:
        await broker.close()

    # Derived-index corruption and an incomplete active tail, using only temporary data.
    log_dir = root / "topics" / "orders" / "partition-0000"
    for index in log_dir.glob("*.index"):
        index.write_bytes(b"corrupt derived index")
    active = sorted(log_dir.glob("*.log"))[-1]
    original_size = active.stat().st_size
    with active.open("ab") as file:
        file.write(b"ESTR\x01")

    restarted = await Broker(root, port=0, segment_size=1024, index_interval=2).start()
    try:
        assert active.stat().st_size == original_size

        def check_restart():
            with Consumer(
                f"127.0.0.1:{restarted.port}", "orders", "analytics", True
            ) as consumer:
                assert consumer.poll(0) == []
                with Producer(port=restarted.port) as producer:
                    appended = producer.send(
                        "orders", b"after restart", partition=0, acks="fsync"
                    )
                    assert appended.offset == ends[0]
                records = consumer.poll(0)
                assert len(records) == 1 and records[0].value == b"after restart"
                consumer.commit()
            with Consumer(
                f"127.0.0.1:{restarted.port}", "orders", "email-service", True
            ) as consumer:
                assert len(consumer.poll(0)) == 13

        await asyncio.to_thread(check_restart)
        result.update(
            persistence_restart="passed",
            committed_next_offset_resume="passed",
            independent_groups="passed",
            index_rebuild="passed",
            incomplete_tail_recovery="passed",
            deterministic_key_partitioning="passed",
            keyless_round_robin="passed",
        )
    finally:
        await restarted.close()
    final = await Broker(root, port=0).start()
    try:
        assert sum(log.end - log.start for log in final.registry.get("orders")[1]) == 13
        result["final_storage_integrity"] = "passed"
    finally:
        await final.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Write the measured JSON report")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(
        prefix="event-stream-verify-", dir="."
    ) as directory:
        report = json.dumps(asyncio.run(verify(Path(directory))), indent=2)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(report + "\n", encoding="utf8")
        print(report)
