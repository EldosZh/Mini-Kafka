import asyncio
import subprocess
import sys
import time

import pytest

from event_streaming import Consumer, Producer, StreamError
from event_streaming.broker import Broker
from event_streaming.client import Connection
from event_streaming.protocol import b64
from event_streaming.storage import PartitionLog


async def request(broker, operation, **payload):
    def execute():
        with Connection(port=broker.port) as client:
            return client.request(operation, **payload)

    return await asyncio.to_thread(execute)


def test_failed_append_preserves_offsets(tmp_path):
    log = PartitionLog(tmp_path, max_record=80)
    log.append(None, b"valid")
    before = log.segments[0].path.read_bytes()
    with pytest.raises(StreamError):
        log.append(None, b"x" * 100)
    assert log.segments[0].path.read_bytes() == before
    assert log.append(None, b"next").offset == 1


async def test_periodic_retention_and_errors(tmp_path):
    broker = await Broker(
        tmp_path, port=0, segment_size=50, retention_interval=0.02
    ).start()
    try:
        await request(broker, "CREATE_TOPIC", topic="orders", retention_ms=0)
        for _ in range(5):
            await request(broker, "PRODUCE", topic="orders", value=b64(b"payload"))
        deadline = time.monotonic() + 2
        while (
            broker.registry.partition("orders", 0).start < 4
            and time.monotonic() < deadline
        ):
            await asyncio.sleep(0.02)
        with pytest.raises(StreamError) as error:
            await request(broker, "FETCH", topic="orders", partition=0, offset=0)
        assert error.value.code == "OFFSET_OUT_OF_RANGE"
        assert error.value.details["earliest_offset"] == 4
        invalid = [
            ("PRODUCE", dict(topic="orders", value="%%%"), "INVALID_REQUEST"),
            (
                "PRODUCE",
                dict(topic="orders", value=b64(b"x"), partition=99),
                "UNKNOWN_PARTITION",
            ),
            ("FETCH", dict(topic="missing", partition=0, offset=0), "UNKNOWN_TOPIC"),
            ("FETCH", dict(topic="orders", partition=0, offset=-1), "INVALID_REQUEST"),
            (
                "FETCH",
                dict(topic="orders", partition=0, offset=4, max_bytes=1),
                "MESSAGE_TOO_LARGE",
            ),
            ("PRODUCE_BATCH", dict(records=[1]), "INVALID_REQUEST"),
        ]
        for operation, payload, code in invalid:
            with pytest.raises(StreamError) as error:
                await request(broker, operation, **payload)
            assert error.value.code == code
        assert await request(broker, "PING") == {"status": "ok"}
    finally:
        await broker.close()


async def test_real_consumers_rebalance_and_fence_records(tmp_path):
    broker = await Broker(tmp_path, port=0, session_timeout=1).start()
    try:
        await request(broker, "CREATE_TOPIC", topic="orders", partitions=6)

        def clients():
            address = f"127.0.0.1:{broker.port}"
            with Producer(port=broker.port) as producer:
                producer.send_batch("orders", [str(i).encode() for i in range(12)])
            with Consumer(address, "orders", "analytics", True) as a:
                old = a.poll(0)[0]
                with Consumer(address, "orders", "analytics", True) as b:
                    a.poll(0)
                    b.poll(0)
                    assert not set(a.assignment) & set(b.assignment)
                    assert sorted(a.assignment + b.assignment) == list(range(6))
                    with pytest.raises(StreamError) as error:
                        a.commit(old)
                    assert error.value.code == "ILLEGAL_GENERATION"
                    a.commit()
                    b.commit()
                    # Background heartbeats keep both members alive during processing.
                    time.sleep(1.3)
                    assert not a.error and not b.error
                    assert a.poll(0) == b.poll(0) == []
                a.poll(0)
                assert a.assignment == list(range(6))

        await asyncio.to_thread(clients)
    finally:
        await broker.close()


async def test_connection_reconnects_after_explicit_close(tmp_path):
    broker = await Broker(tmp_path, port=0).start()
    try:

        def exercise():
            with Producer(port=broker.port) as client:
                client.request("CREATE_TOPIC", topic="orders")
                assert client.send("orders", b"a").offset == 0
                client.close()
                assert client.send("orders", b"b").offset == 1

        await asyncio.to_thread(exercise)
    finally:
        await broker.close()


async def test_installed_cli_workflow(tmp_path):
    broker = await Broker(tmp_path, port=0, segment_size=100).start()
    try:
        # Invoke the public module entry point in an isolated subprocess.
        async def public_cli(*args, input=None):
            result = await asyncio.to_thread(
                subprocess.run,
                [
                    sys.executable,
                    "-m",
                    "event_streaming",
                    *args,
                    "--port",
                    str(broker.port),
                ],
                capture_output=True,
                text=True,
                input=input,
                timeout=15,
            )
            assert result.returncode == 0, result.stderr
            return result.stdout

        assert "orders" in await public_cli(
            "topic", "create", "orders", "--partitions", "3"
        )
        await public_cli("produce", "orders", "--stdin", input="one\ntwo\nthree\n")
        consumed = await public_cli(
            "consume",
            "orders",
            "--group",
            "analytics",
            "--from-beginning",
            "--limit",
            "3",
        )
        assert all(value in consumed for value in ("one", "two", "three"))
        for args in [
            ("topic", "list"),
            ("topic", "describe", "orders"),
            ("group", "list"),
            ("group", "describe", "analytics"),
            ("storage", "inspect", "orders"),
            ("storage", "dump", "orders"),
            ("broker", "info"),
            ("broker", "metrics"),
        ]:
            assert await public_cli(*args)
        assert "messages_per_second" in await public_cli(
            "benchmark",
            "produce",
            "--topic",
            "orders",
            "--messages",
            "12",
            "--batch-size",
            "3",
        )
        assert "messages_per_second" in await public_cli(
            "benchmark", "consume", "--topic", "orders", "--messages", "12"
        )
        await public_cli("topic", "delete", "orders", "--yes")
        assert await request(broker, "LIST_TOPICS") == []
    finally:
        await broker.close()
