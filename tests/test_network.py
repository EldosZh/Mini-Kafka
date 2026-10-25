import asyncio
import json
import struct

import pytest
import pytest_asyncio

from event_streaming import Consumer, Producer, StreamError
from event_streaming.benchmark import run
from event_streaming.broker import Broker
from event_streaming.client import Connection
from event_streaming.protocol import MAX_FRAME, b64, frame, read_frame


@pytest_asyncio.fixture
async def broker(tmp_path):
    service = await Broker(
        tmp_path, port=0, segment_size=160, session_timeout=0.4
    ).start()
    try:
        yield service
    finally:
        await service.close()


async def call(broker, op, **payload):
    def execute():
        with Connection(port=broker.port) as connection:
            return connection.request(op, **payload)

    return await asyncio.to_thread(execute)


async def test_framing_fragmentation_and_coalescing(broker):
    reader, writer = await asyncio.open_connection("127.0.0.1", broker.port)
    packet = frame(dict(version=1, request_id="fragment", type="PING", payload={}))
    for byte in packet:
        writer.write(bytes([byte]))
        await writer.drain()
    assert (await read_frame(reader))["data"] == {"status": "ok"}
    writer.write(packet + packet)
    await writer.drain()
    assert (await read_frame(reader))["ok"]
    assert (await read_frame(reader))["ok"]
    writer.close()
    await writer.wait_closed()


@pytest.mark.parametrize(
    "packet",
    [
        struct.pack(">I", 0),
        struct.pack(">I", MAX_FRAME + 1),
        struct.pack(">I", 1) + b"{",
        frame([]),
        frame(dict(version=99, request_id="x", type="PING")),
    ],
)
async def test_malformed_clients_isolated(broker, packet):
    reader, writer = await asyncio.open_connection("127.0.0.1", broker.port)
    writer.write(packet)
    await writer.drain()
    response = await asyncio.wait_for(read_frame(reader), 2)
    assert not response["ok"]
    writer.close()
    await writer.wait_closed()
    assert await call(broker, "PING") == {"status": "ok"}


async def test_partitioning_batches_concurrency(broker):
    await call(broker, "CREATE_TOPIC", topic="orders", partitions=3)

    def produce():
        with Producer(port=broker.port) as producer:
            keyed = [
                producer.send("orders", str(i).encode(), key=b"same") for i in range(5)
            ]
            keyless = producer.send_batch(
                "orders", [b"a", b"b", b"c", b"d", b"e", b"f"]
            )
            assert len({m.partition for m in keyed}) == 1
            assert [m.partition for m in keyless] == [0, 1, 2, 0, 1, 2]
            return keyed + keyless

    metadata = await asyncio.to_thread(produce)
    results = await asyncio.gather(
        *[
            call(
                broker,
                "PRODUCE",
                topic="orders",
                value=b64(str(i).encode()),
                partition=i % 3,
            )
            for i in range(30)
        ]
    )
    assert len(results) == 30
    for partition in range(3):
        records = (
            await call(broker, "FETCH", topic="orders", partition=partition, offset=0)
        )["records"]
        assert [r["offset"] for r in records] == list(range(len(records)))
        assert len(records) == 10 + sum(m.partition == partition for m in metadata)
    with pytest.raises(StreamError) as error:
        await call(broker, "PRODUCE", topic="orders", value=b64(b"x" * 1024**2))
    assert error.value.code == "MESSAGE_TOO_LARGE"
    metrics = await call(broker, "METRICS")
    assert metrics["messages_produced_total"] == 41
    assert metrics["produce_errors_total"] == 1
    assert (await call(broker, "INSPECT", topic="orders", partition=0))[0][
        "records"
    ] > 0
    assert "value" not in (await call(broker, "DUMP", topic="orders", partition=0))[0]


async def test_long_poll_wakes_on_append(broker):
    await call(broker, "CREATE_TOPIC", topic="orders")
    pending = asyncio.create_task(
        call(broker, "FETCH", topic="orders", partition=0, offset=0, max_wait_ms=1000)
    )
    await asyncio.sleep(0.05)
    assert not pending.done()
    await call(broker, "PRODUCE", topic="orders", value=b64(b"wake"))
    assert len((await pending)["records"]) == 1


async def test_groups_fencing_timeout_and_independence(broker):
    await call(broker, "CREATE_TOPIC", topic="orders", partitions=6)
    a = await call(broker, "JOIN_GROUP", group_id="analytics", topic="orders")
    b = await call(broker, "JOIN_GROUP", group_id="analytics", topic="orders")
    c = await call(broker, "JOIN_GROUP", group_id="analytics", topic="orders")
    group = await call(broker, "DESCRIBE_GROUP", group_id="analytics")
    assignments = [m["assignment"] for m in group["members"].values()]
    assert sorted(p for parts in assignments for p in parts) == list(range(6))
    assert all(len(parts) == 2 for parts in assignments)
    with pytest.raises(StreamError) as error:
        await call(
            broker,
            "COMMIT_OFFSET",
            group_id="analytics",
            member_id=a["member_id"],
            generation_id=a["generation_id"],
            topic="orders",
            partition=0,
            next_offset=0,
        )
    assert error.value.code == "ILLEGAL_GENERATION"
    independent = await call(
        broker, "JOIN_GROUP", group_id="email-service", topic="orders"
    )
    assert independent["assignment"] == list(range(6))
    # Keep A alive while B and C fail without LEAVE_GROUP.
    for _ in range(6):
        await asyncio.sleep(0.1)
        response = await call(
            broker, "HEARTBEAT", group_id="analytics", member_id=a["member_id"]
        )
    assert response["assignment"] == list(range(6))
    assert response["generation_id"] > c["generation_id"]
    assert (
        b["member_id"]
        not in (await call(broker, "DESCRIBE_GROUP", group_id="analytics"))["members"]
    )


async def test_clients_commit_restart_and_independent_groups(tmp_path):
    broker = await Broker(tmp_path, port=0).start()
    try:
        await call(broker, "CREATE_TOPIC", topic="orders", partitions=3)

        def session():
            with Producer(port=broker.port) as producer:
                for i in range(9):
                    producer.send("orders", str(i).encode(), acks="fsync")
            with Consumer(
                f"127.0.0.1:{broker.port}", "orders", "analytics", True
            ) as consumer:
                records = consumer.poll(0)
                assert len(records) == 9
                consumer.commit()
            with Consumer(
                f"127.0.0.1:{broker.port}", "orders", "email-service", True
            ) as other:
                assert len(other.poll(0)) == 9

        await asyncio.to_thread(session)
    finally:
        await broker.close()
    restarted = await Broker(tmp_path, port=0).start()
    try:

        def verify():
            with Consumer(
                f"127.0.0.1:{restarted.port}", "orders", "analytics", True
            ) as consumer:
                assert consumer.poll(0) == []
                with Producer(port=restarted.port) as producer:
                    assert producer.send("orders", b"after", partition=0).offset == 3
                records = consumer.poll(0)
                assert len(records) == 1 and records[0].value == b"after"
                consumer.commit(records[0])

        await asyncio.to_thread(verify)
    finally:
        await restarted.close()


async def test_benchmarks(broker):
    await call(broker, "CREATE_TOPIC", topic="benchmark", partitions=3)
    produced = await asyncio.to_thread(
        run, "produce", "127.0.0.1", broker.port, "benchmark", 30, 64, 10
    )
    consumed = await asyncio.to_thread(
        run, "consume", "127.0.0.1", broker.port, "benchmark", 30, 64, 10
    )
    assert produced["messages"] == consumed["messages"] == 30
    assert produced["messages_per_second"] > 0
    assert produced["latency_p99_ms"] >= produced["latency_p50_ms"]
    assert json.dumps(produced)


async def test_owner_fencing_and_invalid_commit(broker):
    await call(broker, "CREATE_TOPIC", topic="orders", partitions=2)
    first = await call(broker, "JOIN_GROUP", group_id="analytics", topic="orders")
    second = await call(broker, "JOIN_GROUP", group_id="analytics", topic="orders")
    first = await call(
        broker, "HEARTBEAT", group_id="analytics", member_id=first["member_id"]
    )
    partition = second["assignment"][0]
    with pytest.raises(StreamError) as error:
        await call(
            broker,
            "COMMIT_OFFSET",
            group_id="analytics",
            member_id=first["member_id"],
            generation_id=first["generation_id"],
            topic="orders",
            partition=partition,
            next_offset=0,
        )
    assert error.value.code == "NOT_PARTITION_OWNER"
    with pytest.raises(StreamError) as error:
        await call(
            broker,
            "COMMIT_OFFSET",
            group_id="analytics",
            member_id=second["member_id"],
            generation_id=second["generation_id"],
            topic="orders",
            partition=partition,
            next_offset=-1,
        )
    assert error.value.code == "INVALID_REQUEST"


async def test_partial_client_disconnect_and_fetch_timeout(broker):
    _, writer = await asyncio.open_connection("127.0.0.1", broker.port)
    writer.write(struct.pack(">I", 500) + b"partial")
    await writer.drain()
    writer.close()
    await writer.wait_closed()
    await call(broker, "CREATE_TOPIC", topic="orders")
    response = await call(
        broker, "FETCH", topic="orders", partition=0, offset=0, max_wait_ms=30
    )
    assert response["records"] == []
    assert (await call(broker, "PING"))["status"] == "ok"
