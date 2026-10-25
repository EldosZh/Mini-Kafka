"""Async TCP broker; disk work is offloaded and serialized per partition."""

import asyncio
import contextlib
import hashlib
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from .groups import Coordinator
from .models import Record, StreamError
from .protocol import MAX_FRAME, frame, read_frame, unb64, wire
from .registry import Registry
from .storage import integer

LOG = logging.getLogger(__name__)


class Broker:
    def __init__(
        self,
        data_dir,
        host="127.0.0.1",
        port=9092,
        session_timeout=10,
        retention_interval=30,
        max_connections=128,
        default_retention_ms=604800000,
        **log_config,
    ):
        self.registry = Registry(data_dir, **log_config)
        try:
            self.coordinator = Coordinator(self.registry, session_timeout)
        except BaseException:
            self.registry.close()
            raise
        self.host, self.port = host, port
        self.retention_interval = retention_interval
        self.max_connections = max_connections
        self.default_retention_ms = default_retention_ms
        self.disk_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="broker-disk"
        )
        self.locks = defaultdict(asyncio.Lock)
        self.events = defaultdict(asyncio.Event)
        self.round_robin = defaultdict(int)
        self.metrics = defaultdict(float)
        self.tasks, self.writers = set(), set()
        self.started = time.monotonic()
        self.server = None
        self.stopping = False

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle, self.host, self.port, limit=MAX_FRAME + 4
        )
        self.port = self.server.sockets[0].getsockname()[1]
        self.maintenance = asyncio.create_task(self.maintain())
        LOG.info("Broker listening on %s:%d", self.host, self.port)
        return self

    async def close(self):
        self.stopping = True
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            await self.maintenance
        for event in self.events.values():
            event.set()
        for writer in list(self.writers):
            writer.close()
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)
        self.disk_executor.shutdown(wait=True)
        self.registry.close()

    async def disk(self, function, *args):
        return await asyncio.get_running_loop().run_in_executor(
            self.disk_executor, partial(function, *args)
        )

    async def maintain(self):
        last_retention = time.monotonic()
        while not self.stopping:
            await asyncio.sleep(min(0.1, self.retention_interval))
            self.coordinator.expire()
            if time.monotonic() - last_retention >= self.retention_interval:
                # Same per-partition locks used by append/fetch/deletion.
                for topic, (metadata, logs) in list(self.registry.topics.items()):
                    for partition, log in enumerate(logs):
                        async with self.locks[topic, partition]:
                            current = self.registry.topics.get(topic)
                            if current is None or current[1][partition] is not log:
                                continue
                            await self.disk(
                                log.retain,
                                metadata["retention_ms"],
                                metadata["retention_bytes"],
                            )
                last_retention = time.monotonic()

    async def handle(self, reader, writer):
        if self.stopping or len(self.writers) >= self.max_connections:
            writer.close()
            await writer.wait_closed()
            return
        task = asyncio.current_task()
        self.tasks.add(task)
        self.writers.add(writer)
        try:
            while not self.stopping:
                request_id = None
                operation = ""
                started = time.monotonic()
                try:
                    request = await asyncio.wait_for(read_frame(reader), 30)
                    if not isinstance(request, dict):
                        raise StreamError("INVALID_REQUEST", "Expected request object")
                    request_id = request.get("request_id")
                    if not isinstance(request_id, str) or len(request_id) > 128:
                        raise StreamError("INVALID_REQUEST", "Invalid request ID")
                    if (
                        type(request.get("version")) is not int
                        or request["version"] != 1
                    ):
                        raise StreamError(
                            "UNSUPPORTED_VERSION", "Only version 1 is supported"
                        )
                    operation = request.get("type")
                    payload = request.get("payload", {})
                    if not isinstance(operation, str) or not isinstance(payload, dict):
                        raise StreamError(
                            "INVALID_REQUEST", "Invalid operation or payload"
                        )
                    self.coordinator.expire()
                    data = await self.dispatch(operation, payload)
                    response = dict(
                        version=1, request_id=request_id, ok=True, data=data
                    )
                except (TimeoutError, asyncio.IncompleteReadError, ConnectionError):
                    break
                except StreamError as error:
                    response = dict(
                        version=1,
                        request_id=request_id,
                        ok=False,
                        error=dict(
                            code=error.code, message=str(error), **error.details
                        ),
                    )
                except (ValueError, TypeError, KeyError, OverflowError):
                    response = dict(
                        version=1,
                        request_id=request_id,
                        ok=False,
                        error=dict(
                            code="INVALID_REQUEST", message="Invalid request fields"
                        ),
                    )
                except Exception:
                    LOG.exception("Request failed")
                    response = dict(
                        version=1,
                        request_id=request_id,
                        ok=False,
                        error=dict(
                            code="INTERNAL_ERROR", message="Broker operation failed"
                        ),
                    )
                if operation in ("PRODUCE", "PRODUCE_BATCH", "FETCH"):
                    label = "fetch" if operation == "FETCH" else "produce"
                    if not response["ok"]:
                        self.metrics[f"{label}_errors_total"] += 1
                    self.metrics[f"{label}_latency_seconds_sum"] += (
                        time.monotonic() - started
                    )
                    self.metrics[f"{label}_latency_seconds_count"] += 1
                writer.write(frame(response))
                await asyncio.wait_for(writer.drain(), 10)
        except (TimeoutError, ConnectionError, StreamError):
            pass
        finally:
            self.tasks.discard(task)
            self.writers.discard(writer)
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    async def produce(self, p):
        topic = p["topic"]
        logs = self.registry.get(topic)[1]
        key, value = unb64(p.get("key"), True), unb64(p["value"])
        headers = p.get("headers", {})
        if not isinstance(headers, dict) or len(headers) > 256:
            raise StreamError("INVALID_REQUEST", "Invalid headers")
        headers = {k: unb64(v) for k, v in headers.items()}
        largest_envelope = Record(2**63 - 1, 2**63 - 1, key, value, headers)
        if len(frame(wire(largest_envelope))) > MAX_FRAME - 1024:
            raise StreamError("MESSAGE_TOO_LARGE", "Record cannot fit a fetch response")
        partition = p.get("partition")
        if partition is None:
            if key is None:
                partition = self.round_robin[topic] % len(logs)
                self.round_robin[topic] += 1
            else:
                partition = int.from_bytes(hashlib.sha256(key).digest(), "big") % len(
                    logs
                )
        log = self.registry.partition(topic, partition)
        acks = p.get("acks", "1")
        if acks not in ("1", "fsync"):
            raise StreamError("INVALID_REQUEST", "acks must be 1 or fsync")
        async with self.locks[topic, partition]:
            # Topic may have been removed while waiting for its partition lock.
            if self.registry.partition(topic, partition) is not log:
                raise StreamError("UNKNOWN_TOPIC", "Topic was replaced during request")
            record = await self.disk(log.append, key, value, headers, acks == "fsync")
            self.events[topic, partition].set()
        self.metrics["messages_produced_total"] += 1
        self.metrics["bytes_produced_total"] += len(value)
        return dict(
            topic=topic,
            partition=partition,
            offset=record.offset,
            timestamp_ms=record.timestamp_ms,
        )

    async def dispatch(self, op, p):
        if op == "PING":
            return {"status": "ok"}
        if op in ("METADATA", "LIST_TOPICS"):
            return [self.registry.describe(t) for t in sorted(self.registry.topics)]
        if op == "CREATE_TOPIC":
            p.setdefault("retention_ms", self.default_retention_ms)
            return self.registry.create(**p)
        if op == "DESCRIBE_TOPIC":
            return self.registry.describe(p["topic"])
        if op == "DELETE_TOPIC":
            topic = p["topic"]
            logs = self.registry.get(topic)[1]
            async with contextlib.AsyncExitStack() as stack:
                for partition in range(len(logs)):
                    await stack.enter_async_context(self.locks[topic, partition])
                self.registry.delete(topic)
                for partition in range(len(logs)):
                    self.events[topic, partition].set()
                for group_id in self.coordinator.groups:
                    group = self.coordinator.groups[group_id]
                    group["offsets"] = {
                        key: value
                        for key, value in group["offsets"].items()
                        if not key.startswith(f"{topic}:")
                    }
                    self.coordinator.rebalance(group_id)
            return {}
        if op == "PRODUCE":
            return await self.produce(p)
        if op == "PRODUCE_BATCH":
            records = p["records"]
            if not isinstance(records, list) or not 1 <= len(records) <= 1000:
                raise StreamError(
                    "INVALID_REQUEST", "Batch must contain 1..1000 records"
                )
            if not all(isinstance(record, dict) for record in records):
                raise StreamError("INVALID_REQUEST", "Batch entries must be objects")
            return [await self.produce(record) for record in records]
        if op == "FETCH":
            topic, partition = p["topic"], p["partition"]
            log = self.registry.partition(topic, partition)
            wait = integer(p.get("max_wait_ms", 0), 0, 10000) / 1000
            event = self.events[topic, partition]
            deadline = time.monotonic() + wait
            while True:
                async with self.locks[topic, partition]:
                    if self.registry.partition(topic, partition) is not log:
                        raise StreamError(
                            "UNKNOWN_TOPIC", "Topic was replaced during request"
                        )
                    if "group_id" in p:
                        self.coordinator.owner(
                            p["group_id"],
                            p["member_id"],
                            p["generation_id"],
                            topic,
                            partition,
                        )
                    event.clear()
                    records = await self.disk(
                        log.fetch,
                        p["offset"],
                        p.get("max_records", 500),
                        p.get("max_bytes", 4 * 1024**2),
                    )
                    if "group_id" in p:
                        self.coordinator.owner(
                            p["group_id"],
                            p["member_id"],
                            p["generation_id"],
                            topic,
                            partition,
                        )
                if records or time.monotonic() >= deadline or self.stopping:
                    break
                try:
                    await asyncio.wait_for(event.wait(), deadline - time.monotonic())
                except TimeoutError:
                    break
            returned, used = [], 0
            for record in records:
                item = wire(record)
                length = len(frame(item))
                if used + length > MAX_FRAME - 1024:
                    break
                returned.append(item)
                used += length
            if records and not returned:
                raise StreamError(
                    "MESSAGE_TOO_LARGE", "Record cannot fit a fetch response"
                )
            records = records[: len(returned)]
            self.metrics["messages_fetched_total"] += len(records)
            self.metrics["bytes_fetched_total"] += sum(len(r.value) for r in records)
            return dict(
                records=returned,
                start_offset=log.start,
                end_offset=log.end,
            )
        if op == "JOIN_GROUP":
            return self.coordinator.join(**p)
        if op == "HEARTBEAT":
            return self.coordinator.heartbeat(**p)
        if op == "LEAVE_GROUP":
            self.coordinator.leave(**p)
            return {}
        if op == "COMMIT_OFFSET":
            self.coordinator.commit(**p)
            return {}
        if op == "FETCH_OFFSETS":
            return self.coordinator.get(p["group_id"])["offsets"]
        if op == "LIST_GROUPS":
            return sorted(self.coordinator.groups)
        if op == "DESCRIBE_GROUP":
            return self.coordinator.describe(p["group_id"])
        if op in ("INSPECT", "DUMP"):
            log = self.registry.partition(p["topic"], p["partition"])
            async with self.locks[p["topic"], p["partition"]]:
                if op == "INSPECT":
                    return log.inspect()
                records = log.fetch(p.get("offset", log.start), p.get("limit", 20))
                return [
                    wire(r)
                    if p.get("show_values", False)
                    else dict(
                        offset=r.offset,
                        timestamp_ms=r.timestamp_ms,
                        value_bytes=len(r.value),
                    )
                    for r in records
                ]
        if op == "BROKER_INFO":
            return dict(
                version="0.1.0",
                uptime_seconds=time.monotonic() - self.started,
                topics=len(self.registry.topics),
                partitions=sum(len(v[1]) for v in self.registry.topics.values()),
                active_connections=len(self.writers),
                data_directory=str(self.registry.root),
                storage_bytes=sum(
                    f.stat().st_size
                    for f in self.registry.root.rglob("*")
                    if f.is_file()
                ),
            )
        if op == "METRICS":
            result = {
                key: self.metrics[key]
                for key in (
                    "messages_produced_total",
                    "bytes_produced_total",
                    "messages_fetched_total",
                    "bytes_fetched_total",
                    "produce_errors_total",
                    "fetch_errors_total",
                    "produce_latency_seconds_sum",
                    "produce_latency_seconds_count",
                    "fetch_latency_seconds_sum",
                    "fetch_latency_seconds_count",
                )
            }
            result.update(
                active_connections=len(self.writers),
                topics=len(self.registry.topics),
                partitions=sum(len(v[1]) for v in self.registry.topics.values()),
            )
            return result
        raise StreamError("INVALID_REQUEST", "Unknown operation")
