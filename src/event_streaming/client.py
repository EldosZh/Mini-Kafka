"""Synchronous connection-reusing clients. Ambiguous writes are never retried."""

import json
import socket
import struct
import threading
import time
import uuid

from .models import Metadata, Record, StreamError
from .protocol import MAX_FRAME, b64, frame, unb64


class Connection:
    def __init__(self, host="127.0.0.1", port=9092, timeout=15):
        self.host, self.port, self.timeout = host, port, timeout
        self.socket = None
        self.lock = threading.Lock()

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def _read(self, size):
        data = bytearray()
        while len(data) < size:
            part = self.socket.recv(size - len(data))
            if not part:
                raise ConnectionError(
                    "Broker disconnected; write outcome may be unknown"
                )
            data.extend(part)
        return data

    def request(self, operation, **payload):
        # ID позволяет убедиться, что ответ относится именно к этому запросу.
        request_id = uuid.uuid4().hex
        packet = frame(
            dict(version=1, request_id=request_id, type=operation, payload=payload)
        )
        with self.lock:
            try:
                if self.socket is None:
                    self.socket = socket.create_connection(
                        (self.host, self.port), self.timeout
                    )
                    self.socket.settimeout(self.timeout)
                self.socket.sendall(packet)
                size = struct.unpack(">I", self._read(4))[0]
                if not 0 < size <= MAX_FRAME:
                    raise ConnectionError("Invalid response size")
                response = json.loads(self._read(size))
                if response["request_id"] != request_id or response["version"] != 1:
                    raise ConnectionError("Response correlation failed")
            except (OSError, ConnectionError, ValueError, KeyError):
                # Автоповтор записи опасен: брокер мог уже сохранить её до обрыва связи.
                self.close()
                raise
            if not response["ok"]:
                error = response["error"]
                raise StreamError(
                    error["code"],
                    error["message"],
                    **{k: v for k, v in error.items() if k not in ("code", "message")},
                )
            return response["data"]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Producer(Connection):
    def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        partition: int | None = None,
        headers: dict[str, bytes] | None = None,
        acks: str = "1",
    ) -> Metadata:
        return Metadata(
            **self.request(
                "PRODUCE", **self.payload(topic, value, key, partition, headers, acks)
            )
        )

    @staticmethod
    def payload(topic, value, key=None, partition=None, headers=None, acks="1"):
        return dict(
            topic=topic,
            value=b64(value),
            key=b64(key),
            partition=partition,
            headers={k: b64(v) for k, v in (headers or {}).items()},
            acks=acks,
        )

    def send_batch(
        self, topic: str, records: list[bytes | dict], acks: str = "1"
    ) -> list[Metadata]:
        """Records are bytes or dictionaries with value/key/partition/headers fields.

        One bounded frame; sequential partial success is possible on error.
        """
        payloads = [
            self.payload(topic, **r, acks=acks)
            if isinstance(r, dict)
            else self.payload(topic, r, acks=acks)
            for r in records
        ]
        return [Metadata(**r) for r in self.request("PRODUCE_BATCH", records=payloads)]


class Consumer:
    def __init__(
        self,
        broker="127.0.0.1:9092",
        topic="orders",
        group_id="default",
        from_beginning=False,
        max_records=500,
        max_bytes=4 * 1024**2,
    ):
        host, port = broker.rsplit(":", 1)
        self.connection = Connection(host, int(port))
        self.heartbeat_connection = Connection(host, int(port))
        self.topic, self.group_id = topic, group_id
        self.from_beginning = from_beginning
        self.max_records, self.max_bytes = max_records, max_bytes
        self.lock = threading.RLock()
        self.positions, self.assignment = {}, []
        self.member_id, self.generation_id = "", -1
        self.error = None
        self.closed = threading.Event()
        self._join()
        self.thread = threading.Thread(target=self._heartbeats, daemon=True)
        self.thread.start()

    def _join(self):
        response = self.connection.request(
            "JOIN_GROUP", group_id=self.group_id, topic=self.topic
        )
        self.session_timeout = response["session_timeout"]
        self._update(response)

    def _update(self, response):
        if response["generation_id"] != self.generation_id:
            # Discard uncommitted local positions on every generation change.
            self.positions.clear()
        self.member_id = response["member_id"]
        self.generation_id = response["generation_id"]
        self.assignment = response["assignment"]

    def _heartbeats(self):
        while not self.closed.wait(max(0.01, self.session_timeout / 3)):
            try:
                with self.lock:
                    self._update(
                        self.heartbeat_connection.request(
                            "HEARTBEAT",
                            group_id=self.group_id,
                            member_id=self.member_id,
                        )
                    )
            except Exception as error:
                self.error = error
                return

    def _identity(self):
        return dict(
            group_id=self.group_id,
            member_id=self.member_id,
            generation_id=self.generation_id,
        )

    def poll(self, timeout_ms: int = 1000) -> list[Record]:
        deadline = time.monotonic() + timeout_ms / 1000
        while True:
            with self.lock:
                if self.error:
                    raise self.error
                self._update(
                    self.connection.request(
                        "HEARTBEAT", group_id=self.group_id, member_id=self.member_id
                    )
                )
                offsets = self.connection.request(
                    "FETCH_OFFSETS", group_id=self.group_id
                )
                description = self.connection.request(
                    "DESCRIBE_TOPIC", topic=self.topic
                )
                result = []
                for partition in self.assignment:
                    if partition not in self.positions:
                        row = description["offsets"][partition]
                        self.positions[partition] = offsets.get(
                            f"{self.topic}:{partition}",
                            row["start_offset"]
                            if self.from_beginning
                            else row["end_offset"],
                        )
                    try:
                        response = self.connection.request(
                            "FETCH",
                            topic=self.topic,
                            partition=partition,
                            offset=self.positions[partition],
                            max_records=self.max_records,
                            max_bytes=self.max_bytes,
                            max_wait_ms=0,
                            **self._identity(),
                        )
                    except StreamError as error:
                        if error.code == "ILLEGAL_GENERATION":
                            break
                        raise
                    for r in response["records"]:
                        result.append(
                            Record(
                                r["offset"],
                                r["timestamp_ms"],
                                unb64(r["key"], True),
                                unb64(r["value"]),
                                {k: unb64(v) for k, v in r["headers"].items()},
                                self.topic,
                                partition,
                                self.generation_id,
                            )
                        )
                        self.positions[partition] = r["offset"] + 1
                else:
                    if result or time.monotonic() >= deadline:
                        return result
            # Bounded pause keeps heartbeats independent and avoids a busy empty poll.
            self.closed.wait(min(0.05, max(0, deadline - time.monotonic())))
            if self.closed.is_set():
                return []

    def commit(self, record: Record | None = None) -> None:
        with self.lock:
            if record is not None and record.generation_id != self.generation_id:
                raise StreamError(
                    "ILLEGAL_GENERATION", "Record was fetched before rebalance"
                )
            positions = (
                self.positions
                if record is None
                else {record.partition: record.offset + 1}
            )
            if record is not None and record.topic != self.topic:
                raise ValueError("Record belongs to another topic")
            for partition, offset in positions.items():
                self.connection.request(
                    "COMMIT_OFFSET",
                    topic=self.topic,
                    partition=partition,
                    next_offset=offset,
                    **self._identity(),
                )

    def __iter__(self):
        while not self.closed.is_set():
            yield from self.poll()

    def close(self):
        self.closed.set()
        self.thread.join(timeout=16)
        try:
            self.connection.request(
                "LEAVE_GROUP", group_id=self.group_id, member_id=self.member_id
            )
        finally:
            self.connection.close()
            self.heartbeat_connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
