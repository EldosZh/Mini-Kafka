"""CRC-protected segmented logs. Indexes are always disposable derived data."""

import bisect
import json
import logging
import os
import re
import struct
import threading
import time
import zlib
from pathlib import Path

from .models import Record, StreamError

LOG = logging.getLogger(__name__)
PREFIX = struct.Struct(">4sBII")
BODY = struct.Struct(">QQiIH")
ENTRY = struct.Struct(">QQ")
MAX_RECORD = 1024 * 1024


def name(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value
    ):
        raise StreamError("INVALID_REQUEST", "Invalid topic or group name")
    return value


def integer(value, minimum=0, maximum=2**63 - 1):
    # bool в Python тоже считается int, но для offset значение True не подходит.
    if type(value) is not int or not minimum <= value <= maximum:
        raise StreamError("INVALID_REQUEST", "Integer outside allowed range")
    return value


def contained(root: Path, path: Path) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(root.resolve()) or path.is_symlink():
        raise StreamError("INVALID_REQUEST", "Unsafe storage path")
    return resolved


def atomic(path: Path, value):
    # Сначала сохраняем новый файл целиком, потом заменяем старый.
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    with temp.open("w", encoding="utf8") as file:
        json.dump(value, file)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp, path)


def encode(record: Record, limit=MAX_RECORD) -> bytes:
    key = record.key
    headers = b""
    for label, value in record.headers.items():
        label = label.encode("utf8")
        headers += struct.pack(">HI", len(label), len(value)) + label + value
    body = BODY.pack(
        record.offset,
        record.timestamp_ms,
        -1 if key is None else len(key),
        len(record.value),
        len(record.headers),
    )
    body += (key or b"") + record.value + headers
    if len(body) + PREFIX.size > limit:
        raise StreamError("MESSAGE_TOO_LARGE", "Encoded record exceeds limit")
    # Контрольная сумма покрывает всё тело записи, включая offset и заголовки.
    return PREFIX.pack(b"ESTR", 1, len(body), zlib.crc32(body)) + body


def decode(data: bytes) -> Record:
    magic, version, length, crc = PREFIX.unpack(data[: PREFIX.size])
    body = data[PREFIX.size :]
    if (
        magic != b"ESTR"
        or version != 1
        or length != len(body)
        or zlib.crc32(body) != crc
    ):
        raise ValueError("Invalid record envelope or CRC")
    offset, stamp, kl, vl, count = BODY.unpack_from(body)
    if kl < -1:
        raise ValueError("Invalid key length")
    pos = BODY.size
    key = None if kl == -1 else body[pos : pos + kl]
    pos += max(kl, 0)
    value = body[pos : pos + vl]
    pos += vl
    headers = {}
    for _ in range(count):
        nl, size = struct.unpack_from(">HI", body, pos)
        pos += 6
        label = body[pos : pos + nl].decode("utf8")
        pos += nl
        headers[label] = body[pos : pos + size]
        pos += size
    if pos != len(body):
        raise ValueError("Invalid record lengths")
    return Record(offset, stamp, key, value, headers)


class Segment:
    def __init__(self, path: Path, limit: int, interval: int, active: bool):
        self.path, self.limit, self.interval = path, limit, interval
        self.base = int(path.stem)
        self.index = []
        self.count = 0
        self.last = self.base - 1
        self.timestamp = 0
        path.touch(exist_ok=True)
        with path.open("r+b") as file:
            while True:
                pos = file.tell()
                head = file.read(PREFIX.size)
                if not head:
                    break
                size, body = limit, b""
                try:
                    if len(head) != PREFIX.size:
                        raise ValueError("Partial prefix")
                    size = PREFIX.unpack(head)[2]
                    if size > limit - PREFIX.size:
                        raise ValueError("Invalid record size")
                    body = file.read(size)
                    record = decode(head + body)
                    if record.offset != self.last + 1:
                        raise ValueError("Noncontiguous offsets")
                except (ValueError, struct.error, UnicodeError):
                    # Only incomplete final writes are repairable; CRC corruption fails closed.
                    incomplete = len(head) < PREFIX.size or (
                        len(head) == PREFIX.size
                        and size <= limit - PREFIX.size
                        and len(body) < size
                    )
                    if not active or not incomplete:
                        raise ValueError(f"Corrupted log at {path}:{pos}") from None
                    file.truncate(pos)
                    LOG.warning("Recovered incomplete tail in %s", path)
                    break
                if self.count % interval == 0:
                    self.index.append((record.offset, pos))
                self.count += 1
                self.last = record.offset
                self.timestamp = max(self.timestamp, record.timestamp_ms)
        self.size = path.stat().st_size
        self.rebuild_index()

    def rebuild_index(self):
        with contained(self.path.parent, self.path.with_suffix(".index")).open(
            "wb"
        ) as file:
            for entry in self.index:
                file.write(ENTRY.pack(*entry))

    def append(self, record, data, fsync):
        pos = self.size
        with self.path.open("r+b") as file:
            file.seek(pos)
            try:
                file.write(data)
                file.flush()
                if fsync:
                    os.fsync(file.fileno())
            except BaseException:
                file.truncate(pos)
                raise
        if self.count % self.interval == 0:
            self.index.append((record.offset, pos))
            # An index failure must not turn a successful log append into a failed append.
            try:
                with contained(self.path.parent, self.path.with_suffix(".index")).open(
                    "ab"
                ) as file:
                    file.write(ENTRY.pack(record.offset, pos))
            except OSError:
                LOG.exception("Derived index write failed")
        self.size += len(data)
        self.count += 1
        self.last = record.offset
        self.timestamp = max(self.timestamp, record.timestamp_ms)

    def read(self, offset):
        index = bisect.bisect_right(self.index, (offset, 2**63)) - 1
        with self.path.open("rb") as file:
            file.seek(self.index[index][1] if index >= 0 else 0)
            while head := file.read(PREFIX.size):
                record = decode(head + file.read(PREFIX.unpack(head)[2]))
                if record.offset >= offset:
                    yield record


class PartitionLog:
    def __init__(
        self,
        path: Path,
        segment_size=64 * 1024**2,
        max_record=MAX_RECORD,
        index_interval=128,
    ):
        integer(segment_size, 1)
        integer(max_record, PREFIX.size + BODY.size, 4 * 1024**2)
        integer(index_interval, 1)
        self.path = path
        self.segment_size, self.max_record, self.interval = (
            segment_size,
            max_record,
            index_interval,
        )
        self.lock = threading.RLock()
        path.mkdir(parents=True, exist_ok=True)
        files = sorted(path.glob("*.log"))
        if not files:
            files = [path / f"{0:020d}.log"]
        self.segments = [
            Segment(contained(path, p), max_record, index_interval, i == len(files) - 1)
            for i, p in enumerate(files)
        ]
        for left, right in zip(self.segments, self.segments[1:]):
            if left.last + 1 != right.base:
                raise ValueError("Noncontiguous segment sequence")

    @property
    def start(self):
        return self.segments[0].base

    @property
    def end(self):
        return self.segments[-1].last + 1

    def append(self, key, value, headers=None, fsync=False):
        with self.lock:
            record = Record(
                self.end, time.time_ns() // 1_000_000, key, value, headers or {}
            )
            data = encode(record, self.max_record)
            active = self.segments[-1]
            if active.count and active.size + len(data) > self.segment_size:
                active = Segment(
                    self.path / f"{self.end:020d}.log",
                    self.max_record,
                    self.interval,
                    True,
                )
                self.segments.append(active)
                LOG.info("Segment rollover %s", active.path)
            active.append(record, data, fsync)
            return record

    def fetch(self, offset, max_records=500, max_bytes=4 * 1024**2):
        with self.lock:
            integer(offset)
            integer(max_records, 1, 10000)
            integer(max_bytes, 1, 4 * 1024**2)
            if not self.start <= offset <= self.end:
                raise StreamError(
                    "OFFSET_OUT_OF_RANGE",
                    "Offset outside retained log",
                    earliest_offset=self.start,
                    latest_offset=self.end,
                )
            result, size = [], 0
            bases = [s.base for s in self.segments]
            for segment in self.segments[
                max(0, bisect.bisect_right(bases, offset) - 1) :
            ]:
                for record in segment.read(offset):
                    length = len(encode(record, self.max_record))
                    if size + length > max_bytes:
                        if not result:
                            raise StreamError(
                                "MESSAGE_TOO_LARGE", "Increase fetch max_bytes"
                            )
                        return result
                    result.append(record)
                    size += length
                    if len(result) >= max_records:
                        return result
            return result

    def retain(self, retention_ms, retention_bytes=None, now_ms=None):
        now_ms = time.time_ns() // 1_000_000 if now_ms is None else now_ms
        with self.lock:
            total = sum(s.size for s in self.segments)
            while len(self.segments) > 1:
                segment = self.segments[0]
                if segment.timestamp >= now_ms - retention_ms and (
                    retention_bytes is None or total <= retention_bytes
                ):
                    break
                for path in (segment.path, segment.path.with_suffix(".index")):
                    contained(self.path, path).unlink(missing_ok=True)
                total -= segment.size
                self.segments.pop(0)
                LOG.info("Retention removed %s", segment.path)

    def inspect(self):
        with self.lock:
            return [
                dict(
                    base_offset=s.base,
                    last_offset=s.last,
                    bytes=s.size,
                    records=s.count,
                    index_entries=len(s.index),
                )
                for s in self.segments
            ]
