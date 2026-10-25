import struct
from concurrent.futures import ThreadPoolExecutor

import pytest

from event_streaming.cli import size
from event_streaming.models import Record, StreamError
from event_streaming.registry import Registry
from event_streaming.storage import PREFIX, PartitionLog, decode, encode, name


def test_binary_roundtrip_crc():
    record = Record(42, 1234, b"\x00\xff", bytes(range(256)), {"trace": b"\xfe"})
    assert decode(encode(record)) == record
    assert decode(encode(Record(0, 0, None, b""))).key is None
    packet = bytearray(encode(record))
    packet[-1] ^= 1
    with pytest.raises(ValueError, match="CRC"):
        decode(packet)
    with pytest.raises(StreamError, match="limit"):
        encode(record, 50)


@pytest.mark.parametrize(
    "value", ["../x", "/tmp", "..", "", "a/b", "a\\b", "x" * 129, None]
)
def test_invalid_names(value):
    with pytest.raises(StreamError):
        name(value)


@pytest.mark.parametrize(
    "text,expected", [("1KB", 1024), ("64MB", 64 * 1024**2), ("1GB", 1024**3), ("8", 8)]
)
def test_sizes(text, expected):
    assert size(text) == expected


def test_segments_indexes_restart_recovery(tmp_path):
    path = tmp_path / "partition"
    log = PartitionLog(path, segment_size=180, index_interval=2)
    for i in range(12):
        assert log.append(b"key", str(i).encode(), fsync=True).offset == i
    assert len(log.segments) > 1
    assert [r.offset for r in log.fetch(3)] == list(range(3, 12))
    for index in path.glob("*.index"):
        index.write_bytes(b"broken")
    last = log.segments[-1].path
    original = last.stat().st_size
    with last.open("ab") as file:
        file.write(encode(Record(12, 0, None, b"tail"))[:-2])
    recovered = PartitionLog(path, segment_size=180, index_interval=2)
    assert last.stat().st_size == original
    assert recovered.end == 12
    assert recovered.append(None, b"next").offset == 12
    assert all(p.stat().st_size % 16 == 0 for p in path.glob("*.index"))
    assert [r.value for r in recovered.fetch(0)][:12] == [
        str(i).encode() for i in range(12)
    ]


@pytest.mark.parametrize(
    "tail", [b"E", b"ESTR\x01", PREFIX.pack(b"ESTR", 1, 50, 0) + b"x"]
)
def test_partial_tails(tmp_path, tail):
    log = PartitionLog(tmp_path)
    log.append(None, b"valid")
    with log.segments[-1].path.open("ab") as file:
        file.write(tail)
    assert PartitionLog(tmp_path).fetch(0)[0].value == b"valid"


def test_crc_corruption_fails_closed(tmp_path):
    log = PartitionLog(tmp_path)
    log.append(None, b"valid")
    path = log.segments[0].path
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    with pytest.raises(ValueError, match="Corrupted"):
        PartitionLog(tmp_path)
    assert path.read_bytes() == data


def test_retention_and_range(tmp_path):
    log = PartitionLog(tmp_path, segment_size=50)
    for _ in range(5):
        log.append(None, b"x" * 10)
    log.retain(0, now_ms=10**15)
    assert len(log.segments) == 1
    assert log.start == 4 and log.end == 5
    with pytest.raises(StreamError) as error:
        log.fetch(0)
    assert error.value.code == "OFFSET_OUT_OF_RANGE"
    assert error.value.details == dict(earliest_offset=4, latest_offset=5)
    assert PartitionLog(tmp_path).append(None, b"next").offset == 5


def test_parallel_storage_appends(tmp_path):
    log = PartitionLog(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        offsets = list(
            pool.map(lambda i: log.append(None, str(i).encode()).offset, range(100))
        )
    assert sorted(offsets) == list(range(100))
    assert len(log.fetch(0)) == 100


def test_registry_lock_and_safe_deletion(tmp_path):
    registry = Registry(tmp_path)
    try:
        registry.create("orders", 3)
        with pytest.raises(RuntimeError, match="already owned"):
            Registry(tmp_path)
        with pytest.raises(StreamError):
            registry.delete("../outside")
        registry.delete("orders")
        assert not (tmp_path / "topics" / "orders").exists()
    finally:
        registry.close()


def test_bad_record_lengths():
    with pytest.raises((ValueError, struct.error)):
        decode(b"bad")


def test_size_retention_keeps_active(tmp_path):
    log = PartitionLog(tmp_path, segment_size=50)
    for _ in range(4):
        log.append(None, b"payload")
    active = log.segments[-1].path
    log.retain(10**15, retention_bytes=1, now_ms=0)
    assert len(log.segments) == 1
    assert active.exists() and log.start == 3 and log.end == 4


def test_closed_segment_corruption_is_not_repaired(tmp_path):
    log = PartitionLog(tmp_path, segment_size=50)
    log.append(None, b"first")
    log.append(None, b"second")
    path = log.segments[0].path
    data = path.read_bytes()[:-1]
    path.write_bytes(data)
    with pytest.raises(ValueError, match="Corrupted"):
        PartitionLog(tmp_path)
    assert path.read_bytes() == data
