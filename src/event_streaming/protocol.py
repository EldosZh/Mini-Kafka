"""Version one: bounded four-byte length-prefixed JSON, binary fields base64."""

import base64
import json
import struct

from .models import StreamError

MAX_FRAME = 8 * 1024**2


def b64(value):
    return None if value is None else base64.b64encode(value).decode("ascii")


def unb64(value, nullable=False):
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise StreamError("INVALID_REQUEST", "Expected base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except ValueError:
        raise StreamError("INVALID_REQUEST", "Invalid base64") from None


def wire(record):
    return dict(
        offset=record.offset,
        timestamp_ms=record.timestamp_ms,
        key=b64(record.key),
        value=b64(record.value),
        headers={k: b64(v) for k, v in record.headers.items()},
    )


def frame(value):
    data = json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
    if not 0 < len(data) <= MAX_FRAME:
        raise StreamError("MESSAGE_TOO_LARGE", "Frame exceeds limit")
    return struct.pack(">I", len(data)) + data


async def read_frame(reader):
    # TCP может разбить даже длину кадра на несколько пакетов.
    size = struct.unpack(">I", await reader.readexactly(4))[0]
    if not 0 < size <= MAX_FRAME:
        raise StreamError("INVALID_REQUEST", "Invalid frame length")
    return json.loads(await reader.readexactly(size))
