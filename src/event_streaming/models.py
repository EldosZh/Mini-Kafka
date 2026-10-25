"""Public record models and stable service errors."""

from dataclasses import dataclass, field


class StreamError(Exception):
    def __init__(self, code: str, message: str, **details):
        super().__init__(message)
        self.code = code
        self.details = details


@dataclass(frozen=True)
class Record:
    offset: int
    timestamp_ms: int
    key: bytes | None
    value: bytes
    headers: dict[str, bytes] = field(default_factory=dict)
    topic: str = ""
    partition: int = 0
    generation_id: int | None = None


@dataclass(frozen=True)
class Metadata:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int
