"""Educational event streaming; not an Apache Kafka protocol implementation."""

from .client import Consumer, Producer
from .models import Metadata, Record, StreamError

__version__ = "0.1.0"
__all__ = ["Consumer", "Producer", "Metadata", "Record", "StreamError"]
