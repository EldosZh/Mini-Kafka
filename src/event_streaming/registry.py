"""Topic metadata, safe deletion, and exclusive broker directory ownership."""

import json
import logging
import os
import shutil
import time
from pathlib import Path

from .models import StreamError
from .storage import PartitionLog, atomic, contained, integer, name


class Registry:
    def __init__(self, root, **log_config):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_file = (self.root / ".broker.lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                self.lock_file.seek(0)
                self.lock_file.write(b"0")
                self.lock_file.flush()
                self.lock_file.seek(0)
                msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock_file.close()
            raise RuntimeError(
                "Data directory is already owned by another broker"
            ) from None
        self.config, self.topics = log_config, {}
        self.topics_path = contained(self.root, self.root / "topics")
        self.topics_path.mkdir(exist_ok=True)
        try:
            for path in self.topics_path.iterdir():
                contained(self.topics_path, path)
                if path.is_dir() and (path / "topic.json").exists():
                    metadata = json.loads((path / "topic.json").read_text())
                    if name(metadata["name"]) != path.name:
                        raise ValueError("Topic metadata does not match directory")
                    self._load(metadata)
        except BaseException:
            self.close()
            raise

    def close(self):
        self.lock_file.close()

    def _load(self, metadata):
        count = integer(metadata["partitions"], 1, 1024)
        path = contained(self.topics_path, self.topics_path / name(metadata["name"]))
        logs = [
            PartitionLog(contained(path, path / f"partition-{p:04d}"), **self.config)
            for p in range(count)
        ]
        self.topics[metadata["name"]] = (metadata, logs)

    def create(self, topic, partitions=1, retention_ms=604800000, retention_bytes=None):
        name(topic)
        integer(partitions, 1, 1024)
        integer(retention_ms)
        if retention_bytes is not None:
            integer(retention_bytes, 1)
        if topic in self.topics:
            raise StreamError("TOPIC_EXISTS", "Topic already exists")
        metadata = dict(
            name=topic,
            partitions=partitions,
            retention_ms=retention_ms,
            retention_bytes=retention_bytes,
            created_ms=time.time_ns() // 1_000_000,
        )
        # Publish metadata only once all partition directories are initialized.
        self._load(metadata)
        try:
            atomic(self.topics_path / topic / "topic.json", metadata)
        except BaseException:
            del self.topics[topic]
            raise
        logging.getLogger(__name__).info("Created topic %s", topic)
        return self.describe(topic)

    def get(self, topic):
        name(topic)
        if topic not in self.topics:
            raise StreamError("UNKNOWN_TOPIC", "Topic does not exist")
        return self.topics[topic]

    def partition(self, topic, partition):
        logs = self.get(topic)[1]
        integer(partition)
        if partition >= len(logs):
            raise StreamError("UNKNOWN_PARTITION", "Partition does not exist")
        return logs[partition]

    def describe(self, topic):
        metadata, logs = self.get(topic)
        return dict(
            **metadata,
            offsets=[
                dict(
                    partition=i,
                    start_offset=p.start,
                    end_offset=p.end,
                    messages=p.end - p.start,
                )
                for i, p in enumerate(logs)
            ],
        )

    def delete(self, topic):
        self.get(topic)
        path = contained(self.topics_path, self.topics_path / topic)
        # Check every descendant before recursive removal; reject links/junction escapes.
        for descendant in path.rglob("*"):
            contained(path, descendant)
        shutil.rmtree(path)
        del self.topics[topic]
        logging.getLogger(__name__).info("Deleted topic %s", topic)

    def retain(self):
        for metadata, logs in list(self.topics.values()):
            for log in logs:
                log.retain(metadata["retention_ms"], metadata["retention_bytes"])
