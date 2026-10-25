"""Single-broker group coordinator with generation fencing and persistent offsets."""

import json
import logging
import time
import uuid

from .models import StreamError
from .storage import atomic, contained, integer, name


class Coordinator:
    def __init__(self, registry, session_timeout=10):
        self.registry, self.timeout = registry, session_timeout
        self.path = contained(registry.root, registry.root / "consumer-groups")
        self.path.mkdir(exist_ok=True)
        self.groups = {}
        for path in self.path.glob("*.json"):
            contained(self.path, path)
            group = json.loads(path.read_text())
            name(path.stem)
            self.groups[path.stem] = dict(
                offsets=group["offsets"], generation=group["generation"] + 1, members={}
            )

    def persist(self, group_id):
        group = self.groups[group_id]
        atomic(
            self.path / f"{name(group_id)}.json",
            dict(offsets=group["offsets"], generation=group["generation"]),
        )

    def get(self, group_id):
        name(group_id)
        if group_id not in self.groups:
            raise StreamError("GROUP_NOT_FOUND", "Unknown consumer group")
        return self.groups[group_id]

    def rebalance(self, group_id):
        group = self.get(group_id)
        # Старые участники не смогут закоммитить offset с прошлым поколением.
        group["generation"] += 1
        members = group["members"]
        for member in members.values():
            member["assignment"] = []
        topics = sorted({m["topic"] for m in members.values()})
        for topic in topics:
            if topic not in self.registry.topics:
                continue
            # Сортировка делает распределение одинаковым при том же составе группы.
            eligible = sorted(k for k, m in members.items() if m["topic"] == topic)
            for partition in range(len(self.registry.get(topic)[1])):
                members[eligible[partition % len(eligible)]]["assignment"].append(
                    partition
                )
        self.persist(group_id)
        logging.getLogger(__name__).info(
            "Rebalanced group %s generation %d", group_id, group["generation"]
        )

    def expire(self):
        # Упавший consumer может не отправить LEAVE_GROUP, поэтому проверяем heartbeat.
        now = time.monotonic()
        for group_id, group in self.groups.items():
            expired = [
                k
                for k, m in group["members"].items()
                if now - m["heartbeat"] > self.timeout
            ]
            for member_id in expired:
                del group["members"][member_id]
            if expired:
                self.rebalance(group_id)

    def join(self, group_id, topic):
        name(group_id)
        self.registry.get(topic)
        group = self.groups.setdefault(
            group_id, dict(offsets={}, generation=0, members={})
        )
        member_id = uuid.uuid4().hex
        group["members"][member_id] = dict(
            topic=topic, heartbeat=time.monotonic(), assignment=[]
        )
        self.rebalance(group_id)
        return self.heartbeat(group_id, member_id)

    def heartbeat(self, group_id, member_id):
        group = self.get(group_id)
        if member_id not in group["members"]:
            raise StreamError("UNKNOWN_MEMBER", "Member expired; rejoin required")
        member = group["members"][member_id]
        member["heartbeat"] = time.monotonic()
        return dict(
            member_id=member_id,
            generation_id=group["generation"],
            assignment=member["assignment"],
            session_timeout=self.timeout,
        )

    def leave(self, group_id, member_id):
        group = self.get(group_id)
        if group["members"].pop(member_id, None):
            self.rebalance(group_id)

    def owner(self, group_id, member_id, generation_id, topic, partition):
        integer(generation_id)
        group = self.get(group_id)
        if generation_id != group["generation"]:
            raise StreamError("ILLEGAL_GENERATION", "Group has rebalanced")
        member = group["members"].get(member_id)
        if (
            not member
            or member["topic"] != topic
            or partition not in member["assignment"]
        ):
            raise StreamError("NOT_PARTITION_OWNER", "Member does not own partition")
        return group

    def commit(self, group_id, member_id, generation_id, topic, partition, next_offset):
        log = self.registry.partition(topic, partition)
        integer(next_offset)
        if not log.start <= next_offset <= log.end:
            raise StreamError(
                "OFFSET_OUT_OF_RANGE", "Cannot commit outside retained log"
            )
        group = self.owner(group_id, member_id, generation_id, topic, partition)
        old = dict(group["offsets"])
        group["offsets"][f"{topic}:{partition}"] = next_offset
        try:
            self.persist(group_id)
        except BaseException:
            group["offsets"] = old
            raise

    def describe(self, group_id):
        group = self.get(group_id)
        rows = []
        for topic, (_, logs) in self.registry.topics.items():
            for partition, log in enumerate(logs):
                committed = group["offsets"].get(f"{topic}:{partition}")
                if committed is not None or any(
                    m["topic"] == topic for m in group["members"].values()
                ):
                    rows.append(
                        dict(
                            topic=topic,
                            partition=partition,
                            committed_next_offset=committed,
                            end_offset=log.end,
                            lag=None
                            if committed is None
                            else max(0, log.end - committed),
                        )
                    )
        return dict(
            group_id=group_id,
            generation_id=group["generation"],
            state="stable" if group["members"] else "empty",
            members={
                k: dict(topic=v["topic"], assignment=v["assignment"])
                for k, v in group["members"].items()
            },
            offsets=rows,
        )
