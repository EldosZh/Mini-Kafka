"""Administration over the broker protocol; JSON output contains real data."""

import asyncio
import json
import logging
import re
import signal
import sys
from pathlib import Path

import typer

from . import __version__
from .benchmark import run
from .broker import Broker
from .client import Connection, Consumer, Producer

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
broker_app = typer.Typer()
topic_app = typer.Typer()
group_app = typer.Typer()
storage_app = typer.Typer()
benchmark_app = typer.Typer()
app.add_typer(broker_app, name="broker")
app.add_typer(topic_app, name="topic")
app.add_typer(group_app, name="group")
app.add_typer(storage_app, name="storage")
app.add_typer(benchmark_app, name="benchmark")


def size(value: str):
    match = re.fullmatch(r"(\d+)(B|KB|MB|GB)?", value.upper())
    if not match or int(match[1]) == 0:
        raise typer.BadParameter("Use a positive size such as 1KB or 64MB")
    return (
        int(match[1]) * 1024 ** {None: 0, "B": 0, "KB": 1, "MB": 2, "GB": 3}[match[2]]
    )


def output(value):
    typer.echo(json.dumps(value, indent=2))


def request(host, port, operation, **payload):
    with Connection(host, port) as client:
        output(client.request(operation, **payload))


@app.callback(invoke_without_command=True)
def main(version: bool = typer.Option(False, "--version", is_eager=True)):
    if version:
        typer.echo(__version__)
        raise typer.Exit()


@broker_app.command("start")
def start(
    host: str = "127.0.0.1",
    port: int = 9092,
    data_dir: Path = Path("broker-data"),
    segment_size: str = "64MB",
    max_record_size: str = "1MB",
    index_interval: int = 128,
    retention_hours: float = 168,
    retention_interval: float = 30,
    session_timeout: float = 10,
    log_level: str = "INFO",
):
    if (
        index_interval < 1
        or session_timeout <= 0
        or retention_interval <= 0
        or retention_hours < 0
    ):
        raise typer.BadParameter("Invalid broker configuration")
    logging.basicConfig(
        level=log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    async def serve():
        service = Broker(
            data_dir,
            host,
            port,
            segment_size=size(segment_size),
            max_record=size(max_record_size),
            index_interval=index_interval,
            retention_interval=retention_interval,
            session_timeout=session_timeout,
            default_retention_ms=int(retention_hours * 3600000),
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
        try:
            await service.start()
            await stop.wait()
        finally:
            await service.close()

    asyncio.run(serve())


@broker_app.command("info")
def info(host: str = "127.0.0.1", port: int = 9092):
    request(host, port, "BROKER_INFO")


@broker_app.command("metrics")
def metrics(host: str = "127.0.0.1", port: int = 9092):
    request(host, port, "METRICS")


@topic_app.command("create")
def create(
    topic: str,
    partitions: int = 1,
    retention_hours: float | None = None,
    retention_bytes: str | None = None,
    host: str = "127.0.0.1",
    port: int = 9092,
):
    payload = {}
    if retention_hours is not None:
        payload["retention_ms"] = int(retention_hours * 3600000)
    request(
        host,
        port,
        "CREATE_TOPIC",
        topic=topic,
        partitions=partitions,
        retention_bytes=None if retention_bytes is None else size(retention_bytes),
        **payload,
    )


@topic_app.command("list")
def list_topics(host: str = "127.0.0.1", port: int = 9092):
    request(host, port, "LIST_TOPICS")


@topic_app.command("describe")
def describe(topic: str, host: str = "127.0.0.1", port: int = 9092):
    request(host, port, "DESCRIBE_TOPIC", topic=topic)


@topic_app.command("delete")
def delete(
    topic: str,
    yes: bool = typer.Option(False, "--yes"),
    host: str = "127.0.0.1",
    port: int = 9092,
):
    if yes or typer.confirm(f"Delete topic {topic} and all its records?"):
        request(host, port, "DELETE_TOPIC", topic=topic)


@app.command()
def produce(
    topic: str,
    value: str | None = None,
    key: str | None = None,
    partition: int | None = None,
    stdin: bool = False,
    acks: str = "1",
    host: str = "127.0.0.1",
    port: int = 9092,
):
    if not stdin and value is None:
        raise typer.BadParameter("Provide --value or --stdin")
    with Producer(host, port) as producer:
        if stdin:
            while line := sys.stdin.buffer.readline(1024 * 1024 + 1):
                if len(line) > 1024 * 1024:
                    raise typer.BadParameter("Input line exceeds record limit")
                record = producer.send(
                    topic,
                    line.rstrip(b"\r\n"),
                    None if key is None else key.encode(),
                    partition,
                    acks=acks,
                )
                output(vars(record))
        else:
            output(
                vars(
                    producer.send(
                        topic,
                        value.encode(),
                        None if key is None else key.encode(),
                        partition,
                        acks=acks,
                    )
                )
            )


@app.command()
def consume(
    topic: str,
    group: str = "default",
    from_beginning: bool = False,
    limit: int = 0,
    max_records: int = 500,
    max_bytes: str = "4MB",
    host: str = "127.0.0.1",
    port: int = 9092,
):
    with Consumer(
        f"{host}:{port}", topic, group, from_beginning, max_records, size(max_bytes)
    ) as consumer:
        for i, record in enumerate(consumer):
            typer.echo(
                f"[{record.topic}:{record.partition}:{record.offset}] {record.value.decode('utf8', errors='backslashreplace')}"
            )
            consumer.commit(record)
            if limit and i + 1 >= limit:
                break


@group_app.command("list")
def list_groups(host: str = "127.0.0.1", port: int = 9092):
    request(host, port, "LIST_GROUPS")


@group_app.command("describe")
def describe_group(group: str, host: str = "127.0.0.1", port: int = 9092):
    request(host, port, "DESCRIBE_GROUP", group_id=group)


@storage_app.command("inspect")
def inspect(topic: str, partition: int = 0, host: str = "127.0.0.1", port: int = 9092):
    request(host, port, "INSPECT", topic=topic, partition=partition)


@storage_app.command("dump")
def dump(
    topic: str,
    partition: int = 0,
    limit: int = 20,
    offset: int | None = None,
    show_values: bool = False,
    host: str = "127.0.0.1",
    port: int = 9092,
):
    payload = dict(
        topic=topic, partition=partition, limit=limit, show_values=show_values
    )
    if offset is not None:
        payload["offset"] = offset
    request(host, port, "DUMP", **payload)


@benchmark_app.command("produce")
def benchmark_produce(
    topic: str = "benchmark",
    messages: int = 1000,
    message_size: int = 256,
    batch_size: int = 100,
    host: str = "127.0.0.1",
    port: int = 9092,
):
    output(run("produce", host, port, topic, messages, message_size, batch_size))


@benchmark_app.command("consume")
def benchmark_consume(
    topic: str = "benchmark",
    messages: int = 1000,
    batch_size: int = 100,
    host: str = "127.0.0.1",
    port: int = 9092,
):
    output(run("consume", host, port, topic, messages, batch_size=batch_size))
