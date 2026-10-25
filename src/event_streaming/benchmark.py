"""Small local-only benchmarks, with exact interpolated sample percentiles."""

import statistics
import time

from .client import Producer


def summary(count, size, started, latencies):
    # Считаем время по монотонным часам: перевод системных часов не повлияет на результат.
    elapsed = time.monotonic() - started
    ordered = sorted(latencies)

    def percentile(p):
        if not ordered:
            return 0
        index = (len(ordered) - 1) * p
        # Если позиция между двумя замерами, берём значение между ними.
        lo = int(index)
        hi = min(lo + 1, len(ordered) - 1)
        return ordered[lo] + (ordered[hi] - ordered[lo]) * (index - lo)

    return dict(
        messages=count,
        seconds=elapsed,
        messages_per_second=count / elapsed,
        mib_per_second=size / 1024**2 / elapsed,
        latency_mean_ms=statistics.mean(latencies) if latencies else 0,
        latency_p50_ms=percentile(0.5),
        latency_p95_ms=percentile(0.95),
        latency_p99_ms=percentile(0.99),
        latency_samples=len(latencies),
    )


def run(mode, host, port, topic, messages=1000, message_size=256, batch_size=100):
    if host not in ("localhost", "127.0.0.1", "::1"):
        raise ValueError("Benchmarks require an explicitly local broker")
    if (
        not 1 <= messages <= 1_000_000
        or not 1 <= batch_size <= 1000
        or not 1 <= message_size <= 512 * 1024
    ):
        raise ValueError("Invalid benchmark bounds")
    latencies, count, size = [], 0, 0
    with Producer(host, port) as client:
        metadata = client.request("DESCRIBE_TOPIC", topic=topic)
        started = time.monotonic()
        if mode == "produce":
            while count < messages:
                # Последняя пачка может быть меньше остальных.
                n = min(batch_size, messages - count)
                tick = time.monotonic()
                client.send_batch(topic, [b"x" * message_size] * n)
                latencies.append((time.monotonic() - tick) * 1000)
                count += n
                size += n * message_size
        else:
            # Читаем до конца, который был известен перед началом замера.
            for row in metadata["offsets"]:
                offset = row["start_offset"]
                while offset < row["end_offset"] and count < messages:
                    tick = time.monotonic()
                    response = client.request(
                        "FETCH",
                        topic=topic,
                        partition=row["partition"],
                        offset=offset,
                        max_records=min(batch_size, messages - count),
                    )
                    latencies.append((time.monotonic() - tick) * 1000)
                    records = response["records"]
                    if not records:
                        break
                    from .protocol import unb64

                    count += len(records)
                    size += sum(len(unb64(r["value"])) for r in records)
                    # Следующий запрос начинается после последней полученной записи.
                    offset = records[-1]["offset"] + 1
        return summary(count, size, started, latencies)
