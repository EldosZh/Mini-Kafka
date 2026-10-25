# Verification record

Verified on 2026-10-25, Windows, CPython 3.12.15. The project also passed its
earlier 39-test suite on the available CPython 3.15.0 interpreter. The final
43-test suite was run on Python 3.12.15.

| Check | Actual result |
|---|---|
| `python -m pytest` | 43 passed in 20.13 seconds |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 19 files already formatted |
| `pip install -e .` | Editable package built and installed successfully |
| `event-stream --help` | Exit 0; broker/topic/group/storage/benchmark and produce/consume commands listed |
| `event-stream --version` | Exit 0; `0.1.0` |
| Actual CLI subprocess workflow | Topic creation, stdin produce, consume, commit, list/describe, storage inspection/dump, metrics, benchmarks, deletion passed |
| Normal broker restart | Acknowledged records retained; offsets continue |
| Committed next-offset resume | Existing analytics group resumes without replaying committed records |
| Multiple real consumer clients | Partitions divided without duplicate assignment; graceful leave redistributes partitions |
| Heartbeat failure | Members without heartbeats expire and remaining member receives partitions |
| Stale generation and wrong owner | Commits rejected with appropriate stable error codes |
| Independent groups | Both groups read the same retained records |
| Incomplete active tail | Only incomplete bytes truncated; valid records and next offset preserved |
| Complete CRC corruption | Startup fails closed and preserves file bytes |
| Closed segment corruption | Startup fails closed without truncation |
| Missing/corrupt derived indexes | Rebuilt from validated log files |
| Retention | Time, size, and periodic retention preserve active segments; old offsets return range errors |
| TCP framing | Bytewise fragmentation and multiple coalesced frames both passed |
| Malformed network clients | Invalid frames, JSON, version, base64, batch entries and disconnected partial frames isolated |
| Limits/concurrency | Oversized records rejected; parallel appends have unique contiguous partition offsets |
| Final smoke restart | 13 order records present after a third broker start |

The initial default `python` executable was an unavailable Windows alias.
An existing interpreter was used to bootstrap the first environment, and a
workspace-local CPython 3.12.15 installation was then used for final verification.
The test process used a workspace-local TEMP/TMP because the default pytest
temporary-directory location was inaccessible. Tests still use `tmp_path` and
dynamic loopback ports; they do not touch user broker storage.

## Actual small benchmark

Full measured output and smoke assertions are in [verification.json](verification.json).
Reproduce with:

```sh
python scripts/verify.py --output docs/verification.json
```

The script creates temporary storage in the current directory, runs an actual
loopback TCP broker, and removes its temporary data on completion. It runs three
broker starts, tests commits/recovery/index rebuild, and benchmarks a separate
three-partition topic. Benchmark settings: 300 records, 256 value bytes per record,
30 records per producer batch, 1 KiB segment target, index interval two records,
OS-buffer acknowledgement mode. These intentionally tiny segments exercise
storage internals; these are smoke measurements rather than optimized performance
claims. Both measurements ran on the workspace filesystem in this session.

| Metric | Producer | Consumer |
|---|---:|---:|
| Records | 300 | 300 |
| Elapsed seconds | 4.219 | 1.515 |
| Records/sec | 71.107 | 198.020 |
| MiB/sec | 0.01736 | 0.04834 |
| Mean request latency, ms | 421.900 | 126.250 |
| P50 request latency, ms | 429.500 | 140.000 |
| P95 request latency, ms | 446.250 | 163.200 |
| P99 request latency, ms | 451.650 | 170.240 |
| Request samples | 10 | 12 |

Producer latencies measure each complete batch request. Consumer latencies measure
each fetch request. Percentiles interpolate sorted measured samples. Neither
metric measures downstream application processing latency.

## Scope and publication

Replication, idempotent producers, transactions, compression, TLS, authentication,
and HTTP metrics are intentionally future work. The implemented metrics are
available over the broker TCP protocol. This is an educational single-broker
implementation and is not compatible with the Apache Kafka protocol.

Source, tests, examples, README and package metadata were scanned for unfinished
markers, persisted unsafe serialization, personal absolute paths, and common
secret patterns; no matches were found. Generated environments, interpreter,
caches, test directories, and broker/benchmark data are excluded by `.gitignore`.
No commits or pushes were created during the initial implementation verification.

The supplied folder initially inherited an unrelated Git repository from a parent
directory. A separate local repository was subsequently initialized inside the
project for publication; the parent repository was not modified. Review status
from this project directory and add only this project's files:

```sh
git status --short
```

The README contains the suggested logical commit sequence. The requested GitHub
repository name is `event-streaming-platform`; the existing workspace folder name
was preserved.
