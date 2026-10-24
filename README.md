# Compute Job Queue

## Question

Can long-running scientific jobs be queued, tracked and retried reliably?

## Approach

A Linux Python service exposes an HTTP API over a persistent SQLite queue.
Worker threads atomically claim jobs; each attempt runs in a separate child
process. Only synthetic `sum_squares`, `sleep` and `flaky` tasks are accepted.
No external data, shell commands or uploaded code are executed.

Jobs move through `queued → running → succeeded`. Failed or timed-out attempts
return to `queued` until their attempt budget is exhausted, then enter `dead`
(the dead-letter view). Running cancellation first enters `cancelling`; it becomes
`cancelled` only after the child has exited and been reaped. Queued cancellation
starts no attempt. Retry creates a new job with a `parent_id`, preserving history.

SQLite stores inputs, results, attempt counts, child PIDs, errors and timestamps.
An exclusive Linux file lock permits one service per database. On restart,
interrupted attempts consume their existing attempt slot and are requeued if
budget remains; otherwise they become dead. Interrupted cancellation becomes
cancelled. Linux parent-death signals stop orphan computation children.

Atomic claims use SQLite [`BEGIN IMMEDIATE`](https://www.sqlite.org/lang_transaction.html).
The concrete scope and acceptance contract are in [docs/SCOPE.md](docs/SCOPE.md).

### Run locally

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/compute-queue --db data/queue.sqlite --workers 3
```

In another terminal:

```sh
curl -s http://127.0.0.1:8000/health
curl -s -H 'Content-Type: application/json' -d '{"task":"sum_squares","n":100}' http://127.0.0.1:8000/jobs
# Substitute the returned id below.
curl -s http://127.0.0.1:8000/jobs/ID
curl -s -X POST http://127.0.0.1:8000/jobs/ID/cancel
curl -s -X POST http://127.0.0.1:8000/jobs/ID/retry
curl -s 'http://127.0.0.1:8000/jobs?state=dead'
```

`POST /jobs` returns 201; cancellation returns 202; explicit retry returns 201.
Invalid inputs return 400, missing jobs/routes 404 and invalid state transitions
409. `GET /jobs` returns at most 1000 jobs in creation order, optionally filtered
by state. `GET /jobs/ID` includes the full attempt history. Health reports the
number of live worker threads.

| Input | Default | Accepted range / values |
| --- | --- | --- |
| `task` | `sum_squares` | `sum_squares`, `sleep`, `flaky` |
| `n` | 100 | integer 0..10000000; sum of i² for 0 ≤ i < n |
| `seconds` | 0 | 0..60; used by `sleep` |
| `fail_until` | 0 | integer 0..5; `flaky` fails through this attempt number |
| `max_attempts` | 3 | integer 1..5 |
| `timeout` | 30 | 0.05..300 seconds per attempt, including child startup |

Retries are immediate and use the same inputs. A new retry job resets the attempt
number, so retrying a deterministic failing task can fail again. Unknown fields,
booleans in numeric fields, non-finite values and oversized bodies are rejected.

### Container

```sh
podman build -t compute-job-queue .
podman run --rm -p 127.0.0.1:8000:8000 -v queue-data:/data compute-job-queue
```

The image runs as a non-root user. Keep `/data` on a persistent local volume.
GitHub Actions tests Python 3.11 and 3.12 and builds/starts the Docker image.

### Verification

```sh
.venv/bin/python -m pytest -v -s --junitxml=test-results.xml
```

The tests use real SQLite files, actual child processes, HTTP sockets, concurrent
submitters and a service killed with SIGKILL. They cover successful calculation,
error budgets, timeouts, queued/running cancellation, explicit retries, exclusive
database ownership, recovery with/without remaining budget and invalid requests
without mutations. There are no mocked queue or task execution paths.

## Results

Measured locally on 2026-10-24 with Python 3.11.15:

- 19/19 pytest cases passed in 8.03 seconds.
- Concurrent workload: 12/12 succeeded, 12 unique job IDs, 12 attempt records,
  peak 3 simultaneous attempts with 3 configured workers.
- `sum_squares(n=100)` returned 328350 through HTTP and survived service restart.
- A task failing twice succeeded on attempt 3/3; persistent failure became dead
  after 2/2 attempts. Both 0.1-second deadlines produced recorded timeout errors.
- Queued cancellation used 0 attempts. Running cancellation and both timed-out
  child processes were verified absent after completion.
- SIGKILL recovery succeeded with a 2-attempt budget and became dead with a
  1-attempt budget. Invalid-payload cases each left 0 jobs.
- Podman build and live container HTTP execution passed; after container restart,
  the same job still returned 328350 with state `succeeded`.

Machine-readable local evidence: [docs/validation.json](docs/validation.json).
CI results are available in the repository's Actions tab.

## Limitations

- Distributed execution, authentication/TLS, production deployment, resource
  quotas, retry backoff, scheduling, priorities and arbitrary task plugins:
  **not done**. Bind to loopback; this is a trusted local demonstration service.
- Exactly-once execution/side effects: **not done**. Recovery is at-least-once,
  bounded by the attempt budget. Built-in tasks are side-effect-free.
- Throughput/load benchmarks, database retention, pagination, network-filesystem
  support and hardware/power-loss durability tests: **not done**. The concurrency
  test demonstrates coordination, not production performance.
- Non-Linux support: **not done**; service locking and parent-death signals depend
  on Linux. Timeout/cancellation observation uses polling and is not a real-time
  deadline guarantee. FIFO claims do not imply FIFO completion.
- External scientific datasets and a real scientific workload: **not done**;
  all computations and injected failures use synthetic inputs.
