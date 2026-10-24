# One-day scope — 2026-10-24

## Question
Can long-running scientific jobs be queued, tracked and retried reliably?

## Deliverable
A single-host, single-service Python HTTP queue backed by SQLite. Workers claim
jobs atomically and execute only built-in synthetic tasks in child processes.
Expose submission, status/listing, cancellation, explicit retry and a dead-letter
view. Persist inputs, results, attempts, errors and timestamps. Recover interrupted
attempts at service restart. No external dataset or arbitrary executable input.

## Acceptance criteria
1. An HTTP-submitted sum of squares for n=100 returns exactly 328350; status and
   stored result survive a service restart.
2. A synthetic task failing its first 2 attempts succeeds on attempt 3 with a
   budget of 3; a permanently failing task reaches `dead` after exactly 2 attempts.
3. A task sleeping 2 seconds with a 0.1-second attempt deadline reaches `dead`
   after 2 timed-out attempts; attempt errors are persisted.
4. Both queued and running cancellation terminate in `cancelled`; the running
   child's PID no longer exists before cancellation is reported complete.
5. Explicit retry creates a new job linked to the dead/cancelled original;
   a running or successful job cannot be retried.
6. With 3 workers, 12 submitted jobs finish once each (12 unique IDs, 12 attempt
   records), at least 2 attempt intervals overlap and no more than 3 overlap.
7. Restart preserves queued work and recovers an interrupted attempt within its
   attempt budget; exhausted interrupted jobs become dead. A second service
   cannot open the same database concurrently.
8. Invalid HTTP payloads create 0 jobs. The complete pytest suite passes locally
   and in GitHub Actions. Include a Dockerfile and try a local Podman build/run;
   report `not verified` if the runtime prevents verification.

## Boundaries
Linux/Python 3.11+, local SQLite filesystem, one service with configurable worker
threads and one subprocess per active job. At-least-once execution on restart;
not exactly-once side effects. No distributed workers, authentication, scheduler,
priority, arbitrary user code, production deployment or throughput claim.
