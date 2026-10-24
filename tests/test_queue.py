import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from compute_queue.api import make_server
from compute_queue.store import Store
from compute_queue.worker import Service


def wait(store, job_id, states=("succeeded", "dead", "cancelled"), timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = store.get(job_id)
        if job["state"] in states:
            return job
        time.sleep(.01)
    pytest.fail(f"job did not reach {states}: {store.get(job_id)}")


@contextmanager
def running(path, workers=2):
    service = Service(path, workers).start()
    try:
        yield service
    finally:
        service.close()


@contextmanager
def http_service(path):
    with running(path) as service:
        server = make_server(service, port=0)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield service, f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


def request(base, path, data=None):
    req = Request(base + path, data=json.dumps(data).encode() if data is not None else None,
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=5) as response:
            return response.status, json.load(response)
    except HTTPError as response:
        return response.code, json.load(response)


def test_http_result_survives_restart(tmp_path):
    path = tmp_path / "q.db"
    with http_service(path) as (service, base):
        assert request(base, "/health")[1]["workers_alive"] == 2
        status, job = request(base, "/jobs", {"task": "sum_squares", "n": 100})
        assert status == 201
        finished = wait(service.store, job["id"])
        assert finished["state"] == "succeeded"
        assert finished["result"] == 328350
        assert request(base, f'/jobs/{job["id"]}')[1] == finished
    with http_service(path) as (_, base):
        assert request(base, f'/jobs/{job["id"]}')[1] == finished


def test_failure_retry_and_dead_letter(tmp_path):
    with running(tmp_path / "q.db") as service:
        flaky = service.store.submit({"task": "flaky", "fail_until": 2, "max_attempts": 3})
        dead = service.store.submit({"task": "flaky", "fail_until": 5, "max_attempts": 2})
        flaky = wait(service.store, flaky["id"])
        dead = wait(service.store, dead["id"])
        assert (flaky["state"], flaky["attempts"]) == ("succeeded", 3)
        assert (dead["state"], dead["attempts"]) == ("dead", 2)
        assert [h["error"] for h in flaky["history"]] == ["synthetic failure", "synthetic failure", None]
        assert [j["id"] for j in service.store.list("dead")] == [dead["id"]]


def test_timeout_kills_process_and_records_each_attempt(tmp_path):
    with running(tmp_path / "q.db", 1) as service:
        job = service.store.submit({"task": "sleep", "seconds": 2, "timeout": .1, "max_attempts": 2})
        job = wait(service.store, job["id"])
        assert (job["state"], job["attempts"]) == ("dead", 2)
        assert [h["error"] for h in job["history"]] == ["attempt timed out"] * 2
        for attempt in job["history"]:
            with pytest.raises(ProcessLookupError):
                os.kill(attempt["pid"], 0)


def test_http_running_cancel_and_retry(tmp_path):
    with http_service(tmp_path / "q.db") as (service, base):
        _, job = request(base, "/jobs", {"task": "sleep", "seconds": 5})
        job = wait(service.store, job["id"], ("running",))
        assert request(base, f'/jobs/{job["id"]}/retry', {})[0] == 409
        assert request(base, f'/jobs/{job["id"]}/cancel', {})[0] == 202
        cancelled = wait(service.store, job["id"])
        assert cancelled["state"] == "cancelled"
        with pytest.raises(ProcessLookupError):
            os.kill(cancelled["history"][0]["pid"], 0)
        status, child = request(base, f'/jobs/{job["id"]}/retry', {})
        assert status == 201
        assert child["parent_id"] == job["id"] and child["id"] != job["id"]
        service.store.cancel(child["id"])


def test_queued_cancel_and_rejected_terminal_actions(tmp_path):
    service = Service(tmp_path / "q.db", 1)
    try:
        job = service.store.submit({})
        cancelled = service.store.cancel(job["id"])
        assert cancelled["state"] == "cancelled" and cancelled["attempts"] == 0
        assert cancelled["history"] == []
        service.start()
        good = wait(service.store, service.store.submit({})["id"])
        from compute_queue.store import Conflict
        with pytest.raises(Conflict):
            service.store.cancel(good["id"])
        with pytest.raises(Conflict):
            service.store.retry(good["id"])
    finally:
        service.close()


def test_dead_retry_preserves_original(tmp_path):
    with running(tmp_path / "q.db") as service:
        job = wait(service.store, service.store.submit({"task": "flaky", "fail_until": 5, "max_attempts": 1})["id"])
        child = service.store.retry(job["id"])
        assert child["parent_id"] == job["id"]
        assert child["id"] != job["id"]
        assert service.store.get(job["id"]) == job
        assert wait(service.store, child["id"])["state"] == "dead"


def test_concurrent_claims_and_execution(tmp_path):
    service = Service(tmp_path / "q.db", 3)
    try:
        with ThreadPoolExecutor(max_workers=6) as pool:
            jobs = list(pool.map(lambda _: service.store.submit({"task": "sleep", "seconds": .2}), range(12)))
        service.start()
        finished = [wait(service.store, job["id"]) for job in jobs]
        assert len({j["id"] for j in finished}) == 12
        assert all(j["state"] == "succeeded" and j["attempts"] == 1 for j in finished)
        intervals = [h for j in finished for h in j["history"]]
        assert len(intervals) == 12
        events = sorted([(h["started"], 1) for h in intervals] + [(h["finished"], -1) for h in intervals])
        active = peak = 0
        for _, change in events:
            active += change
            peak = max(peak, active)
        assert 2 <= peak <= 3
        print(f"concurrency: jobs={len(finished)}, unique_ids={len({j['id'] for j in finished})}, attempts={len(intervals)}, peak={peak}")
    finally:
        service.close()


def test_queued_work_survives_restart_and_exclusive_owner(tmp_path):
    path = tmp_path / "q.db"
    service = Service(path)
    try:
        job = service.store.submit({})
        with pytest.raises(RuntimeError, match="already owned"):
            Service(path)
    finally:
        service.close()
    with running(path) as service:
        assert wait(service.store, job["id"])["state"] == "succeeded"


@pytest.mark.parametrize("budget,expected", [(2, "succeeded"), (1, "dead")])
def test_real_service_crash_recovery(tmp_path, budget, expected):
    path = tmp_path / "q.db"
    store = Store(path)
    job = store.submit({"task": "sleep", "seconds": .4, "max_attempts": budget})
    # A separate service is killed while its first child is executing.
    proc = subprocess.Popen([sys.executable, "-m", "compute_queue.api", "--db", str(path), "--port", "0", "--workers", "1"])
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            current = store.get(job["id"])
            if current["history"] and current["history"][0]["pid"]:
                break
            time.sleep(.005)
        else:
            pytest.fail("child never launched")
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    with running(path, 1) as service:
        recovered = wait(service.store, job["id"])
        assert recovered["state"] == expected
        assert recovered["attempts"] == budget
        assert recovered["history"][0]["error"] == "interrupted by service restart"
        assert all(h["finished"] is not None for h in recovered["history"])


@pytest.mark.parametrize("payload", [[], {"task": "shell"}, {"n": -1}, {"timeout": 0},
                                      {"max_attempts": True}, {"seconds": float("nan")},
                                      {"n": 1.5}, {"unexpected": 1}])
def test_invalid_http_payload_has_no_mutation(tmp_path, payload):
    with http_service(tmp_path / "q.db") as (service, base):
        assert request(base, "/jobs", payload)[0] == 400
        assert service.store.list() == []


def test_http_malformed_missing_and_not_found(tmp_path):
    with http_service(tmp_path / "q.db") as (service, base):
        assert request(base, "/jobs/missing")[0] == 404
        assert request(base, "/jobs/missing/cancel", {})[0] == 404
        for body in (b"{", b"", b"x" * 8193):
            req = Request(base + "/jobs", data=body)
            with pytest.raises(HTTPError) as exc:
                urlopen(req, timeout=5)
            assert exc.value.code == 400
        assert service.store.list() == []
