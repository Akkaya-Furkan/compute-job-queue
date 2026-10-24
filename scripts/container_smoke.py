"""Run against a freshly started container; leave a job for restart verification."""
import json
import sys
import time
from urllib.request import Request, urlopen

base = sys.argv[1]

def call(path, body=None):
    req = Request(base + path, data=json.dumps(body).encode() if body is not None else None,
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=5) as response:
        return json.load(response)

if len(sys.argv) == 3:
    job = call("/jobs/" + sys.argv[2])
    assert job["state"] == "succeeded" and job["result"] == sum(i*i for i in range(100))
    print(json.dumps({"restart_result": job["result"], "id": job["id"], "state": job["state"]}))
else:
    deadline = time.monotonic() + 15
    while True:
        try:
            health = call("/health")
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(.1)
    assert health["workers_alive"] == 2
    job = call("/jobs", {"n": 100})
    while time.monotonic() < deadline:
        job = call("/jobs/" + job["id"])
        if job["state"] == "succeeded":
            break
        time.sleep(.02)
    assert job["result"] == sum(i*i for i in range(100))
    print(json.dumps({"workers_alive": health["workers_alive"], "result": job["result"], "id": job["id"], "state": job["state"]}))
