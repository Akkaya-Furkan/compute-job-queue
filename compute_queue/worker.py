import fcntl
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from .store import Store


class Service:
    def __init__(self, path, workers=2):
        if not 1 <= workers <= 16:
            raise ValueError("workers must be 1..16")
        path = Path(path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = open(str(path) + ".lock", "a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise RuntimeError("database already owned by another service") from None
        self.store = Store(path)
        self.store.recover()
        self.stop = threading.Event()
        self.threads = [threading.Thread(target=self.loop, name=f"worker-{i}") for i in range(workers)]

    def start(self):
        for thread in self.threads:
            thread.start()
        return self

    def close(self):
        self.stop.set()
        for thread in self.threads:
            if thread.ident is not None:
                thread.join()
        self.lock.close()

    def loop(self):
        while not self.stop.is_set():
            job = self.store.claim()
            if job is None:
                self.stop.wait(.02)
                continue
            self.execute(job)

    def execute(self, job):
        proc = None
        try:
            proc = subprocess.Popen([sys.executable, "-m", "compute_queue.taskrunner",
                                     json.dumps(job["spec"]), str(job["attempts"]), str(os.getpid())],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.store.pid(job, proc.pid)
            deadline = time.monotonic() + job["spec"]["timeout"]
            error = None
            while proc.poll() is None:
                if self.stop.is_set():
                    return  # Leave the persisted attempt for restart recovery.
                if self.store.get(job["id"])["state"] == "cancelling":
                    error = "cancelled by request"
                    break
                if time.monotonic() >= deadline:
                    error = "attempt timed out"
                    break
                self.stop.wait(.01)
            if error:
                proc.kill()
            out, err = proc.communicate()
            if error is None and proc.returncode:
                error = err.decode(errors="replace").strip() or f"exit {proc.returncode}"
            result = json.loads(out) if error is None else None
            self.store.finish(job, result, error)
        except Exception as exc:
            self.store.finish(job, error=f"worker error: {type(exc).__name__}: {exc}")
        finally:
            if proc is not None:
                if proc.poll() is None:
                    proc.kill()
                proc.communicate()
