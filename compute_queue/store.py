"""Short SQLite transactions; each operation owns its connection."""
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager


class Conflict(ValueError):
    pass


def validate(data):
    if not isinstance(data, dict) or set(data) - {"task", "n", "seconds", "fail_until", "max_attempts", "timeout"}:
        raise ValueError("unknown fields or non-object body")
    task = data.get("task", "sum_squares")
    if task not in ("sum_squares", "sleep", "flaky"):
        raise ValueError("unknown task")
    values = {"task": task, "n": data.get("n", 100), "seconds": data.get("seconds", 0),
              "fail_until": data.get("fail_until", 0),
              "max_attempts": data.get("max_attempts", 3), "timeout": data.get("timeout", 30)}
    for key, low, high, integer in (("n", 0, 10000000, True), ("seconds", 0, 60, False),
                                    ("fail_until", 0, 5, True), ("max_attempts", 1, 5, True),
                                    ("timeout", .05, 300, False)):
        value = values[key]
        if type(value) not in ((int,) if integer else (int, float)) or not low <= value <= high:
            raise ValueError(f"invalid {key}: expected {low}..{high}")
    return values


class Store:
    def __init__(self, path):
        self.path = str(path)
        with self.db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript('''
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY, parent_id TEXT, spec TEXT NOT NULL,
                  state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                  result TEXT, error TEXT, created REAL NOT NULL, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS attempts (
                  job_id TEXT NOT NULL, number INTEGER NOT NULL, started REAL NOT NULL,
                  finished REAL, error TEXT, pid INTEGER,
                  PRIMARY KEY(job_id, number));
                CREATE INDEX IF NOT EXISTS jobs_state_created ON jobs(state, created);
            ''')

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def submit(self, data, parent=None):
        spec = validate(data)
        job_id = uuid.uuid4().hex
        now = time.time()
        with self.db() as db:
            db.execute("INSERT INTO jobs(id,parent_id,spec,state,created,updated) VALUES(?,?,?,'queued',?,?)",
                       (job_id, parent, json.dumps(spec), now, now))
        return self.get(job_id)

    def get(self, job_id):
        with self.db() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            job = dict(row)
            job["spec"] = json.loads(job["spec"])
            job["result"] = json.loads(job["result"]) if job["result"] is not None else None
            job["history"] = [dict(r) for r in db.execute(
                "SELECT * FROM attempts WHERE job_id=? ORDER BY number", (job_id,))]
            return job

    def list(self, state=None):
        with self.db() as db:
            rows = db.execute("SELECT id FROM jobs WHERE (? IS NULL OR state=?) ORDER BY created LIMIT 1000",
                              (state, state)).fetchall()
        return [self.get(r["id"]) for r in rows]

    def claim(self):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE state='queued' ORDER BY created LIMIT 1").fetchone()
            if row is None:
                return None
            now = time.time()
            db.execute("UPDATE jobs SET state='running', attempts=attempts+1, updated=? WHERE id=?", (now, row["id"]))
            db.execute("INSERT INTO attempts(job_id,number,started) VALUES(?,?,?)", (row["id"], row["attempts"]+1, now))
        return self.get(row["id"])

    def pid(self, job, pid):
        with self.db() as db:
            db.execute("UPDATE attempts SET pid=? WHERE job_id=? AND number=?", (pid, job["id"], job["attempts"]))

    def finish(self, job, result=None, error=None):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            state = db.execute("SELECT state FROM jobs WHERE id=?", (job["id"],)).fetchone()[0]
            if state == "cancelling":
                state, error = "cancelled", "cancelled by request"
            elif error:
                state = "dead" if job["attempts"] >= job["spec"]["max_attempts"] else "queued"
            else:
                state = "succeeded"
            now = time.time()
            db.execute("UPDATE attempts SET finished=?,error=? WHERE job_id=? AND number=?", (now, error, job["id"], job["attempts"]))
            db.execute("UPDATE jobs SET state=?,result=?,error=?,updated=? WHERE id=?",
                       (state, json.dumps(result) if state == "succeeded" else None, error, now, job["id"]))

    def cancel(self, job_id):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            if row[0] not in ("queued", "running", "cancelling"):
                raise Conflict("only queued/running jobs can be cancelled")
            state = "cancelled" if row[0] == "queued" else "cancelling"
            db.execute("UPDATE jobs SET state=?,updated=? WHERE id=?", (state, time.time(), job_id))
        return self.get(job_id)

    def retry(self, job_id):
        job = self.get(job_id)
        if job["state"] not in ("dead", "cancelled"):
            raise Conflict("only dead/cancelled jobs can be retried")
        return self.submit(job["spec"], parent=job_id)

    def recover(self):
        # Called only after acquiring the exclusive service lock.
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            for row in db.execute("SELECT * FROM jobs WHERE state IN ('running','cancelling')").fetchall():
                spec = json.loads(row["spec"])
                state = "cancelled" if row["state"] == "cancelling" else (
                    "dead" if row["attempts"] >= spec["max_attempts"] else "queued")
                now = time.time()
                error = "interrupted by service restart"
                db.execute("UPDATE attempts SET finished=?,error=? WHERE job_id=? AND finished IS NULL", (now, error, row["id"]))
                db.execute("UPDATE jobs SET state=?,error=?,updated=? WHERE id=?", (state, error, now, row["id"]))
