"""Allowlisted, side-effect-free synthetic jobs; never evaluates user code."""
import ctypes
import json
import os
import signal
import sys
import time


def main():
    # Linux: kill this child if the service crashes, including SIGKILL.
    parent = int(sys.argv[3])
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_PDEATHSIG failed")
    if os.getppid() != parent:
        return 1
    spec, attempt = json.loads(sys.argv[1]), int(sys.argv[2])
    if spec["task"] == "flaky" and attempt <= spec["fail_until"]:
        print("synthetic failure", file=sys.stderr)
        return 1
    if spec["task"] == "sleep":
        time.sleep(spec["seconds"])
        result = {"slept": spec["seconds"]}
    else:
        result = sum(i * i for i in range(spec["n"]))
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
