import argparse
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .store import Conflict
from .worker import Service


def make_server(service, host="127.0.0.1", port=8000):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.dispatch()

        def do_POST(self):
            self.dispatch()

        def dispatch(self):
            try:
                url = urlsplit(self.path)
                parts = url.path.strip("/").split("/")
                store = service.store
                status = 200
                if self.command == "GET" and parts == ["health"]:
                    result = {"status": "ok", "workers_alive": sum(t.is_alive() for t in service.threads)}
                elif self.command == "GET" and parts == ["jobs"]:
                    result = store.list(parse_qs(url.query).get("state", [None])[0])
                elif self.command == "GET" and len(parts) == 2 and parts[0] == "jobs":
                    result = store.get(parts[1])
                elif self.command == "POST" and parts == ["jobs"]:
                    if self.headers.get("Transfer-Encoding"):
                        raise ValueError("chunked bodies are not supported")
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= 8192:
                        raise ValueError("body must contain 1..8192 bytes")
                    self.connection.settimeout(5)
                    result = store.submit(json.loads(self.rfile.read(length)))
                    status = 201
                elif self.command == "POST" and len(parts) == 3 and parts[0] == "jobs" and parts[2] in ("cancel", "retry"):
                    result = getattr(store, parts[2])(parts[1])
                    status = 202 if parts[2] == "cancel" else 201
                else:
                    raise KeyError("route not found")
            except KeyError:
                status, result = 404, {"error": "not found"}
            except Conflict as exc:
                status, result = 409, {"error": str(exc)}
            except (ValueError, UnicodeError, TimeoutError) as exc:
                status, result = 400, {"error": str(exc)}
            payload = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return ThreadingHTTPServer((host, port), Handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/queue.sqlite")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    service = Service(args.db, args.workers)
    try:
        server = make_server(service, args.host, args.port)
        service.start()
        def shutdown(*_):
            threading.Thread(target=server.shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        try:
            server.serve_forever(poll_interval=.1)
        finally:
            server.server_close()
    finally:
        service.close()


if __name__ == "__main__":
    main()
