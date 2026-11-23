import json
import os
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from kafka import KafkaConsumer

TOPIC = os.getenv("KAFKA_TOPIC", "sine_graph")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sine-web")
PORT = int(os.getenv("PORT", "8000"))
INDEX_HTML = (Path(__file__).parent / "index.html").read_bytes()

clients = set()
clients_lock = threading.Lock()


def consume():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
    )
    print(f"Consuming {TOPIC} from {BOOTSTRAP_SERVERS} as group {GROUP_ID}...", flush=True)
    for message in consumer:
        payload = json.dumps(json.loads(message.value.decode("utf-8")))
        with clients_lock:
            for client in list(clients):
                try:
                    client.put_nowait(payload)
                except queue.Full:
                    pass


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(INDEX_HTML)))
            self.end_headers()
            self.wfile.write(INDEX_HTML)
        elif self.path == "/events":
            self.stream_events()
        else:
            self.send_error(404)

    def stream_events(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        client = queue.Queue(maxsize=1000)
        with clients_lock:
            clients.add(client)
        try:
            while True:
                try:
                    payload = client.get(timeout=15)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with clients_lock:
                clients.discard(client)

    def log_message(self, format, *args):
        pass


def main():
    threading.Thread(target=consume, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Serving sine graph UI on port {PORT}...", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
