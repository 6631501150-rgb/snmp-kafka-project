#!/usr/bin/env python3
"""
dashboard.py - เว็บแดชบอร์ดแสดงทราฟฟิกแบบเรียลไทม์ อ่านข้อมูลจาก Kafka topic "snmp"

เป็น consumer อีกตัวที่แยกจาก worker.py (ไม่ใช้ consumer group จึงไม่แย่งข้อความกัน)
ตอนเริ่มจะโหลดข้อมูลย้อนหลัง --history ข้อความ แล้วส่งข้อมูลใหม่ให้เบราว์เซอร์ผ่าน
Server-Sent Events (SSE) ทันทีที่เข้ามาใน Kafka

ต้องติดตั้ง:  pip install kafka-python
ตัวอย่างการรัน:
    python3 dashboard.py
    python3 dashboard.py --port 8000 --alert-util 5
แล้วเปิดเบราว์เซอร์ไปที่  http://<IP ของ Ubuntu>:8000
"""

import argparse
import json
import os
import queue
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from kafka import KafkaConsumer, TopicPartition
    from kafka.serializer import Deserializer
except ImportError:
    sys.exit("ไม่พบไลบรารี kafka  ->  pip install kafka-python")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, bytes_):
        return json.loads(bytes_.decode("utf-8"))


class Hub:
    """เก็บข้อมูลย้อนหลังแยกตาม key (router:interface) และกระจายข้อมูลใหม่ให้เบราว์เซอร์ทุกตัว"""

    def __init__(self, maxlen):
        self.lock = threading.Lock()
        self.maxlen = maxlen
        self.history = {}
        self.subscribers = set()

    def add(self, key, record):
        with self.lock:
            self.history.setdefault(key, deque(maxlen=self.maxlen)).append(record)
            subscribers = list(self.subscribers)
        data = json.dumps({"key": key, "record": record}, ensure_ascii=False)
        for q in subscribers:
            try:
                q.put_nowait(data)
            except queue.Full:
                pass  # เบราว์เซอร์ที่ค้าง/ช้า ข้ามไป ไม่ให้ถ่วงตัวอื่น

    def snapshot(self):
        with self.lock:
            return {key: list(records) for key, records in self.history.items()}

    def subscribe(self):
        q = queue.Queue(maxsize=100)
        with self.lock:
            self.subscribers.add(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            self.subscribers.discard(q)


def consume(hub, args):
    """อ่าน Kafka ใน thread แยก ถ้า Kafka ล่มจะลองเชื่อมต่อใหม่ทุก 5 วินาที"""
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=args.kafka,
                group_id=None,
                enable_auto_commit=False,
                value_deserializer=JsonDeserializer(),
            )
            partitions = consumer.partitions_for_topic(args.topic) or {0}
            tps = [TopicPartition(args.topic, p) for p in partitions]
            consumer.assign(tps)
            for tp, end in consumer.end_offsets(tps).items():
                consumer.seek(tp, max(0, end - args.history))
            print(f"# อ่าน Kafka {args.kafka} topic '{args.topic}' แล้ว", file=sys.stderr)

            for msg in consumer:
                key = msg.key.decode("utf-8") if msg.key else "unknown"
                hub.add(key, msg.value)
        except Exception as e:  # noqa: BLE001 - ให้ thread อยู่รอดและลองใหม่
            print(f"# Kafka error: {e} (ลองใหม่ใน 5 วินาที)", file=sys.stderr)
            time.sleep(5)
        finally:
            if consumer is not None:
                consumer.close()


def make_handler(hub, config):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # ไม่ต้อง log ทุก request

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path in ("/", "/index.html"):
                self.send_file("index.html", "text/html; charset=utf-8")
            elif path == "/api/history":
                self.send_json({"config": config, "series": hub.snapshot()})
            elif path == "/api/stream":
                self.stream()
            else:
                self.send_error(404)

        def send_file(self, name, content_type):
            with open(os.path.join(STATIC_DIR, name), "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, obj):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def stream(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            q = hub.subscribe()
            try:
                while True:
                    try:
                        self.wfile.write(f"data: {q.get(timeout=15)}\n\n".encode("utf-8"))
                    except queue.Empty:
                        self.wfile.write(b": ping\n\n")  # กันการเชื่อมต่อหลุดเพราะเงียบนาน
                    self.wfile.flush()
            except OSError:
                pass  # เบราว์เซอร์ปิดหน้าไปแล้ว
            finally:
                hub.unsubscribe(q)

    return Handler


def main():
    p = argparse.ArgumentParser(description="เว็บแดชบอร์ดทราฟฟิก SNMP จาก Kafka")
    p.add_argument("--kafka", default="localhost:9092")
    p.add_argument("--topic", default="snmp")
    p.add_argument("--host", default="0.0.0.0", help="IP ที่เปิดรับ (0.0.0.0 = ทุกการ์ดเครือข่าย)")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--history", type=int, default=360,
                   help="จำนวนข้อมูลย้อนหลังที่เก็บต่ออินเทอร์เฟซ (360 x 10 วินาที = 1 ชั่วโมง)")
    p.add_argument("--alert-util", type=float, default=80.0, help="ค่าเริ่มต้นของเกณฑ์แจ้งเตือน (%%)")
    args = p.parse_args()

    hub = Hub(args.history)
    threading.Thread(target=consume, args=(hub, args), daemon=True).start()

    config = {"topic": args.topic, "alert_util": args.alert_util}
    server = ThreadingHTTPServer((args.host, args.port), make_handler(hub, config))
    server.daemon_threads = True
    print(f"# แดชบอร์ดพร้อมแล้ว: http://{args.host}:{args.port}  (Ctrl+C เพื่อหยุด)", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
