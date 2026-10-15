#!/usr/bin/env python3
"""
worker.py - อ่านข้อมูลทราฟฟิกจาก Kafka topic "snmp" (ที่ collector.py ส่งมา) แล้ว
  1) แสดงผลเป็นตาราง Mbps / % utilization
  2) บันทึกลงไฟล์ CSV
  3) แจ้งเตือนเมื่อ utilization เกินค่าที่กำหนด หรือมี error / อินเทอร์เฟซ down

ต้องติดตั้ง:  pip install kafka-python
ตัวอย่างการรัน:
    python3 worker.py
    python3 worker.py --alert-util 5 --csv traffic.csv
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime

try:
    from kafka import KafkaConsumer
    from kafka.serializer import Deserializer
except ImportError:
    sys.exit("ไม่พบไลบรารี kafka  ->  pip install kafka-python")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, bytes_):
        return json.loads(bytes_.decode("utf-8"))

CSV_FIELDS = [
    "timestamp", "router", "interface", "oper_status",
    "in_mbps", "out_mbps", "util_pct", "in_pkts_delta", "out_pkts_delta",
    "in_errors", "out_errors",
]


def to_mbps(bps):
    return None if bps is None else round(bps / 1_000_000, 3)


def utilization(record):
    """% การใช้งานลิงก์ คิดจากทิศที่มากกว่า (in หรือ out)"""
    speed = record.get("speed_mbps")
    rates = [r for r in (record.get("in_bps"), record.get("out_bps")) if r is not None]
    if not speed or not rates:
        return None
    return round(max(rates) / (speed * 1_000_000) * 100, 2)


def local_time(iso_ts):
    try:
        return datetime.fromisoformat(iso_ts).astimezone().strftime("%H:%M:%S")
    except (TypeError, ValueError):
        return str(iso_ts)


def fmt(value, width, digits=2):
    return f"{'-':>{width}}" if value is None else f"{value:>{width}.{digits}f}"


def short_if(name):
    """ย่อชื่ออินเทอร์เฟซแบบ Cisco เช่น FastEthernet1/1 -> Fa1/1"""
    for full, short in (("FastEthernet", "Fa"), ("GigabitEthernet", "Gi"), ("Ethernet", "Et")):
        if name and name.startswith(full):
            return short + name[len(full):]
    return name or "?"


USE_COLOR = sys.stdout.isatty()


def red(text):
    return f"\033[1;31m{text}\033[0m" if USE_COLOR else text


def main():
    p = argparse.ArgumentParser(description="Kafka consumer สำหรับข้อมูลทราฟฟิก SNMP")
    p.add_argument("--kafka", default="localhost:9092")
    p.add_argument("--topic", default="snmp")
    p.add_argument("--group", default="snmp-worker", help="Kafka consumer group id")
    p.add_argument("--csv", default="traffic.csv", help="ไฟล์ CSV ที่จะบันทึก (ใส่ '' เพื่อไม่บันทึก)")
    p.add_argument("--alert-util", type=float, default=80.0,
                   help="แจ้งเตือนเมื่อ utilization (%%) มากกว่าหรือเท่ากับค่านี้")
    args = p.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.kafka,
        group_id=args.group,
        auto_offset_reset="earliest",   # ครั้งแรกอ่านตั้งแต่ข้อความแรก ครั้งต่อไปอ่านต่อจากที่ค้างไว้
        value_deserializer=JsonDeserializer(),
    )
    print(f"# รอข้อมูลจาก Kafka {args.kafka} topic '{args.topic}' (group '{args.group}')", file=sys.stderr)
    print(f"# แจ้งเตือน HIGH-UTIL เมื่อ utilization >= {args.alert_util:g}%", file=sys.stderr)

    csv_file = writer = None
    if args.csv:
        new_file = not os.path.exists(args.csv) or os.path.getsize(args.csv) == 0
        csv_file = open(args.csv, "a", newline="", encoding="utf-8")
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()

    print(f"{'TIME':8} {'IF':6} {'STAT':4} {'IN Mbps':>7} {'OUT Mbps':>8} {'UTIL%':>6}  ALERT")
    last_errors = {}
    try:
        for msg in consumer:
            r = msg.value
            if r.get("in_bps") is None and r.get("out_bps") is None:
                continue  # record แรกของ collector ยังไม่มีอัตรา (ใช้เป็น baseline)

            in_mbps, out_mbps = to_mbps(r.get("in_bps")), to_mbps(r.get("out_bps"))
            util = utilization(r)

            alerts = []
            if r.get("oper_status") != "up":
                alerts.append("IF-DOWN")
            if util is not None and util >= args.alert_util:
                alerts.append("HIGH-UTIL")
            key = msg.key.decode("utf-8") if msg.key else r.get("interface")
            errors = (r.get("in_errors") or 0, r.get("out_errors") or 0)
            if key in last_errors and any(now > before for now, before in zip(errors, last_errors[key])):
                alerts.append("NEW-ERR")
            last_errors[key] = errors

            print(f"{local_time(r.get('timestamp')):8} {short_if(r.get('interface')):6} "
                  f"{r.get('oper_status', '?'):4} {fmt(in_mbps, 7)} {fmt(out_mbps, 8)} "
                  f"{fmt(util, 6)}  {red(' '.join(alerts)) if alerts else ''}", flush=True)

            if writer:
                writer.writerow({
                    "timestamp": r.get("timestamp"),
                    "router": r.get("router"),
                    "interface": r.get("interface"),
                    "oper_status": r.get("oper_status"),
                    "in_mbps": in_mbps,
                    "out_mbps": out_mbps,
                    "util_pct": util,
                    "in_pkts_delta": r.get("in_pkts_delta"),
                    "out_pkts_delta": r.get("out_pkts_delta"),
                    "in_errors": r.get("in_errors"),
                    "out_errors": r.get("out_errors"),
                })
                csv_file.flush()
    finally:
        consumer.close()
        if csv_file:
            csv_file.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
