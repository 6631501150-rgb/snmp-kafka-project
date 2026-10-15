#!/usr/bin/env python3
"""
collector.py - ดึงข้อมูลทราฟฟิกของอินเทอร์เฟซบน Cisco 7200 ด้วยคำสั่ง snmpwalk (Net-SNMP)

ต้องติดตั้งบน Ubuntu ก่อน:  sudo apt install -y snmp   (และ pip install kafka-python ถ้าจะส่งเข้า Kafka)
ตัวอย่างการรัน:
    python3 collector.py --host 192.168.75.129 --community public --interface FastEthernet1/1
    python3 collector.py --interval 5 --count 12
    python3 collector.py --kafka localhost:9092 --topic snmp
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

# OID ที่ใช้ (ตัวเลขล้วน จึงไม่ต้องติดตั้งไฟล์ MIB)
OID_IF_DESCR = "1.3.6.1.2.1.2.2.1.2"            # IF-MIB::ifDescr
OID_IF_OPER_STATUS = "1.3.6.1.2.1.2.2.1.8"      # IF-MIB::ifOperStatus (1=up, 2=down)
OID_IF_IN_UCAST = "1.3.6.1.2.1.2.2.1.11"        # IF-MIB::ifInUcastPkts
OID_IF_IN_ERRORS = "1.3.6.1.2.1.2.2.1.14"       # IF-MIB::ifInErrors
OID_IF_OUT_UCAST = "1.3.6.1.2.1.2.2.1.17"       # IF-MIB::ifOutUcastPkts
OID_IF_OUT_ERRORS = "1.3.6.1.2.1.2.2.1.20"      # IF-MIB::ifOutErrors
OID_IF_HC_IN_OCTETS = "1.3.6.1.2.1.31.1.1.1.6"  # IF-MIB::ifHCInOctets  (64-bit)
OID_IF_HC_OUT_OCTETS = "1.3.6.1.2.1.31.1.1.1.10"  # IF-MIB::ifHCOutOctets (64-bit)
OID_IF_HIGH_SPEED = "1.3.6.1.2.1.31.1.1.1.15"   # IF-MIB::ifHighSpeed (Mbps)

COUNTER64_MAX = 2 ** 64
COUNTER32_MAX = 2 ** 32


def snmpwalk(host, community, oid, timeout=5):
    """รัน snmpwalk แล้วคืนค่าเป็น dict {ifIndex: value}"""
    cmd = [
        "snmpwalk", "-v2c", "-c", community,
        "-On",   # แสดง OID เป็นตัวเลข
        "-Oq",   # ตัด '= TYPE:' ออก เหลือ 'OID value'
        "-Oe",   # แสดง enum เป็นตัวเลข เช่น up(1) -> 1
        "-t", str(timeout), "-r", "1",
        host, oid,
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True,
                             timeout=timeout * 3, check=True).stdout
    except FileNotFoundError:
        sys.exit("ไม่พบคำสั่ง snmpwalk  ->  sudo apt install -y snmp")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"snmpwalk {oid} ล้มเหลว: {e.stderr.strip()}") from e

    result = {}
    prefix = "." + oid + "."
    for line in out.splitlines():
        if not line.startswith(prefix):
            continue  # เช่น 'No Such Object available on this agent at this OID'
        full_oid, _, value = line.partition(" ")
        index = int(full_oid[len(prefix):])
        result[index] = value.strip().strip('"')
    return result


def find_if_index(host, community, if_name):
    """หา ifIndex จากชื่ออินเทอร์เฟซ เช่น FastEthernet1/0 (รองรับชื่อย่อ เช่น '1/0')"""
    descr = snmpwalk(host, community, OID_IF_DESCR)
    for index, name in descr.items():
        if name == if_name:
            return index, name
    for index, name in descr.items():
        if name.endswith(if_name):
            return index, name
    names = ", ".join(f"{i}={n}" for i, n in sorted(descr.items()))
    sys.exit(f"ไม่พบอินเทอร์เฟซ '{if_name}' บน {host} (ที่มีอยู่: {names})")


def read_counters(host, community, index):
    def get(oid):
        value = snmpwalk(host, community, oid).get(index)
        return int(value) if value is not None and value.lstrip("-").isdigit() else None

    return {
        "oper_status": "up" if get(OID_IF_OPER_STATUS) == 1 else "down",
        "speed_mbps": get(OID_IF_HIGH_SPEED),
        "in_octets": get(OID_IF_HC_IN_OCTETS),
        "out_octets": get(OID_IF_HC_OUT_OCTETS),
        "in_ucast_pkts": get(OID_IF_IN_UCAST),
        "out_ucast_pkts": get(OID_IF_OUT_UCAST),
        "in_errors": get(OID_IF_IN_ERRORS),
        "out_errors": get(OID_IF_OUT_ERRORS),
    }


def delta(new, old, wrap):
    """ผลต่างของ counter โดยรองรับกรณี counter วนกลับเป็น 0"""
    if new is None or old is None:
        return None
    return new - old if new >= old else new + wrap - old


def main():
    p = argparse.ArgumentParser(description="ดึงทราฟฟิกอินเทอร์เฟซของเราเตอร์ด้วย snmpwalk")
    p.add_argument("--host", default="192.168.75.129")
    p.add_argument("--community", default="public")
    p.add_argument("--interface", default="FastEthernet1/1",
                   help="ชื่อตาม ifDescr หรือชื่อท้าย เช่น 1/1")
    # Cisco อัปเดต interface counter ให้ SNMP ทุก ~10 วินาที ถ้า poll ถี่กว่านี้ค่าจะสลับ 0 / สูงเกินจริง
    p.add_argument("--interval", type=float, default=10, help="วินาทีระหว่างการ poll (ควร >= 10)")
    p.add_argument("--count", type=int, default=0, help="จำนวนครั้ง (0 = ไม่จำกัด)")
    p.add_argument("--json", action="store_true", help="แสดงผลเป็น JSON เต็ม แทนบรรทัดสรุป")
    p.add_argument("--kafka", default=None,
                   help="Kafka bootstrap server เช่น localhost:9092 (ไม่ใส่ = print อย่างเดียว)")
    p.add_argument("--topic", default="snmp")
    args = p.parse_args()

    index, if_name = find_if_index(args.host, args.community, args.interface)
    print(f"# {args.host} {if_name} -> ifIndex {index}", file=sys.stderr)

    producer = None
    if args.kafka:
        try:
            from kafka import KafkaProducer
            from kafka.serializer import Serializer
        except ImportError:
            sys.exit("ไม่พบไลบรารี kafka  ->  pip install kafka-python")

        class StrSerializer(Serializer):
            def serialize(self, topic, value):
                return value.encode("utf-8")

        class JsonSerializer(Serializer):
            def serialize(self, topic, value):
                return json.dumps(value, ensure_ascii=False).encode("utf-8")

        producer = KafkaProducer(
            bootstrap_servers=args.kafka,
            key_serializer=StrSerializer(),
            value_serializer=JsonSerializer(),
        )
        print(f"# ส่งข้อมูลเข้า Kafka {args.kafka} topic '{args.topic}'", file=sys.stderr)

    try:
        poll_loop(args, index, if_name, producer)
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


def short_if(name):
    """ย่อชื่ออินเทอร์เฟซแบบ Cisco เช่น FastEthernet1/1 -> Fa1/1"""
    for full, short in (("FastEthernet", "Fa"), ("GigabitEthernet", "Gi"), ("Ethernet", "Et")):
        if name.startswith(full):
            return short + name[len(full):]
    return name


def summary_line(record):
    """บรรทัดสรุปสั้น ๆ (~45 ตัวอักษร) สำหรับดูบนหน้าจอ"""
    def mbps(bps):
        return f"{'-':>6}" if bps is None else f"{bps / 1_000_000:6.2f}"

    t = datetime.fromisoformat(record["timestamp"]).astimezone().strftime("%H:%M:%S")
    line = (f"{t} {short_if(record['interface'])} {record['oper_status']:4} "
            f"in {mbps(record['in_bps'])} out {mbps(record['out_bps'])} Mbps")
    return line + (" (baseline)" if record["in_bps"] is None else "")


def poll_loop(args, index, if_name, producer):
    prev, prev_time, n = None, None, 0
    next_poll = time.time()
    while True:
        now = time.time()
        cur = read_counters(args.host, args.community, index)

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "router": args.host,
            "interface": if_name,
            "if_index": index,
            **cur,
            "in_bps": None,
            "out_bps": None,
        }
        if prev is not None:
            elapsed = now - prev_time
            d_in = delta(cur["in_octets"], prev["in_octets"], COUNTER64_MAX)
            d_out = delta(cur["out_octets"], prev["out_octets"], COUNTER64_MAX)
            record["in_bps"] = round(d_in * 8 / elapsed, 2) if d_in is not None else None
            record["out_bps"] = round(d_out * 8 / elapsed, 2) if d_out is not None else None
            record["in_pkts_delta"] = delta(cur["in_ucast_pkts"], prev["in_ucast_pkts"], COUNTER32_MAX)
            record["out_pkts_delta"] = delta(cur["out_ucast_pkts"], prev["out_ucast_pkts"], COUNTER32_MAX)

        if producer is not None:
            # key = router:interface เพื่อให้ข้อมูลของอินเทอร์เฟซเดียวกันอยู่ partition เดียวกัน (เรียงลำดับถูก)
            producer.send(args.topic, key=f"{args.host}:{if_name}", value=record)
            producer.flush()
        if args.json:
            print(json.dumps(record, ensure_ascii=False), flush=True)
        else:
            print(summary_line(record), flush=True)

        prev, prev_time, n = cur, now, n + 1
        if args.count and n >= args.count:
            break
        # นับรอบจากเวลาเริ่ม ไม่ใช่จากเวลาที่ snmpwalk เสร็จ เพื่อให้แต่ละรอบห่างกันเท่ากันพอดี
        next_poll += args.interval
        time.sleep(max(0, next_poll - time.time()))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
