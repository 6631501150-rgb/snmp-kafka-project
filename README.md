# SNMP → Kafka Traffic Monitor

เก็บ traffic counter ของอินเทอร์เฟซบน Cisco Router ด้วย SNMP แล้วส่งผ่าน Kafka ไปแสดงผล

```
Cisco 7200 (R1 Fa1/1) ──SNMP──> collector.py ──produce──> Kafka topic "snmp" ──consume──> worker.py / dashboard.py
```

| ไฟล์ | หน้าที่ |
|---|---|
| `collector.py` | Producer: ใช้ `snmpwalk` ดึง ifHCInOctets/ifHCOutOctets ฯลฯ คำนวณ bps แล้วส่งเข้า Kafka |
| `worker.py` | Consumer: แสดงตารางใน terminal, บันทึก `traffic.csv`, แจ้งเตือนเมื่อ utilization เกินเกณฑ์ |
| `dashboard.py` + `static/index.html` | Consumer: เว็บแดชบอร์ดแบบเรียลไทม์ (SSE + Chart.js) ที่พอร์ต 8000 |
| `docker-compose.yml` | Kafka (KRaft) โหนดเดียว |

## Lab (GNS3)

| อุปกรณ์ | อินเทอร์เฟซ | IP |
|---|---|---|
| R1 | Fa0/0 (DHCP, management) | 192.168.75.129 |
| R1 | Fa1/0 ↔ R2 Fa1/0 | 10.12.0.1 / 10.12.0.2 |
| R1 | Fa1/1 → ipterm-1 | 172.16.0.1 / 172.16.0.10 |
| R2 | Fa1/1 → ipterm-2 (iperf3 server) | 172.16.1.1 / 172.16.1.10 |

Router ใช้ OSPF area 0 และ `snmp-server community public RO`

## วิธีรัน (Ubuntu)

```bash
sudo apt install -y snmp python3-venv
python3 -m venv ~/snmp-venv && source ~/snmp-venv/bin/activate
pip install kafka-python

docker compose up -d
docker exec snmp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic snmp --partitions 1 --replication-factor 1

python3 collector.py --host 192.168.75.129 --interface FastEthernet1/1 --kafka localhost:9092
python3 dashboard.py --alert-util 5      # เปิด http://<IP ของ Ubuntu>:8000
python3 worker.py --alert-util 5         # (ไม่บังคับ)
```

สร้างทราฟฟิกทดสอบ:

```bash
# ipterm-2
iperf3 -s
# ipterm-1: ส่ง 5MB จำนวน 20 ครั้ง
for i in $(seq 1 20); do iperf3 -c 172.16.1.10 -n 5M -b 10M; sleep 2; done
```

> Cisco อัปเดต interface counter ให้ SNMP ทุก ~10 วินาที จึงตั้ง poll interval เริ่มต้นไว้ที่ 10 วินาที
