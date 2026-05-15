# Lakehouse — Real-time Data Pipeline

> **MySQL → Debezium → Kafka → (Iceberg Sink / Flink) → Iceberg (HMS + MinIO) → Trino → Superset**

Pipeline streaming dữ liệu realtime từ MySQL vào Iceberg Lakehouse, hỗ trợ truy vấn analytics qua Trino và visualize qua Superset.

---

## 📐 Kiến trúc tổng quan

```
┌──────────┐    CDC     ┌──────────┐  topic_DB.DB.table  ┌───────────┐
│  MySQL   │──────────▶│ Debezium │────────────────────▶│   Kafka   │
│(source DB│  binlog   │ (Source  │                     │  Broker   │
└──────────┘           │Connector)│                     └─────┬─────┘
                       └──────────┘                           │
                                          ┌───────────────────┼────────────────────┐
                                          │                   │                    │
                                   [Luồng 1: Sink]    [Luồng 2: Flink]      field.py
                                          │                   │             (sanitizer)
                                    ┌─────▼─────┐      ┌─────▼─────┐            │
                                    │  Iceberg  │      │   Flink   │   iceberg_v9.DB.table
                                    │   Kafka   │      │  SQL Jobs │            │
                                    │   Sink    │      └─────┬─────┘            │
                                    └─────┬─────┘            │                  │
                                          │                  │                  │
                                          └──────────────────┘◀─────────────────┘
                                                             │
                                                      ┌──────▼──────┐
                                                      │   Iceberg   │
                                                      │ (HMS + MinIO│
                                                      │  S3-compat) │
                                                      └──────┬──────┘
                                                             │
                                                      ┌──────▼──────┐
                                                      │    Trino    │◀── SQL queries
                                                      │  (engine)   │
                                                      └──────┬──────┘
                                                             │
                                                      ┌──────▼──────┐
                                                      │  Superset   │
                                                      │ (Dashboard) │
                                                      └─────────────┘
```

---

## 🗂️ Cấu trúc thư mục

```
Lakehouse/                          ← Thư mục gốc dự án
├── .gitignore
├── README.md                       ← file này
├── find_unused_jars.sh
│
├── lakehouse/                      ← ⚙️ Core scripts chạy pipeline
│   ├── .env                        ← Biến môi trường (DB, Hive host...)
│   ├── pipeline_common.sh          ← Boilerplate functions dùng chung
│   ├── deploy_platform.sh          ← Deploy Luồng 1 (Iceberg Sink) cho bất kỳ DB
│   ├── submit_flink.sh             ← Submit/restart Flink jobs
│   ├── field.py                    ← Kafka transformer: chuẩn hoá tên cột + routing
│   ├── datalakehouse_schema.sql    ← Schema tổng quan
│   ├── jars/                       ← Cache JARs tải về (gitignored)
│   ├── generated/                  ← SQL do gen_db.py tạo ra (gitignored)
│   │   └── <project>/
│   │       ├── setup_metadata.sql
│   │       ├── tables.list
│   │       └── jobs/
│   │           ├── job_1.sql
│   │           └── job_N.sql
│   └── logs/                       ← Log file của field.py và các script
│
├── student/                        ← 🐳 Infra Docker + JARs cho dự án HRM
│   ├── docker-compose.yml          ← Toàn bộ stack: Kafka, Flink, Trino, MinIO, HMS...
│   ├── iceberg-kafka-connect/      ← Plugin Iceberg Sink cho Kafka Connect
│   ├── iceberg-kafka-connect-1.7.1.jar
│   ├── setup_hrm.sh                ← All-in-one: deploy pipeline HRM (Luồng 1)
│   └── demo/                       ← Demo data, scripts thử nghiệm
│
└── flink/                          ← 🔄 Luồng 2: Flink SQL Pipeline (đang phát triển)
    ├── docker-compose.yml          ← (planned) Flink cluster riêng hoặc override
    ├── jobs/                       ← Flink SQL job files
    │   └── <project>/
    │       └── job_N.sql
    └── jars/                       ← JARs dành riêng cho Flink pipeline
```

---

## 🚀 Hai luồng xử lý

### Luồng 1 — Iceberg Sink (Kafka Connect)

Dùng `deploy_platform.sh` để deploy cho **bất kỳ MySQL database nào** mà không cần viết SQL thủ công.

```
Cách dùng (từ thư mục lakehouse/):
  bash deploy_platform.sh <DB_NAME>          # Deploy thường (giữ data cũ)
  bash deploy_platform.sh <DB_NAME> --reset  # Reset toàn bộ từ đầu

Luồng chạy:
[1]  Kiểm tra Iceberg Sink plugin       (iceberg-kafka-connect/)
[1b] Tải + copy Hive/Thrift JARs       (cache ở lakehouse/jars/)
[2]  Kiểm tra HMS                       port 9083
[3]  Kiểm tra plugin Debezium           restart nếu chưa load iceberg class
[4]  Chờ services                       Kafka, Kafka Connect, Trino
[5]  Vệ sinh                            tuỳ --reset hay update mode
[6]  Tạo MinIO bucket + Trino schema
[7]  Đăng ký Debezium Source Connector
     └─ initial snapshot (nếu mới) → chờ xong → chuyển when_needed
     └─ Tránh conflict offset: dùng tên tạm (SOURCE_NAME-init-<timestamp>)
[8]  Đăng ký Iceberg Sink Connector
     └─ topics.regex: iceberg_v9.<DB>.*
     └─ Catalog: Hive (HMS), warehouse: s3://<BUCKET>/iceberg-data
     └─ Dynamic routing theo field: target_table
[9]  Chờ Sink RUNNING
[10] Khởi động field.py (nền)          → tạo iceberg_v9.* topics → restart Sink
```

---

### Luồng 2 — Flink SQL Pipeline

Dùng Flink SQL Jobs để xử lý và transform dữ liệu với logic nghiệp vụ phức tạp hơn (enrichment, aggregation, join...).

**Thư mục**: `flink/`

```
Cách dùng (từ thư mục lakehouse/):
  bash submit_flink.sh                      # Submit SQL mặc định
  bash submit_flink.sh path/to/custom.sql   # Submit SQL tùy chọn
  bash submit_flink.sh --cancel             # Hủy tất cả Jobs
  bash submit_flink.sh --restart            # Hủy cũ + submit lại
  bash submit_flink.sh --restart file.sql   # Hủy + submit file cụ thể

Luồng chạy (HRM Demo — setup_hrm.sh):
[1] Tải JARs         → iceberg-flink-runtime, iceberg-aws-bundle, flink-shaded-hadoop...
    └─ Copy vào flink-jobmanager, flink-taskmanager, flink-sql-client

[2] Tạo MySQL schema → CREATE DATABASE hrm + 4 bảng
    └─ employees, attendance, leave_requests, payroll + seed data

[3] Khởi động HMS   → Kiểm tra port 9083, reset nếu cần

[4] Dọn dẹp        → Cancel Flink jobs cũ, xoá topics/tables/bucket

[5] Debezium Source → Đăng ký MySQL connector, chờ RUNNING
    └─ Sinh topics: hrm.hrm.employees, hrm.hrm.attendance...

[6] Trino Schema    → CREATE SCHEMA iceberg.db_hrm (s3a://hrm/iceberg-data/)

[7] Flink SQL Submit → Tạo Iceberg tables + Kafka source tables
    └─ hrm_catalog (type=hive, uri=thrift://HMS:9083)
    └─ 4 INSERT jobs chạy song song (STATEMENT SET)
    └─ Enrichment: tính work_hours, overtime, tax, insurance

[8] Trino Views     → Tạo 6 analytics views sau 90s:
    └─ attendance_summary, department_stats, late_ranking
    └─ leave_analysis, payroll_summary, overtime_report
```

---

## 🔧 field.py — Kafka Transformer (Middleware)

**Vai trò**: cầu nối giữa Debezium output (nested envelope) và Iceberg Sink (flat message).

```
Input topic:   topic_<DB>.<DB>.<table>   (Debezium format)
Output topic:  iceberg_v9.<DB>.<table>   (Flat format cho Sink)

Xử lý:
  Debezium Envelope { before, after, source, op, ts_ms }
        │ transform_message()
        ▼
  Flat Message { col1: v1, ..., target_table: "db_x.tbl" }

  - Bỏ qua DELETE (op='d') và after=null
  - sanitize() tên cột: unidecode + chỉ giữ [a-z0-9_]
  - Tự tạo iceberg_v9.* topic nếu chưa có
  - Consumer group: field-sanitizer-v4-flat
```

---

## 🚦 Quick Start

### Khởi động hệ thống (từ đầu)

```bash
# 1. Vào thư mục student, khởi động toàn bộ Docker stack
cd ~/Lakehouse/student
docker compose up -d

# 2. Chờ services sẵn sàng (~2-3 phút), kiểm tra
docker compose ps
```

### Luồng 1 — Deploy Iceberg Sink cho DB bất kỳ

```bash
cd ~/Lakehouse/lakehouse

# Lần đầu: reset + deploy toàn bộ
bash deploy_platform.sh <DB_NAME> --reset

# Lần sau (chỉ update connector):
bash deploy_platform.sh <DB_NAME>
```

### Luồng 2 — Deploy Flink Pipeline (HRM Demo)

```bash
cd ~/Lakehouse/student

# Chạy toàn bộ pipeline HRM (~5-10 phút)
bash setup_hrm.sh

# Kiểm tra kết quả
docker exec trino trino --user admin --execute "SHOW TABLES IN iceberg.db_hrm"
```

### Submit/Restart Flink Jobs thủ công

```bash
cd ~/Lakehouse/lakehouse

bash submit_flink.sh --restart
```

---

## 🔧 Services & Ports

| Service           | Port  | URL                        | Ghi chú                    |
|-------------------|-------|----------------------------|----------------------------|
| **Flink UI**      | 8081  | http://localhost:8081      | Job monitoring             |
| **Kafka UI**      | 8089  | http://localhost:8089      | Topic & message browser    |
| **MinIO UI**      | 9001  | http://localhost:9001      | admin / password           |
| **MinIO API**     | 9000  | http://localhost:9000      | S3-compatible endpoint     |
| **Trino**         | 8080  | http://localhost:8080      | SQL query engine           |
| **Superset**      | 8088  | http://localhost:8088      | Dashboard & visualization  |
| **Kafka Connect** | 8083  | http://localhost:8083      | Debezium + Iceberg Sink    |
| **HMS (Thrift)**  | 9083  | thrift://localhost:9083    | Hive Metastore             |
| **MySQL**         | 3306  | localhost:3306             | Source database            |

---

## 📦 JARs quan trọng

Các file `.jar` **KHÔNG** được push lên Git (xem `.gitignore`). Chúng được tải tự động khi chạy script.

| JAR | Phiên bản | Vai trò |
|-----|-----------|---------|
| `iceberg-flink-runtime-1.18` | 1.5.0 | Flink ↔ Iceberg integration |
| `iceberg-aws-bundle` | 1.7.1 | S3/MinIO file I/O |
| `flink-shaded-hadoop-2-uber` | 2.8.3-10.0 | Hadoop compat cho Flink |
| `hive-exec` | 3.1.3 | HMS catalog support |
| `flink-sql-connector-kafka` | 3.0.1-1.18 | Flink ↔ Kafka |
| `iceberg-kafka-connect-runtime` | 0.6.x | Iceberg Sink Connector (Luồng 1) |
| `aws-java-sdk-bundle` | 1.11.271 | AWS SDK cho MinIO |
| `mysql-connector-java` | 8.0.28 | MySQL JDBC driver |

---

## ⚙️ Cấu hình .env

Tạo file `.env` tại `lakehouse/.env`:

```env
mysql_host=mysql
mysql_user=root
mysql_password=123
mysql_database=<tên_database>
hive_host=172.18.0.6
```

---

## 🎯 Truy vấn kiểm tra (HRM)

```sql
-- Kiểm tra bảng đã có data chưa
SHOW TABLES IN iceberg.db_hrm;

-- Xếp hạng đi muộn
SELECT * FROM iceberg.db_hrm.late_ranking;

-- Thống kê theo phòng ban
SELECT * FROM iceberg.db_hrm.department_stats;

-- Phân tích nghỉ phép
SELECT * FROM iceberg.db_hrm.leave_analysis;

-- Tóm tắt lương
SELECT * FROM iceberg.db_hrm.payroll_summary;

-- Báo cáo OT
SELECT * FROM iceberg.db_hrm.overtime_report;
```

Chạy qua CLI:
```bash
docker exec trino trino --user admin --execute "SELECT * FROM iceberg.db_hrm.late_ranking"
```

Kết nối Superset:
```
trino://admin@trino:8080/iceberg
```

---

## 🛠️ Troubleshooting

### Flink job FAILED
```bash
# Kiểm tra log job
docker exec flink-jobmanager curl -s http://localhost:8081/jobs \
  | python3 -c "import sys,json; [print(j) for j in json.load(sys.stdin)['jobs']]"

# Restart jobs
cd ~/Lakehouse/lakehouse && bash submit_flink.sh --restart
```

### Iceberg Sink FAILED
```bash
# Xem trạng thái
curl -s http://localhost:8083/connectors/<sink-name>/status | python3 -m json.tool

# Xem error trace
curl -s http://localhost:8083/connectors/<sink-name>/status | python3 -c "
import sys,json; s=json.load(sys.stdin)
print(s.get('tasks',[{}])[0].get('trace',''))
"
```

### HMS không kết nối được
```bash
# Kiểm tra port 9083
docker exec hive-metastore bash -c "echo > /dev/tcp/localhost/9083" && echo OK

# Restart HMS
cd ~/Lakehouse/student && docker compose restart hive-metastore
```

### field.py không chạy
```bash
# Chạy thủ công foreground
cd ~/Lakehouse/lakehouse
python3 field.py

# Chạy nền
nohup python3 field.py > logs/sanitizer.log 2>&1 &

# Xem log
tail -f logs/sanitizer.log
```

---

## 📋 Kafka Topics Convention

| Pattern | Ý nghĩa |
|---------|---------|
| `topic_<DB>.<DB>.<table>` | Debezium output (nested envelope) |
| `schemahistory.<DB>` | Schema history của Debezium |
| `iceberg_v9.<DB>.<table>` | Transformed output (flat, cho Iceberg Sink) |
| `control-iceberg-<db>` | Coordination topic của Iceberg Sink |

---

*Cập nhật: Tháng 5/2026*
