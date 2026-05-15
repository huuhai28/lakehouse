# CÔNG TY CỔ PHẦN LIFETEX
## Triển khai Next-Generation Data Platform (Lakehouse Architecture)
### Hà Nội - 2026

---

# 1. TỔNG QUAN HỆ THỐNG

Hệ thống Next-Generation Data Platform được thiết kế theo kiến trúc Lakehouse – kết hợp ưu điểm của Data Lake (lưu trữ chi phí thấp, linh hoạt) và Data Warehouse (truy vấn có cấu trúc, hiệu suất cao). Toàn bộ nền tảng được vận hành trên môi trường container hóa, hỗ trợ xử lý dữ liệu theo thời gian thực và theo lô (batch).

## 1.1 Mục tiêu
- Xây dựng pipeline dữ liệu end-to-end từ nguồn OLTP đến lớp phân tích BI.
- Đảm bảo tính nhất quán dữ liệu (ACID) trên nền tảng lưu trữ phân tán.
- Hỗ trợ xử lý streaming real-time với độ trễ dưới 1 giây.
- Cho phép truy vấn linh hoạt từ nhiều engine (Trino, Flink) trên cùng một data store.
- Cung cấp dashboard trực quan và alert tự động cho đội ngũ vận hành.

## 1.2 Phạm vi
- **Nguồn dữ liệu**: Các database OLTP quan hệ (MySQL).
- **Lớp vận chuyển**: Debezium CDC → Kafka.
- **Lớp xử lý**: Apache Flink (streaming) và Trino (batch query).
- **Lớp lưu trữ**: MinIO (object storage) + Apache Iceberg (table format).
- **Lớp trực quan hóa**: Apache Superset.
- **Lớp quản trị metadata**: OpenMetadata & Hive Metastore (HMS).

---

# 2. KIẾN TRÚC TỔNG THỂ

## 2.1 Sơ đồ kiến trúc

```
Database (OLTP) → CDC → Message Broker → Stream Processing → Storage/Lakehouse → Query Engine → Governance/Metadata
```

## 2.2 Luồng dữ liệu

- **Nguồn dữ liệu**: Các database OLTP quan hệ (MySQL).
- **Lớp vận chuyển**: Debezium CDC → Kafka.
- **Lớp xử lý**: Apache Flink (streaming) và Trino (batch query).
- **Lớp lưu trữ**: MinIO (object storage) + Apache Iceberg (table format).
- **Lớp trực quan hóa**: Apache Superset.
- **Lớp quản trị metadata**: OpenMetadata & Hive Metastore (HMS).

---

# 3. CHI TIẾT CÁC THÀNH PHẦN CÔNG NGHỆ

## 3.1 Lớp Lưu trữ Đối tượng – MinIO

MinIO là hệ thống object storage hiệu năng cao, tương thích hoàn toàn với Amazon S3 API.

### 3.1.1 Vai trò trong hệ thống
- Lưu trữ toàn bộ data files của Iceberg tables dưới định dạng Apache Parquet.
- Lưu trữ metadata files và manifest lists của Iceberg.
- Đóng vai trò là S3-compatible backend cho Flink và Trino.

### 3.1.2 Cấu hình triển khai
- Container image: `minio/minio:latest`
- Console UI: cổng **9001**
- S3 API endpoint: cổng **9000**
- Buckets chính: `warehouse` (Iceberg data), `checkpoints` (Flink state)

### 3.1.3 Tích hợp với Flink
```yaml
s3.endpoint: http://minio:9000
s3.access-key: admin
s3.secret-key: password
s3.path.style.access: true
```

### 3.1.4 Tích hợp với Trino
```properties
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=password
hive.s3.path-style-access=true
```

---

## 3.2 Lớp Message Broker – Apache Kafka

### 3.2.1 Apache Kafka
- Dữ liệu tổ chức theo Topic, mỗi topic tương ứng một bảng nguồn trong OLTP.
- Retention mặc định: 7 ngày.
- Cổng broker: **9092**.

### 3.2.2 Cấu trúc Topic
- Naming convention: `<prefix>.<database>.<table>` (ví dụ: `hrm.hrm.employees`)
- Message format: JSON với schema Debezium (bao gồm before/after/op/ts_ms).

---

## 3.3 Lớp Table Format – Apache Iceberg

### 3.3.1 Vai trò trong Lakehouse
- **ACID Transactions**: đảm bảo tính nhất quán khi nhiều writer ghi đồng thời.
- **Schema Evolution**: thêm/xóa/đổi tên cột mà không làm hỏng dữ liệu hiện có.
- **Time Travel**: truy vấn dữ liệu tại thời điểm bất kỳ trong quá khứ.
- **Hidden Partitioning**: tự động áp dụng partition filter.

### 3.3.2 Cấu trúc lưu trữ
- **Data files**: file Parquet chứa dữ liệu thực tế.
- **Manifest files**: danh sách data files kèm thống kê.
- **Metadata file**: file JSON mô tả schema, partition spec, snapshot history.

---

## 3.4 Lớp Query Engine – Trino

### 3.4.1 Kiến trúc Trino
- **Coordinator**: tiếp nhận query, lập kế hoạch thực thi, điều phối Worker.
- **Worker nodes**: thực thi các task song song, đọc dữ liệu từ MinIO/Iceberg.
- Cổng HTTP: **8080**.

### 3.4.2 Catalogs được cấu hình
- `iceberg`: kết nối Iceberg tables qua Hive Metastore (HMS).
- `tpch`, `tpcds`: built-in catalog cho benchmarking.

### 3.4.3 Cấu hình kết nối từ Superset
```
trino://admin@trino:8080/iceberg
```

---

## 3.5 Lớp Trực quan hóa – Apache Superset

### 3.5.1 Tính năng chính
- Chart builder: hơn 40 loại biểu đồ.
- Dashboard: tập hợp nhiều chart, hỗ trợ filter liên kết.
- SQL Lab: IDE SQL tích hợp.
- Alerts & Reports: gửi email/Slack theo lịch.

### 3.5.2 Cấu hình Database Connection
- Engine: Trino
- SQLAlchemy URI: `trino://admin@trino:8080/iceberg`

### 3.5.3 Triển khai
- Container image: `apache/superset:latest`
- Cổng: **8088**

---

## 3.6 Lớp Quản trị & Metadata – OpenMetadata

### 3.6.1 Vai trò trong hệ thống
- **Data Catalog**: lập chỉ mục bảng, cột từ Trino/Iceberg.
- **Data Lineage**: vẽ đồ thị luồng dữ liệu từ MySQL → Kafka → Flink → Iceberg → Superset.
- **Data Quality**: kiểm tra chất lượng dữ liệu theo định kỳ.

### 3.6.2 Connectors được sử dụng
- Trino connector, Kafka connector, Superset connector.

### 3.6.3 Hive Metastore (HMS)
Hive Metastore Service đóng vai trò là "bộ não" quản lý metadata cho toàn bộ hệ thống Lakehouse. Trong kiến trúc Iceberg, HMS không lưu trữ dữ liệu thực tế mà thực hiện các nhiệm vụ cốt lõi sau:

*   **Quản lý con trỏ Metadata (Metadata Pointer Management):** Lưu trữ vị trí của file metadata Iceberg mới nhất (file `.json`) cho mỗi bảng. Khi Trino hoặc Flink truy vấn, chúng sẽ hỏi HMS "File metadata mới nhất của bảng này nằm ở đâu trên MinIO?".
*   **Danh mục tập trung (Centralized Catalog):** Quản lý cấu trúc phân cấp Database/Table/View. Điều này cho phép một bảng được tạo bởi Flink có thể được nhìn thấy và truy vấn ngay lập tức bởi Trino.
*   **Điều phối giao dịch (Transaction Coordination):** Đảm bảo tính nhất quán khi có nhiều phiên làm việc cùng thay đổi bảng, giúp tránh xung đột metadata.
*   **Khả năng tương thích đa Engine:** Cung cấp giao thức **Thrift** (mặc định cổng **9083**), cho phép các công cụ khác nhau (Spark, Presto, Trino, Flink) làm việc chung trên một nguồn dữ liệu duy nhất mà không bị sai lệch metadata.
*   **Lưu trữ thông tin Schema:** Lưu giữ định nghĩa các cột, kiểu dữ liệu và thông tin phân vùng (partitioning) ở mức logic.

---

# 4. MÔI TRƯỜNG TRIỂN KHAI

## 4.1 Lab / Development
- Toàn bộ stack chạy trên một máy đơn bằng Docker Compose.
- Yêu cầu tối thiểu: 16GB RAM, 8 CPU cores, 100GB disk.
- Khởi động: `docker compose up -d`

## 4.2 Cổng dịch vụ

| Dịch vụ | Cổng | URL |
|---------|------|-----|
| MinIO Console | 9001 | http://localhost:9001 |
| MinIO S3 API | 9000 | http://localhost:9000 |
| Kafka UI | 8089 | http://localhost:8089 |
| Kafka Broker | 9092 | localhost:9092 |
| Flink Web UI | 8081 | http://localhost:8081 |
| Trino | 8080 | http://localhost:8080 |
| Superset | 8088 | http://localhost:8088 |
| OpenMetadata | 8585 | http://localhost:8585 |
| HMS (Thrift) | 9083 | thrift://localhost:9083 |

## 4.3 Production
- Kubernetes: mỗi service triển khai dưới dạng Deployment hoặc StatefulSet.
- MinIO: distributed mode với tối thiểu 4 nodes.
- Kafka: cluster 3 broker với replication factor 3.
- Trino: 1 Coordinator + N Worker nodes.
- Monitoring: Prometheus + Grafana.

---

# 5. TRIỂN KHAI

Tổng quan các bước triển khai:

| Bước | Nội dung |
|------|----------|
| 1 | Khởi động Docker Compose |
| 2 | Tải và cài đặt thư viện JAR cho Flink |
| 3 | Khởi tạo database và schema MySQL |
| 4 | Đăng ký Debezium CDC Connector |
| 5 | Tạo bucket lưu trữ trên MinIO |
| 6 | Tạo Iceberg namespace qua Trino |
| 7 | Cấu hình Trino kết nối HMS |
| 8 | Submit Flink SQL Pipeline |
| 9 | Khởi tạo Superset và kết nối Trino |
| 10 | Cấu hình OpenMetadata |

---

## 5.1 Tải thư viện JAR cho Flink

Flink mặc định không hỗ trợ Iceberg và S3. Cần tải thêm các thư viện và copy vào tất cả Flink containers.

### 5.1.1 Tải các JAR cần thiết
```bash
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.0/iceberg-flink-runtime-1.18-1.5.0.jar
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar
wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
```

### 5.1.2 Copy JAR vào các Flink containers
```bash
for CONTAINER in flink-jobmanager flink-taskmanager flink-sql-client; do
  docker cp iceberg-flink-runtime-1.18-1.5.0.jar $CONTAINER:/opt/flink/lib/
  docker cp iceberg-aws-bundle-1.5.0.jar         $CONTAINER:/opt/flink/lib/
  docker cp flink-shaded-hadoop-2-uber-2.8.3-10.0.jar $CONTAINER:/opt/flink/lib/
done
```

---

## 5.2 Khởi tạo database MySQL

```sql
docker exec -i mysql mysql -uroot -p123

CREATE DATABASE IF NOT EXISTS hrm;
USE hrm;

CREATE TABLE IF NOT EXISTS employees (
  emp_id     INT PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  department VARCHAR(100) NOT NULL,
  salary     DECIMAL(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS attendance (
  id        INT PRIMARY KEY AUTO_INCREMENT,
  emp_id    INT NOT NULL,
  check_in  DATETIME NOT NULL,
  check_out DATETIME,
  status    VARCHAR(50)
);

INSERT INTO employees VALUES
  (1, 'Nguyen Van A', 'Engineering', 25000000),
  (2, 'Tran Thi B',   'Marketing',   20000000),
  (3, 'Le Van C',     'Engineering', 28000000);

INSERT INTO attendance (emp_id, check_in, check_out) VALUES
  (1, '2026-04-01 08:00:00', '2026-04-01 17:30:00'),
  (2, '2026-04-01 07:55:00', '2026-04-01 17:00:00');
```

---

## 5.3 Cấu hình Debezium CDC Connector

### 5.3.1 Đăng ký MySQL Connector
```bash
curl -X POST http://localhost:8083/connectors/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "hrm-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "mysql",
      "database.port": "3306",
      "database.user": "root",
      "database.password": "123",
      "database.server.id": "184055",
      "topic.prefix": "hrm",
      "database.include.list": "hrm",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schemahistory.hrm"
    }
  }'
```

### 5.3.2 Kiểm tra trạng thái connector
```bash
curl -s http://localhost:8083/connectors/hrm-connector/status | python3 -m json.tool
# Kết quả mong đợi: "state": "RUNNING"
```

Kafka Topics được tạo tự động:
- `hrm.hrm.employees` → CDC events từ bảng employees
- `hrm.hrm.attendance` → CDC events từ bảng attendance

---

## 5.4 Tạo Bucket MinIO

### 5.4.1 Tải MinIO Client (mc)
```bash
wget -q https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
docker cp mc minio:/tmp/mc
```

### 5.4.2 Tạo bucket hrm
```bash
docker exec minio sh -c "
  /tmp/mc alias set local http://localhost:9000 admin password --api S3v4
  /tmp/mc mb local/hrm --ignore-existing
"
```

Cấu trúc thư mục sau khi pipeline chạy:
```
s3://hrm/
└── iceberg-data/
    └── db_hrm/
        ├── employees/
        │   ├── data/       ← file .parquet
        │   └── metadata/   ← snapshot, metadata.json
        └── attendance_analytics/
            ├── data/
            └── metadata/
```

---

## 5.5 Tạo Iceberg Namespace

Sau khi HMS đã chạy (kiểm tra port 9083), tạo schema qua Trino:

```bash
# Kiểm tra HMS sẵn sàng
docker exec hive-metastore bash -c "echo > /dev/tcp/localhost/9083" && echo "HMS OK"

# Tạo namespace iceberg.db_hrm
docker exec trino trino --user admin --execute "
  CREATE SCHEMA IF NOT EXISTS iceberg.db_hrm
  WITH (location = 's3a://hrm/iceberg-data/')
"
```

---

## 5.6 Cấu hình Trino kết nối HMS

### 5.6.1 Tạo file cấu hình
```bash
mkdir -p trino-conf
cat > trino-conf/iceberg.properties << EOF
connector.name=iceberg
iceberg.catalog.type=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=password
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
hive.s3.region=us-east-1
EOF
```

### 5.6.2 Restart Trino để load config
```bash
docker restart trino
sleep 20

# Kiểm tra kết nối
docker exec trino trino --execute 'SHOW SCHEMAS FROM iceberg;'
# Kết quả mong đợi: "db_hrm"
```

---

## 5.7 Submit Flink SQL Pipeline

### 5.7.1 Tạo file SQL pipeline
```bash
cat > /tmp/hrm_pipeline.sql << 'EOF'
SET 'execution.checkpointing.interval' = '10s';

-- Tạo Iceberg Catalog kết nối HMS (Hive Metastore)
CREATE CATALOG hrm_catalog WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'hive',
  'uri'                  = 'thrift://hive-metastore:9083',
  'warehouse'            = 's3a://hrm/iceberg-data',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'password',
  'client.region'        = 'us-east-1'
);
```

### 5.7.2 Tạo bảng Iceberg đích
```sql
USE CATALOG hrm_catalog;
CREATE DATABASE IF NOT EXISTS db_hrm;

CREATE TABLE IF NOT EXISTS db_hrm.employees (
  emp_id INT, name STRING, department STRING, salary DOUBLE,
  PRIMARY KEY (emp_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS db_hrm.attendance_analytics (
  id INT, emp_id INT,
  check_in TIMESTAMP(3), check_out TIMESTAMP(3),
  work_hours DOUBLE, status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');
```

### 5.7.3 Tạo Kafka Source tables
```sql
USE CATALOG default_catalog;
USE default_database;

CREATE TABLE employees_kafka (
  emp_id INT, name STRING, department STRING, salary DOUBLE,
  PRIMARY KEY (emp_id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'hrm.hrm.employees',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-hrm-emp',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

-- QUAN TRỌNG: check_in/check_out là BIGINT (milliseconds)
-- vì Debezium gửi DATETIME dưới dạng Unix timestamp × 1000
CREATE TABLE attendance_kafka (
  id INT, emp_id INT,
  check_in BIGINT, check_out BIGINT,
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'hrm.hrm.attendance',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-hrm-att',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);
```

### 5.7.4 Submit streaming jobs
```sql
BEGIN STATEMENT SET;

-- Job 1: Sync employees từ MySQL → Iceberg
INSERT INTO hrm_catalog.db_hrm.employees
SELECT emp_id, name, department, salary FROM employees_kafka;

-- Job 2: Transform attendance với logic nghiệp vụ
INSERT INTO hrm_catalog.db_hrm.attendance_analytics
SELECT
  id, emp_id,
  TO_TIMESTAMP(FROM_UNIXTIME(check_in / 1000))  AS check_in,
  TO_TIMESTAMP(FROM_UNIXTIME(check_out / 1000)) AS check_out,
  CAST((check_out - check_in) / 3600000.0 AS DOUBLE) AS work_hours,
  CASE
    WHEN HOUR(TO_TIMESTAMP(FROM_UNIXTIME(check_in/1000))) > 8  THEN 'LATE'
    WHEN HOUR(TO_TIMESTAMP(FROM_UNIXTIME(check_out/1000))) < 17 THEN 'EARLY_LEAVE'
    ELSE 'ON_TIME'
  END AS status
FROM attendance_kafka
WHERE check_out IS NOT NULL;

END;
```

### 5.7.5 Chạy pipeline
```bash
docker cp /tmp/hrm_pipeline.sql flink-sql-client:/tmp/hrm_pipeline.sql

docker exec -i flink-sql-client \
  ./bin/sql-client.sh \
  -Djobmanager.rpc.address=flink-jobmanager \
  -Drest.address=flink-jobmanager \
  -Drest.port=8081 \
  -f /tmp/hrm_pipeline.sql
```

Kiểm tra job đang chạy tại Flink UI: http://localhost:8081
Sau ~30 giây (1 checkpoint interval), data sẽ xuất hiện trong Iceberg.

---

## 5.8 Khởi tạo Superset

### 5.8.1 Khởi tạo database Superset
```bash
docker exec superset superset db upgrade
docker exec superset superset fab create-admin \
  --username admin --password admin \
  --firstname Admin --lastname Admin \
  --email admin@admin.com
docker exec superset superset init
```

### 5.8.2 Cài Trino driver
```bash
docker exec superset /app/.venv/bin/pip install trino sqlalchemy-trino
docker restart superset
```

### 5.8.3 Kết nối Trino
Vào Superset tại http://localhost:8088 (admin/admin):
- Settings → Database Connections → + Database → Other
- SQLAlchemy URI: `trino://admin@trino:8080/iceberg`
- Nhấn Test Connection → Save

### 5.8.4 Tạo Dataset và Chart
```sql
SELECT
  e.name, e.department,
  COUNT(*) as so_ngay,
  ROUND(SUM(a.work_hours), 1) as tong_gio,
  COUNT(CASE WHEN a.status = 'LATE' THEN 1 END) as so_lan_muon
FROM iceberg.db_hrm.attendance_analytics a
JOIN iceberg.db_hrm.employees e ON a.emp_id = e.emp_id
GROUP BY e.name, e.department
ORDER BY tong_gio DESC;
```

---

## 5.9 Cấu hình OpenMetadata

### 5.9.1 Thêm MySQL Service
Vào http://localhost:8585 (admin/admin):
- Settings → Services → Database → + Add Service → MySQL
- Host And Port: `mysql:3306`
- Username: `root` / Password: `123`
- Database: `hrm`
- Nhấn Test Connection → Save → Add Ingestion → Run

### 5.9.2 Thêm Trino Service
- Settings → Services → Database → + Add Service → Trino
- Host And Port: `trino:8080`
- Catalog: `iceberg`
- Username: `admin` (Password để trống)
- Nhấn Save → Add Ingestion → Run

---

## 5.10 Kịch bản Demo Realtime

### 5.10.1 Insert dữ liệu demo
```bash
# Nhân viên đi muộn (check-in 09:15 > 08:00)
docker exec -i mysql mysql -uroot -p123 hrm <<SQL
INSERT INTO attendance (emp_id, check_in, check_out)
VALUES (1, '2026-04-06 09:15:00', '2026-04-06 18:00:00');
SQL

sleep 30

# Nhân viên về sớm (check-out 15:00 < 17:00)
docker exec -i mysql mysql -uroot -p123 hrm <<SQL
INSERT INTO attendance (emp_id, check_in, check_out)
VALUES (3, '2026-04-06 08:00:00', '2026-04-06 15:00:00');
SQL
```

### 5.10.2 Kiểm tra kết quả qua Trino
```bash
docker exec trino trino --execute \
  'SELECT emp_id, check_in, check_out, work_hours, status
   FROM iceberg.db_hrm.attendance_analytics
   ORDER BY id DESC LIMIT 5;'
```

---

## Script tự động hóa

Toàn bộ các bước từ 5.1 đến 5.7 được tự động hóa trong file `setup_hrm.sh`.

```bash
chmod +x setup_hrm.sh && ./setup_hrm.sh
```

Script tự động:
- Tải JAR và copy vào Flink containers
- Tạo schema MySQL và insert data mẫu
- Đăng ký Debezium connector
- Tạo bucket MinIO và namespace Iceberg
- Cập nhật Trino config
- Submit Flink SQL pipeline
