#!/bin/bash
# =============================================================
#  setup_hdfs_flink.sh — Luồng 2: Flink Analytics cho DB hdfs
#
#  Luồng:
#    MySQL(hdfs) → Debezium → Kafka → Flink SQL → Iceberg(MinIO)
#
#  Cách dùng:
#    cd ~/Lakehouse/flink
#    bash setup_hdfs_flink.sh             # Deploy bình thường
#    bash setup_hdfs_flink.sh --reset     # Reset + deploy lại từ đầu
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$(dirname "$SCRIPT_DIR")/lakehouse/pipeline_common.sh"

# ─── CONFIG ───────────────────────────────────────────────────
DB_NAME="hdfs"
BUCKET="hdfs-lake"
TRINO_SCHEMA="db_hdfs"
TOPIC_PREFIX="topic_hdfs"
SOURCE_CONNECTOR="debezium-source-hdfs"
RESET_MODE=false

[[ "${1:-}" == "--reset" ]] && RESET_MODE=true

# Tables cần CDC
TABLES="Bang_tong_hop_cong_dan,Phan_tich_log,table_stats"

echo "============================================="
echo "  🚀 HDFS Flink Analytics Pipeline"
echo "  DB      : $DB_NAME"
echo "  Bucket  : $BUCKET"
echo "  Reset   : $RESET_MODE"
echo "============================================="

# ─── BƯỚC 1: Chờ services sẵn sàng ───────────────────────────
common::wait_for_kafka
common::wait_for_kafka_connect
common::wait_for_flink

# ─── BƯỚC 2: Reset (nếu có --reset) ──────────────────────────
if [ "$RESET_MODE" = true ]; then
  echo ""
  echo ">>> [Reset] Dọn dẹp toàn bộ..."

  # Cancel Flink jobs cũ
  common::cleanup_flink_jobs

  # Xóa Kafka topics
  TOPIC_LIST=(
    "${TOPIC_PREFIX}.${DB_NAME}.Bang_tong_hop_cong_dan"
    "${TOPIC_PREFIX}.${DB_NAME}.Phan_tich_log"
    "${TOPIC_PREFIX}.${DB_NAME}.table_stats"
    "schemahistory.${DB_NAME}_flink"
  )
  common::cleanup_kafka_topics "${TOPIC_LIST[@]}"

  # Xóa Debezium connector cũ
  common::cleanup_debezium_connectors "$SOURCE_CONNECTOR"

  # Dọn MinIO bucket
  common::cleanup_minio "$BUCKET"

  echo "  ✅ Reset hoàn tất."
fi

# ─── BƯỚC 3: Đăng ký Debezium Source Connector ───────────────
echo ""
echo ">>> [Debezium] Đăng ký MySQL Source Connector cho DB '$DB_NAME'..."

DEBEZIUM_JSON=$(cat <<EOF
{
  "name": "${SOURCE_CONNECTOR}",
  "config": {
    "connector.class"                   : "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname"                 : "mysql",
    "database.port"                     : "3306",
    "database.user"                     : "root",
    "database.password"                 : "123",
    "database.server.id"                : "__ID__",
    "database.server.name"              : "${TOPIC_PREFIX}",
    "database.include.list"             : "${DB_NAME}",
    "table.include.list"                : "${DB_NAME}.Bang_tong_hop_cong_dan,${DB_NAME}.Phan_tich_log,${DB_NAME}.table_stats",
    "topic.prefix"                      : "${TOPIC_PREFIX}",
    "schema.history.internal.kafka.bootstrap.servers" : "kafka:9092",
    "schema.history.internal.kafka.topic"             : "schemahistory.${DB_NAME}_flink",
    "snapshot.mode"                     : "initial",
    "include.schema.changes"            : "false",
    "decimal.handling.mode"             : "double",
    "time.precision.mode"               : "connect",
    "tombstones.on.delete"              : "false",
    "transforms"                        : "unwrap",
    "transforms.unwrap.type"            : "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones" : "true"
  }
}
EOF
)

common::register_debezium "$DEBEZIUM_JSON"

# Chờ topic đầu tiên xuất hiện
common::wait_for_kafka_topic "${TOPIC_PREFIX}.${DB_NAME}.Bang_tong_hop_cong_dan"

echo "  ✅ Debezium đang capture data từ ${DB_NAME}."

# ─── BƯỚC 4: Tạo MinIO bucket + Trino schema ─────────────────
echo ""
echo ">>> [MinIO] Tạo bucket '$BUCKET'..."
common::cleanup_minio "$BUCKET"   # ensure bucket tồn tại (hàm này safe với --ignore-existing)

echo ""
echo ">>> [Trino] Tạo schema iceberg.${TRINO_SCHEMA}..."
docker exec trino trino --user admin --execute "
  CREATE SCHEMA IF NOT EXISTS iceberg.${TRINO_SCHEMA}
  WITH (location = 's3a://${BUCKET}/iceberg-data/')
" && echo "  ✅ Schema iceberg.${TRINO_SCHEMA} sẵn sàng."

# ─── BƯỚC 5: Copy JARs vào Flink (nếu chưa có) ───────────────
echo ""
echo ">>> [JAR] Kiểm tra và copy JARs vào Flink containers..."
JAR_DIR="$(dirname "$SCRIPT_DIR")/lakehouse"
REQUIRED_JARS=(
  "iceberg-flink-runtime-1.18-1.5.0.jar"
  "iceberg-aws-bundle-1.5.0.jar"
  "flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
  "flink-sql-connector-kafka-3.0.1-1.18.jar"
  "hive-exec-3.1.3.jar"
)

for JAR in "${REQUIRED_JARS[@]}"; do
  JAR_PATH="$JAR_DIR/$JAR"
  if [ -f "$JAR_PATH" ]; then
    for CONTAINER in flink-jobmanager flink-taskmanager flink-sql-client; do
      docker cp "$JAR_PATH" "$CONTAINER:/opt/flink/lib/" 2>/dev/null || true
    done
    echo "  ✅ Copied: $JAR"
  else
    echo "  ⚠️  Không tìm thấy: $JAR_PATH (có thể đã có trong container)"
  fi
done

# Restart Flink để load JARs mới
echo ">>> [Flink] Restart để load JARs..."
docker restart flink-jobmanager flink-taskmanager > /dev/null 2>&1
common::wait_for_flink

# ─── BƯỚC 6: Submit Flink SQL Jobs ───────────────────────────
echo ""
SQL_FILE="$SCRIPT_DIR/jobs/hdfs_analytics.sql"
common::submit_flink_sql "$SQL_FILE"

echo ""
echo "============================================="
echo "  ✅ HDFS Flink Analytics Pipeline đã chạy!"
echo ""
echo "  Kiểm tra Flink Jobs:"
echo "    http://localhost:8081"
echo ""
echo "  Kiểm tra kết quả qua Trino:"
echo "    docker exec trino trino --user admin --execute \\"
echo "      \"SHOW TABLES IN iceberg.${TRINO_SCHEMA}\""
echo ""
echo "  Truy vấn thống kê dân số theo huyện:"
echo "    docker exec trino trino --user admin --execute \\"
echo "      \"SELECT * FROM iceberg.${TRINO_SCHEMA}.stats_dan_so_theo_huyen ORDER BY tong_dan_so DESC\""
echo "============================================="
