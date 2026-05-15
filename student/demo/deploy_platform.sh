#!/bin/bash
# =============================================================
#  deploy_platform.sh — Trình quản lý triển khai vạn năng
#  Dựa trên pattern của setup_crm.sh đã chạy thành công
#  Cách dùng: ./deploy_platform.sh [Project_Name]
# =============================================================
set -e

# Tự động sửa lỗi định dạng dòng của Windows (CRLF -> LF) cho các file quan trọng
# Việc này giúp bạn không cần phải gõ 'sed' thủ công nữa
find . -maxdepth 1 -name "*.sh" -o -name "*.py" | xargs sed -i 's/\r$//' 2>/dev/null || true

source "$(dirname "$0")/pipeline_common.sh"

PROJECT_NAME=$1
if [ -z "$PROJECT_NAME" ]; then
    echo "❌ Lỗi: Cần tên Project (ví dụ: ./deploy_platform.sh hrm)"
    exit 1
fi

# ─── [1] CẤU HÌNH BIẾN THEO PROJECT ──────────────────────────
case "$PROJECT_NAME" in
  "hrm")
    SQL_SOURCE="sql/hrm_mysql.sql"
    CATALOG="hrm"           # ← bỏ prefix "catalog_"
    NAMESPACE="db_hrm"
    TOPIC_PREFIX="hrm_mysql"
    BUCKET="hrm"            # ← bỏ suffix "-lake"
    TABLE_LIST="hrm.employees,hrm.attendance,hrm.leave_requests,hrm.payroll"
    ;;
  "admin")
    SQL_SOURCE="sql/admin_schema.sql"
    CATALOG="admin"
    NAMESPACE="db_admin"
    TOPIC_PREFIX="admin_mysql"
    BUCKET="admin"
    TABLE_LIST="admin.*"
    ;;
esac


# ID ngẫu nhiên để schema history topic không bao giờ bị cache
RAND_ID=$(shuf -i 1000-9999 -n 1)

echo "============================================"
echo "  🚀 DEPLOYING PLATFORM PROJECT: $PROJECT_NAME"
echo "============================================"

# ─── [2] POLARIS TOKEN ───────────────────────────────────────
# Lấy token trước - CREDENTIAL và TOKEN sẽ được export
common::get_polaris_token

# ─── [3] SINH CODE SQL ───────────────────────────────────────
# Dùng CREDENTIAL (giống setup_crm.sh) để Flink xác thực với Polaris
echo ">>> [3] Đang sinh code Flink SQL tự động..."
python3 gen_platform_pipeline.py "$PROJECT_NAME" "$SQL_SOURCE" "$CATALOG" "$NAMESPACE" "$TOPIC_PREFIX" "$CREDENTIAL"

# ─── [4] TỔNG VỆ SINH (CLEANUP) ──────────────────────────────
echo ">>> [4] Đang tổng vệ sinh dữ liệu cũ (HARD RESET)..."
common::cleanup_flink_jobs

# Xóa metadata Polaris bằng REST API (Force Delete)
echo "  🧹 Đang quét và xóa triệt để metadata cũ trong Polaris..."
# 1. Lấy danh sách bảng hiện có trong namespace
TABLES_JSON=$(curl -s -X GET "http://localhost:8181/api/catalog/v1/${CATALOG}/namespaces/${NAMESPACE}/tables" \
     -H "Authorization: Bearer $TOKEN" -H "X-Polaris-Realm: POLARIS")

# 2. Xóa từng bảng bằng API (dùng ?purge=true nếu có thể)
echo "$TABLES_JSON" | grep -oP '"name":"\K[^"]+' | while read -r T; do
    echo "    🗑️  Ép xóa bảng: $T"
    curl -s -X DELETE "http://localhost:8181/api/catalog/v1/${CATALOG}/namespaces/${NAMESPACE}/tables/${T}?purge=true" \
         -H "Authorization: Bearer $TOKEN" -H "X-Polaris-Realm: POLARIS" > /dev/null 2>&1 || true
done

# 3. Xóa Namespace và Catalog
curl -s -X DELETE "http://localhost:8181/api/catalog/v1/${CATALOG}/namespaces/${NAMESPACE}" \
     -H "Authorization: Bearer $TOKEN" -H "X-Polaris-Realm: POLARIS" > /dev/null 2>&1 || true
curl -s -X DELETE "http://localhost:8181/api/management/v1/catalogs/${CATALOG}" \
     -H "Authorization: Bearer $TOKEN" -H "X-Polaris-Realm: POLARIS" > /dev/null 2>&1 || true

# Xóa MinIO và Debezium
common::cleanup_minio "$BUCKET"
common::cleanup_debezium_connectors "$PROJECT_NAME"

# Xóa Kafka topics (bao gồm schema history cũ)
echo "  🧹 Xóa Kafka topics cũ..."
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
  | grep -E "^(${TOPIC_PREFIX}|schemahistory\.${PROJECT_NAME})" \
  | while IFS= read -r TOPIC; do
      [ -z "$TOPIC" ] && continue
      docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" > /dev/null 2>&1 \
        && echo "    🗑️  Đã xóa topic: $TOPIC" || true
    done

# ─── [4b] TẠO CATALOG & NAMESPACE MỚI ──────────────────────
echo ">>> [4b] Tạo Catalog & Namespace..."
common::create_polaris_catalog "$TOKEN" "$CATALOG" "s3://$BUCKET/"

common::setup_polaris_grants "$TOKEN" "$CATALOG"

# Refresh token trước khi cập nhật Trino
echo "  🔄 Refresh token..."
common::get_polaris_token 

common::get_polaris_token
# Cập nhật Trino bằng TOKEN và cổng mặc định
common::update_trino_token "$TOKEN" "8282" "$CATALOG"  


docker exec trino trino --execute \
  > /dev/null 2>&1 || true

# ─── [5] DEBEZIUM ────────────────────────────────────────────
echo ">>> [5] Đăng ký Debezium..."
# Dùng ID ngẫu nhiên cho cả connector name và schema history topic
# → Debezium sẽ KHÔNG bao giờ tìm thấy offset cũ và bắt buộc phải chạy snapshot mới
DEBEZIUM_JSON="{
  \"name\": \"${PROJECT_NAME}-connector-${RAND_ID}\",
  \"config\": {
    \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
    \"tasks.max\": \"1\",
    \"database.hostname\": \"mysql\",
    \"database.port\": \"3306\",
    \"database.user\": \"root\",
    \"database.password\": \"123\",
    \"database.server.id\": \"${RAND_ID}\",
    \"snapshot.mode\": \"initial\",
    \"topic.prefix\": \"${TOPIC_PREFIX}\",
    \"database.include.list\": \"${PROJECT_NAME}\",
    \"table.include.list\": \"${TABLE_LIST}\",
    \"decimal.handling.mode\": \"double\",
    \"schema.history.internal.kafka.bootstrap.servers\": \"kafka:9092\",
    \"schema.history.internal.kafka.topic\": \"schemahistory.${PROJECT_NAME}.${RAND_ID}\"
  }
}"
common::register_debezium "$DEBEZIUM_JSON"

# Chờ topic đầu tiên xuất hiện (pattern từ setup_crm.sh)
FIRST_TOPIC="${TOPIC_PREFIX}.${PROJECT_NAME}.employees"
echo ">>> [5b] Đợi topic: ${FIRST_TOPIC}..."
for i in {1..5}; do
  if docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
     | grep -q "^${FIRST_TOPIC}$"; then
    echo "  ✅ Topic sẵn sàng sau ${i}x5s!"
    break
  fi
  echo "  ... lần $i/5..."
  sleep 5
done

# ─── [6] SUBMIT FLINK JOB ────────────────────────────────────
echo ">>> [6] Submit Flink Job..."
common::submit_flink_sql "generated/${PROJECT_NAME}/pipeline.sql"
echo ">>> [7] Chờ Flink checkpoint (90s)..."
sleep 90
echo ">>> [7b] Tạo Trino tables..."
docker exec trino trino --user admin \
  --execute "$(cat generated/${PROJECT_NAME}/trino.sql)" && \
  echo "  ✅ Trino tables sẵn sàng."
