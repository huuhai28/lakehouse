# =============================================================
#  deploy_platform.sh — Iceberg Sink Connector (No Flink)
#  MySQL → Debezium → Kafka → Iceberg Sink → HMS/MinIO → Trino
# =============================================================

find . -maxdepth 1 -name "*.sh" | xargs sed -i 's/\r$//' 2>/dev/null || true

source "$(dirname "$0")/pipeline_common.sh"

if [ -f "../.env" ]; then source "../.env";
elif [ -f ".env" ]; then source ".env"; fi

PROJECT_NAME=${1:-$mysql_database}
mysql_database="$PROJECT_NAME"
PROJECT_LOWER=$(echo "$PROJECT_NAME" | tr '[:upper:]' '[:lower:]')
NAMESPACE="db_${PROJECT_LOWER}"
TOPIC_PREFIX="topic_${PROJECT_NAME}"
BUCKET=$(echo "${PROJECT_NAME}-lake" | tr '[:upper:]_' '[:lower:]-')
ICEBERG_PLUGIN_DIR="iceberg-kafka-connect"
SINK_NAME="${PROJECT_LOWER}-iceberg-sink"
SOURCE_NAME="${PROJECT_NAME}-connector-v9"

echo "============================================"
echo "  🚀 DEPLOYING: $PROJECT_NAME"
echo "  Mode: Debezium → Kafka → Iceberg Sink"
echo "============================================"

# ─── [1] ICEBERG SINK PLUGIN ─────────────────────────────────
echo ">>> [1] Kiểm tra Iceberg Sink plugin..."
if [ ! -d "$ICEBERG_PLUGIN_DIR" ]; then
    echo "  ⚠️  Chưa có thư mục $ICEBERG_PLUGIN_DIR!"
    echo "  Chạy:"
    echo "  wget https://github.com/tabular-io/iceberg-kafka-connect/releases/download/v0.6.15/iceberg-kafka-connect-0.6.15.zip"
    echo "  unzip iceberg-kafka-connect-0.6.15.zip -d iceberg-kafka-connect"
    exit 1
fi

# Kiểm tra bằng runtime JAR thay vì đếm file
SENTINEL_CHECK=$(docker exec debezium ls /kafka/connect/iceberg-kafka-connect/iceberg-kafka-connect-runtime*.jar 2>/dev/null | wc -l)
if [ "$SENTINEL_CHECK" -lt 1 ]; then
    echo "  ⬆️  Copy plugin vào Debezium..."
    docker cp "$ICEBERG_PLUGIN_DIR/." debezium:/kafka/connect/iceberg-kafka-connect/
    echo "  ✅ Plugin đã copy vào Debezium."
else
    echo "  ✅ Plugin đã có sẵn trong Debezium, bỏ qua copy."
fi

# ─── [1b] EXTRA JARs (cache local ~/lakehouse/jars/) ─────────
echo ">>> [1b] Đảm bảo Hive/Thrift JARs có trong plugin dir..."
EXTRA_JARS=(
    "https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar"
    "https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.9.3/libthrift-0.9.3.jar"
    "https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/3.1.3/hive-standalone-metastore-3.1.3.jar"
    "https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.4.0/woodstox-core-6.4.0.jar"
    "https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar"
)

mkdir -p ~/lakehouse/jars

for JAR_URL in "${EXTRA_JARS[@]}"; do
    JAR_FILE=$(basename "$JAR_URL")
    # Tải về local cache nếu chưa có
    if [ ! -f ~/lakehouse/jars/"$JAR_FILE" ]; then
        echo "  ⬇️  Tải $JAR_FILE về local cache..."
        wget -q -O ~/lakehouse/jars/"$JAR_FILE" "$JAR_URL" || {
            echo "  ⚠️ Không tải được $JAR_FILE, bỏ qua."
            continue
        }
    fi
    # Copy vào container nếu chưa có
    if docker exec debezium ls "/kafka/connect/iceberg-kafka-connect/$JAR_FILE" > /dev/null 2>&1; then
        echo "  ✅ $JAR_FILE (đã có)"
    else
        docker cp ~/lakehouse/jars/"$JAR_FILE" debezium:/kafka/connect/iceberg-kafka-connect/
        echo "  ✅ $JAR_FILE (copy từ local cache)"
    fi
done

# ─── [2] HMS ─────────────────────────────────────────────────
echo ">>> [2] Kiểm tra Hive Metastore..."
HMS_RUNNING=$(docker exec hive-metastore bash -c \
    "echo > /dev/tcp/localhost/9083" 2>/dev/null && echo "yes" || echo "no")
if [ "$HMS_RUNNING" = "yes" ]; then
    echo "  ✅ HMS đang chạy."
else
    echo "  ⚠️ HMS chưa chạy, khởi động..."
    docker compose up -d hive-metastore
    until docker exec hive-metastore bash -c \
        "echo > /dev/tcp/localhost/9083" 2>/dev/null; do
        echo -n "."; sleep 3
    done
    echo " OK!"
fi
HMS_IP=$(docker inspect hive-metastore \
    --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' | head -1)
echo "  HMS IP: $HMS_IP"

# ─── [3] RESTART DEBEZIUM (chỉ khi cần) ─────────────────────
echo ">>> [3] Kiểm tra plugin Debezium..."
PLUGIN_CHECK=$(curl -s http://localhost:8083/connector-plugins 2>/dev/null | python3 -c "
import sys,json
plugins = json.load(sys.stdin)
found = [p['class'] for p in plugins if 'iceberg' in p['class'].lower()]
print(found[0] if found else 'NOT_FOUND')
" 2>/dev/null || echo "NOT_FOUND")

if [ "$PLUGIN_CHECK" = "NOT_FOUND" ]; then
    echo "  Plugin chưa load, restart Debezium..."
    docker restart debezium > /dev/null
    until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
        echo -n "."; sleep 2
    done
    sleep 10
    PLUGIN_CHECK=$(curl -s http://localhost:8083/connector-plugins | python3 -c "
import sys,json
plugins = json.load(sys.stdin)
found = [p['class'] for p in plugins if 'iceberg' in p['class'].lower()]
print(found[0] if found else 'NOT_FOUND')
")
    if [ "$PLUGIN_CHECK" = "NOT_FOUND" ]; then
        echo "  ❌ Plugin chưa load được! Kiểm tra lại thư mục $ICEBERG_PLUGIN_DIR"
        exit 1
    fi
    echo " OK!"
else
    echo "  ✅ Plugin đã load sẵn, bỏ qua restart."
fi
echo "  ✅ Plugin: $PLUGIN_CHECK"

# ─── [4] CHỜ SERVICE ─────────────────────────────────────────
echo ">>> [4] Chờ các service..."
common::wait_for_kafka
common::wait_for_kafka_connect
common::wait_for_trino

# ─── [5] VỆ SINH ─────────────────────────────────────────────
echo ">>> [5] Vệ sinh dữ liệu cũ..."
RESET_MODE=${2:-""}

if [ "$RESET_MODE" = "--reset" ]; then
    echo "  🔴 CHẾ ĐỘ RESET TOÀN BỘ..."
    common::cleanup_debezium_connectors "$PROJECT_NAME"
    common::cleanup_schema_history "$PROJECT_NAME"

    echo "  🗑️  Xóa Kafka topics cũ..."
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | \
      grep -E "^(${TOPIC_PREFIX}|iceberg_v9\.${mysql_database})\." | \
      while read t; do
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$t" 2>/dev/null
        echo "  Deleted: $t"
      done

    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | \
      grep "^schemahistory\.${PROJECT_NAME}$" | \
      while read t; do
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$t" 2>/dev/null
        echo "  Deleted schemahistory: $t"
      done
    sleep 5

    common::cleanup_minio "$BUCKET"
    docker exec trino trino --user admin --no-progress --execute \
        "DROP SCHEMA IF EXISTS iceberg.${NAMESPACE}" 2>/dev/null && \
        echo "  ✅ Dropped schema." || echo "  ℹ️ Schema chưa tồn tại."
else
    echo "  🟢 Chế độ update — giữ data cũ..."
    common::cleanup_debezium_connectors "$PROJECT_NAME"
fi

# ─── [6] TẠO BUCKET + SCHEMA ─────────────────────────────────
echo ">>> [6] Tạo schema..."
docker exec minio mc alias set local http://localhost:9000 admin password > /dev/null 2>&1 || true
docker exec minio mc mb "local/${BUCKET}" --ignore-existing > /dev/null 2>&1 || true

docker exec trino trino --user admin --no-progress --execute \
    "CREATE SCHEMA IF NOT EXISTS iceberg.${NAMESPACE} \
     WITH (location = 's3a://${BUCKET}/iceberg-data/')" \
    2>/dev/null || true
echo "  ✅ Schema iceberg.${NAMESPACE} sẵn sàng."

# ─── [7] DEBEZIUM SOURCE ─────────────────────────────────────
echo ">>> [7] Đăng ký Debezium source connector..."

SCHEMA_EXISTS=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep "^schemahistory\.${PROJECT_NAME}$" | wc -l)

# register_source dùng SOURCE_NAME chính thức
register_source() {
    local MODE=$1
    curl -s -o /tmp/dbz_response.json -w "%{http_code}" \
        -X POST -H "Content-Type: application/json" \
        --data "{
  \"name\": \"${SOURCE_NAME}\",
  \"config\": {
    \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
    \"tasks.max\": \"1\",
    \"database.hostname\": \"${mysql_host}\",
    \"database.port\": \"3306\",
    \"database.user\": \"${mysql_user}\",
    \"database.password\": \"${mysql_password}\",
    \"database.server.id\": \"$((RANDOM + 10000))\",
    \"snapshot.mode\": \"${MODE}\",
    \"schema.history.internal.store.only.captured.tables.ddl\": \"true\",
    \"schema.history.internal.recover.with.incomplete.metadata\": \"true\",
    \"topic.prefix\": \"${TOPIC_PREFIX}\",
    \"database.include.list\": \"${mysql_database}\",
    \"table.include.list\": \"${mysql_database}.*\",
    \"decimal.handling.mode\": \"double\",
    \"schema.history.internal.kafka.bootstrap.servers\": \"kafka:9092\",
    \"schema.history.internal.kafka.topic\": \"schemahistory.${PROJECT_NAME}\",
    \"schema.history.internal.skip.unparseable.ddl\": \"true\"
  }
}" http://localhost:8083/connectors
}

wait_for_source_running() {
    for i in {1..20}; do
        STATE=$(curl -s http://localhost:8083/connectors/${SOURCE_NAME}/status \
            | python3 -c "
import sys,json; s=json.load(sys.stdin)
print(s['tasks'][0]['state'] if s.get('tasks') else 'STARTING')
" 2>/dev/null || echo "STARTING")
        echo -n "  [$i] $STATE"
        if [ "$STATE" = "RUNNING" ]; then echo " ✅"; return 0
        elif [ "$STATE" = "FAILED" ]; then
            echo " → Restart..."
            curl -s -X POST "http://localhost:8083/connectors/${SOURCE_NAME}/restart" > /dev/null
        else echo ""; fi
        sleep 3
    done
}

if [ "$SCHEMA_EXISTS" -gt 0 ]; then
    # schemahistory còn → streaming, không snapshot lại
    echo "  📚 Schemahistory còn, dùng when_needed"
    HTTP=$(register_source "when_needed")
    [ "$HTTP" = "201" ] && echo "  ✅ OK" || { echo "  ⚠️ HTTP $HTTP"; cat /tmp/dbz_response.json; exit 1; }
    wait_for_source_running
else
    # Database mới: dùng tên TẠM với initial để tránh offset conflict
    # Sau khi snapshot xong → xóa tên tạm → đăng ký tên chính thức với when_needed
    echo "  🆕 Database mới, dùng initial snapshot"
    TMP_NAME="${SOURCE_NAME}-init"

    curl -s -X DELETE "http://localhost:8083/connectors/${TMP_NAME}" > /dev/null 2>&1
    sleep 2

    HTTP=$(curl -s -o /tmp/dbz_response.json -w "%{http_code}" \
        -X POST -H "Content-Type: application/json" \
        --data "{
  \"name\": \"${TMP_NAME}\",
  \"config\": {
    \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
    \"tasks.max\": \"1\",
    \"database.hostname\": \"${mysql_host}\",
    \"database.port\": \"3306\",
    \"database.user\": \"${mysql_user}\",
    \"database.password\": \"${mysql_password}\",
    \"database.server.id\": \"$((RANDOM + 10000))\",
    \"snapshot.mode\": \"initial\",
    \"schema.history.internal.store.only.captured.tables.ddl\": \"true\",
    \"schema.history.internal.recover.with.incomplete.metadata\": \"true\",
    \"topic.prefix\": \"${TOPIC_PREFIX}\",
    \"database.include.list\": \"${mysql_database}\",
    \"table.include.list\": \"${mysql_database}.*\",
    \"decimal.handling.mode\": \"double\",
    \"schema.history.internal.kafka.bootstrap.servers\": \"kafka:9092\",
    \"schema.history.internal.kafka.topic\": \"schemahistory.${PROJECT_NAME}\",
    \"schema.history.internal.skip.unparseable.ddl\": \"true\"
  }
}" http://localhost:8083/connectors)
    [ "$HTTP" = "201" ] && echo "  ✅ initial OK (tmp)" || { echo "  ⚠️ HTTP $HTTP"; cat /tmp/dbz_response.json; exit 1; }

    # Chờ snapshot xong — RUNNING + có ít nhất 1 per-table topic
    echo "  ⏳ Chờ snapshot hoàn tất..."
    for i in {1..30}; do
        STATE=$(curl -s "http://localhost:8083/connectors/${TMP_NAME}/status" \
            | python3 -c "
import sys,json; s=json.load(sys.stdin)
print(s['tasks'][0]['state'] if s.get('tasks') else 'STARTING')
" 2>/dev/null || echo "STARTING")
        TOPIC_COUNT=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
            | grep -c "^${TOPIC_PREFIX}\." || true)
        echo -n "  [$i] $STATE | topics: $TOPIC_COUNT"
        if [ "$STATE" = "RUNNING" ] && [ "$TOPIC_COUNT" -gt 0 ]; then
            echo " ✅ Snapshot xong!"
            break
        elif [ "$STATE" = "FAILED" ]; then
            echo " ❌"
            curl -s "http://localhost:8083/connectors/${TMP_NAME}/status" | python3 -c "
import sys,json; s=json.load(sys.stdin)
print(s.get('tasks',[{}])[0].get('trace','')[-800:])
"
            exit 1
        else echo ""; fi
        sleep 5
    done

    # Xóa tên tạm, đăng ký tên chính thức với when_needed để streaming CDC
    echo "  🔄 Chuyển sang connector chính thức (when_needed)..."
    curl -s -X DELETE "http://localhost:8083/connectors/${TMP_NAME}" > /dev/null
    sleep 3
    HTTP=$(register_source "when_needed")
    [ "$HTTP" = "201" ] && echo "  ✅ OK" || { echo "  ⚠️ HTTP $HTTP"; cat /tmp/dbz_response.json; exit 1; }
    wait_for_source_running
fi

# ─── [8] ICEBERG SINK ────────────────────────────────────────
echo ">>> [8] Đăng ký Iceberg Sink Connector..."
curl -s -X DELETE http://localhost:8083/connectors/${SINK_NAME} > /dev/null 2>&1

HTTP=$(curl -s -o /tmp/sink_response.json -w "%{http_code}" \
    -X POST -H "Content-Type: application/json" \
    --data "{
  \"name\": \"${SINK_NAME}\",
  \"config\": {
    \"connector.class\": \"io.tabular.iceberg.connect.IcebergSinkConnector\",
    \"tasks.max\": \"1\",
    \"topics.regex\": \"iceberg_v9\\\\.${PROJECT_NAME}\\\\.(.*)\",
    \"iceberg.tables.dynamic-enabled\": \"true\",
    \"iceberg.tables.auto-create-enabled\": \"true\",
    \"iceberg.tables.route-field\": \"target_table\",
    \"iceberg.tables.default-namespace\": \"${NAMESPACE}\",
    \"iceberg.tables.upsert-mode-enabled\": \"false\",
    \"iceberg.tables.evolve-schema-enabled\": \"true\",
    \"iceberg.tables.schema-force-optional\": \"true\",
    \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
    \"value.converter.schemas.enable\": \"true\",
    \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
    \"key.converter.schemas.enable\": \"true\",
    \"iceberg.catalog.type\": \"hive\",
    \"iceberg.catalog.uri\": \"thrift://${HMS_IP}:9083\",
    \"iceberg.catalog.warehouse\": \"s3://${BUCKET}/iceberg-data\",
    \"iceberg.catalog.io-impl\": \"org.apache.iceberg.aws.s3.S3FileIO\",
    \"iceberg.catalog.s3.endpoint\": \"http://minio:9000\",
    \"iceberg.catalog.s3.access-key-id\": \"admin\",
    \"iceberg.catalog.s3.secret-access-key\": \"password\",
    \"iceberg.catalog.s3.path-style-access\": \"true\",
    \"iceberg.catalog.client.region\": \"us-east-1\",
    \"iceberg.control.topic\": \"control-iceberg-${PROJECT_LOWER}\",
    \"iceberg.control.commit.interval-ms\": \"15000\",
    \"consumer.override.auto.offset.reset\": \"earliest\"
  }
}" http://localhost:8083/connectors)

[ "$HTTP" = "201" ] && echo "  ✅ Iceberg Sink OK." || { echo "  ⚠️ HTTP $HTTP"; cat /tmp/sink_response.json; }

# ─── [9] KIỂM TRA SINK ───────────────────────────────────────
echo ">>> [9] Đợi Sink connector RUNNING..."
sleep 20
for i in $(seq 1 6); do
    SINK_STATE=$(curl -s "http://localhost:8083/connectors/${SINK_NAME}/status" | python3 -c "
import sys,json; s=json.load(sys.stdin)
tasks = s.get('tasks', [])
states = [t['state'] for t in tasks]
print('RUNNING' if tasks and all(st=='RUNNING' for st in states) else ('FAILED' if 'FAILED' in states else 'STARTING'))
" 2>/dev/null || echo "STARTING")
    echo "  [$i] Sink: $SINK_STATE"
    if [ "$SINK_STATE" = "RUNNING" ]; then
        echo "  ✅ Sink RUNNING!"
        break
    elif [ "$SINK_STATE" = "FAILED" ]; then
        echo "  ❌ Sink FAILED. Log lỗi:"
        curl -s "http://localhost:8083/connectors/${SINK_NAME}/status" | python3 -c "
import sys,json; s=json.load(sys.stdin)
print(s.get('tasks',[{}])[0].get('trace','')[-1500:])
"
        break
    fi
    sleep 10
done

echo ""
echo "============================================"
echo "  ✅ $PROJECT_NAME DEPLOY XONG!"
echo "  Kafka UI : http://localhost:8089"
echo "  Trino    : http://localhost:8080"
echo "  MinIO    : http://localhost:9001"
echo ""
echo "  Chờ ~2 phút rồi kiểm tra:"
echo "  docker exec trino trino --user admin --execute \\"
echo "    \"SHOW TABLES IN iceberg.${NAMESPACE}\""
echo "============================================"

# Khởi động field.py
pkill -9 -f field.py || true
sleep 2
mkdir -p ~/lakehouse/logs
nohup python3 ~/lakehouse/field.py > ~/lakehouse/logs/sanitizer.log 2>&1 &
echo "  ✅ Field sanitizer started PID: $!"

# ─── [10] AUTO-RESTART SINK (sau khi field.py tạo xong iceberg topics) ──────
echo ">>> [10] Chờ field.py tạo iceberg topics roi restart Sink..."
ICEBERG_READY=0
for i in $(seq 1 36); do
    ICEBERG_COUNT=$(docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 --list 2>/dev/null \
        | grep -c "iceberg_v9\.${PROJECT_NAME}" || true)
    echo "  [$i] iceberg_v9.${PROJECT_NAME}.* topics: $ICEBERG_COUNT"
    if [ "$ICEBERG_COUNT" -gt 0 ]; then
        echo "  Topics san sang! Restart Sink..."
        curl -s -X POST "http://localhost:8083/connectors/${SINK_NAME}/restart" > /dev/null
        ICEBERG_READY=1
        break
    fi
    sleep 10
done
if [ "$ICEBERG_READY" -eq 0 ]; then
    echo "  Timeout! field.py chua tao xong topics. Restart Sink thu cong sau."
fi
sleep 40

# Kiểm tra kết quả cuối
TABLES=$(docker exec trino trino --user admin --no-progress --execute \
    "SHOW TABLES IN iceberg.${NAMESPACE}" 2>/dev/null | wc -l)
echo ""
echo "============================================"
if [ "$TABLES" -gt 0 ]; then
    echo "  ✅ PIPELINE HOÀN CHỈNH!"
    echo "  📊 Số bảng trong iceberg.${NAMESPACE}: $TABLES"
    echo ""
    echo "  Kết nối Superset với Trino:"
    echo "  trino://admin@<server_ip>:8080/iceberg"
else
    echo "  ⏳ Data đang sync... Chờ thêm 1-2 phút rồi kiểm tra:"
    echo "  docker exec trino trino --user admin --execute \\"
    echo "    \"SHOW TABLES IN iceberg.${NAMESPACE}\""
fi
echo "============================================"
