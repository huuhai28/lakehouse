#!/bin/bash
# Script drop và recreate tất cả Iceberg tables để áp dụng flat schema

set -e

echo "=========================================="
echo "RECREATE ICEBERG TABLES - FLAT SCHEMA"
echo "=========================================="

# 1. Stop field.py nếu đang chạy
echo "Step 1: Stopping field.py..."
pkill -f "python.*field.py" || true
sleep 2

# 2. Drop tất cả Iceberg tables
echo "Step 2: Dropping all Iceberg tables..."

docker exec -it trino trino --catalog iceberg --schema default --execute "
SHOW SCHEMAS IN iceberg;
" | grep -E '^db_' | while read schema; do
    echo "Processing schema: $schema"
    
    # Get all tables in schema
    docker exec -it trino trino --catalog iceberg --schema "$schema" --execute "
    SHOW TABLES;
    " | grep -v '^$' | while read table; do
        echo "  Dropping table: $schema.$table"
        docker exec -it trino trino --catalog iceberg --schema "$schema" --execute "
        DROP TABLE IF EXISTS $schema.$table;
        " || true
    done
done

echo "All Iceberg tables dropped."

# 3. Xóa Kafka topics iceberg_v9.*
echo "Step 3: Deleting Kafka topics iceberg_v9.*..."
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list | grep '^iceberg_v9\.' | while read topic; do
    echo "  Deleting topic: $topic"
    docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" || true
done

echo "Kafka topics deleted."

# 4. Reset Kafka Connect Iceberg Sink
echo "Step 4: Resetting Kafka Connect Iceberg Sink..."
curl -X DELETE http://localhost:8083/connectors/iceberg-sink-v9 || true
sleep 3

# Recreate connector với flat schema config
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "iceberg-sink-v9",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics.regex": "iceberg_v9\\..*",
    "iceberg.catalog.type": "hive",
    "iceberg.catalog.uri": "thrift://hive-metastore:9083",
    "iceberg.catalog.warehouse": "s3a://lakehouse/",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.s3.access-key-id": "minioadmin",
    "iceberg.catalog.s3.secret-access-key": "minioadmin",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.route-field": "target_table",
    "iceberg.tables.dynamic-enabled": "true",
    "iceberg.control-topic": "control-iceberg-v9",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "true"
  }
}'

echo "Iceberg Sink connector recreated."

# 5. Restart field.py với flat logic
echo "Step 5: Starting field.py with FLAT schema..."
cd /path/to/lakehouse
nohup python3 field.py > field.log 2>&1 &

echo "field.py started."

# 6. Monitor progress
echo "Step 6: Monitoring progress..."
echo "Waiting 30 seconds for data to flow..."
sleep 30

echo ""
echo "=========================================="
echo "RECREATE COMPLETE"
echo "=========================================="
echo ""
echo "Check status:"
echo "  - Kafka topics: docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list | grep iceberg_v9"
echo "  - Iceberg tables: docker exec -it trino trino --catalog iceberg --execute 'SHOW SCHEMAS;'"
echo "  - field.py log: tail -f lakehouse/field.log"
echo ""
echo "All tables now have FLAT schema - Superset can query directly!"
