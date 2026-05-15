import re
import os
import sys

def map_type(mysql_type):
    t = mysql_type.upper()
    if "BIGINT" in t: return "BIGINT"
    if "INT" in t: return "INT"
    if "VARCHAR" in t: return "STRING"
    if "TEXT" in t: return "STRING"
    if "DATETIME" in t: return "BIGINT"
    if "TIMESTAMP" in t: return "BIGINT"
    if "DATE" in t: return "INT"
    if "DECIMAL" in t: return "DOUBLE"
    if "DOUBLE" in t: return "DOUBLE"
    if "FLOAT" in t: return "FLOAT"
    return "STRING"

def generate_pipeline(project_name, sql_file, catalog, namespace, topic_prefix, credential):
    if not os.path.exists(sql_file):
        print(f"❌ Không tìm thấy file SQL: {sql_file}")
        return

    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    tables = re.findall(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)\s*\((.*?)\);", content, re.DOTALL | re.IGNORECASE)

    output_dir = f"generated/{project_name}"
    os.makedirs(output_dir, exist_ok=True)

    flink_sql_path = f"{output_dir}/pipeline.sql"
    tables_list_path = f"{output_dir}/tables.list"

    table_names = [t[0] for t in tables]
    with open(tables_list_path, "w") as f:
        f.write("\n".join(table_names))

    with open(flink_sql_path, "w", encoding="utf-8") as f:
        f.write(f"-- GENERATED PIPELINE FOR PROJECT: {project_name}\n")
        f.write("SET 'execution.checkpointing.interval' = '60s';\n")
        f.write("SET 'table.exec.sink.upsert-materialize' = 'AUTO';\n\n")

        f.write(f"CREATE CATALOG {catalog} WITH (\n")
        f.write(f"  'type'                 = 'iceberg',\n")
        f.write(f"  'catalog-impl'         = 'org.apache.iceberg.rest.RESTCatalog',\n")
        f.write(f"  'uri'                  = 'http://polaris:8181/api/catalog',\n")
        f.write(f"  'credential'           = '{credential}',\n")
        f.write(f"  'warehouse'            = '{catalog}',\n")
        f.write(f"  'header.X-Polaris-Realm' = 'POLARIS',\n")
        f.write(f"  'scope'                = 'PRINCIPAL_ROLE:ALL',\n")
        f.write(f"  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',\n")
        f.write(f"  's3.endpoint'          = 'http://minio:9000',\n")
        f.write(f"  's3.access-key-id'     = 'admin',\n")
        f.write(f"  's3.secret-access-key' = 'password'\n);\n")
        f.write(f"USE CATALOG {catalog};\n")
        f.write(f"CREATE DATABASE IF NOT EXISTS {namespace};\n\n")

        for table_name, body in tables:
            f.write(f"DROP TABLE IF EXISTS {namespace}.{table_name};\n")

            schema = []
            cols = []
            pk = "id"

            raw_lines = re.split(r",\s*(?![^()]*\))", body)

            for line in raw_lines:
                line = line.strip()
                if not line or line.startswith(("--", "/*", "CONSTRAINT", "INDEX", "KEY", "UNIQUE")):
                    if "PRIMARY KEY" in line.upper() and "(" in line:
                        pk_match = re.search(r"PRIMARY KEY\s*\((.*?)\)", line, re.I)
                        if pk_match: pk = pk_match.group(1).replace("`","").strip()
                    continue

                parts = line.split()
                if len(parts) < 2: continue

                col_name = parts[0].replace("`","")
                if "PRIMARY" in line.upper() and "KEY" in line.upper():
                    pk = col_name

                raw_type = " ".join(parts[1:])
                col_type = map_type(raw_type)
                schema.append({"name": col_name, "type": col_type})
                cols.append(f"  `{col_name}` {col_type}")

            # 1. Sink Table (Iceberg)
            f.write(f"DROP TABLE IF EXISTS {namespace}.`{table_name}`;\n")
            f.write(f"CREATE TABLE IF NOT EXISTS {namespace}.`{table_name}` (\n" + ",\n".join(cols) + f"\n  , PRIMARY KEY (`{pk}`) NOT ENFORCED\n) WITH ('write.upsert.enabled'='true','format-version'='2');\n\n")

            # 2. Kafka Source Table
            f.write(f"DROP TABLE IF EXISTS default_catalog.default_database.`{table_name}_src`;\n")
            f.write(f"CREATE TABLE IF NOT EXISTS default_catalog.default_database.`{table_name}_src` (\n" + ",\n".join(cols) + f"\n  , PRIMARY KEY (`{pk}`) NOT ENFORCED\n) WITH (\n")
            f.write(f"  'connector'                    = 'kafka',\n")
            f.write(f"  'topic'                        = '{topic_prefix}.{project_name}.{table_name}',\n")
            f.write(f"  'properties.bootstrap.servers' = 'kafka:9092',\n")
            f.write(f"  'properties.group.id'          = 'flink-{project_name}-{table_name}',\n")
            f.write(f"  'scan.startup.mode'            = 'earliest-offset',\n")
            f.write(f"  'format'                       = 'debezium-json',\n")
            f.write(f"  'debezium-json.schema-include' = 'true'\n);\n\n")

            # 3. Pipeline
            f.write(f"INSERT INTO {catalog}.{namespace}.`{table_name}`\nSELECT * FROM default_catalog.default_database.`{table_name}_src`;\n\n")
            f.write("-" * 50 + "\n")

    print(f"✅ Đã tạo xong Pipeline cho {project_name} tại {flink_sql_path}")

    # ── Tạo Trino SQL (đã chuyển vào trong hàm để dùng được các biến) ──
    trino_sql_path = f"{output_dir}/trino.sql"
    with open(trino_sql_path, "w") as f:
        f.write(f"CREATE SCHEMA IF NOT EXISTS minio.{namespace} ")
        f.write(f"WITH (location = 's3://{catalog}/iceberg-data/{namespace}/');\n\n")

        for table_name, body in tables:
            cols = []
            for line in re.split(r",\s*(?![^()]*\))", body):
                line = line.strip()
                if not line or line.startswith(("--","CONSTRAINT","INDEX","KEY","UNIQUE","PRIMARY")): continue
                parts = line.split()
                if len(parts) < 2: continue
                col_name = parts[0].replace("`","")
                col_type = map_type(" ".join(parts[1:]))
                trino_type = {"INT":"INTEGER","BIGINT":"BIGINT","STRING":"VARCHAR",
                             "DOUBLE":"DOUBLE","FLOAT":"REAL"}.get(col_type, "VARCHAR")
                cols.append(f"  {col_name} {trino_type}")

            f.write(f"DROP TABLE IF EXISTS minio.{namespace}.{table_name};\n")
            f.write(f"CREATE TABLE minio.{namespace}.{table_name} (\n")
            f.write(",\n".join(cols))
            f.write(f"\n) WITH (\n")
            f.write(f"  external_location = 's3://{catalog}/iceberg-data/{namespace}/{table_name}/data/',\n")
            f.write(f"  format = 'PARQUET'\n);\n\n")

    print(f"✅ Đã tạo Trino SQL tại {trino_sql_path}")

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: python3 gen_platform_pipeline.py <project> <sql_file> <catalog> <namespace> <topic_prefix> <token>")
    else:
        generate_pipeline(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
