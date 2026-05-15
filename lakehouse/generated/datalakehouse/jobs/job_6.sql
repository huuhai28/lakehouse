-- JOB 6 OF 21
CREATE CATALOG IF NOT EXISTS catalog_datalakehouse WITH (
  'type'='iceberg', 'catalog-type'='hive', 'uri'='thrift://hive-metastore:9083',
  'warehouse'='s3a://catalog_datalakehouse/iceberg-data', 'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000', 's3.path-style-access'='true',
  's3.access-key-id'='admin', 's3.secret-access-key'='password'
);
USE CATALOG catalog_datalakehouse;
CREATE DATABASE IF NOT EXISTS db_datalakehouse;
SET 'parallelism.default' = '1';

SET 'execution.checkpointing.interval' = '60s';

CREATE TEMPORARY TABLE default_catalog.default_database.`giaoduc_chart_quy_mo_hoc_sinh_src` (
  `Khối lớp` STRING
  `Cấp học` STRING
  `Học sinh nam` INT
  `Học sinh nữ` INT
  `Tổng học sinh` INT
  , PRIMARY KEY (`Khối lớp`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.giaoduc_chart_quy_mo_hoc_sinh',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job6',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`giaoduc_chart_trinh_do_giao_vien_src` (
  `Loại trường` STRING
  `Trình độ` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.giaoduc_chart_trinh_do_giao_vien',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job6',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`giaoduc_kpi_so_luong_truong_src` (
  `Loại trường` STRING
  `Chỉ tiêu` STRING
  `Giá trị` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.giaoduc_kpi_so_luong_truong',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job6',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`giaoduc_kpi_tong_giao_vien_src` (
  `Loại trường` STRING
  `Chức danh` STRING
  `Tổng số` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.giaoduc_kpi_tong_giao_vien',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job6',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb__pie_progress_src` (
  `snapshot_date` INT
  `project_id` STRING
  `ten_du_an` STRING
  `object_id` STRING
  `planned_finish_date` INT
  `actual_finish_date` INT
  `progress_status` STRING
  , PRIMARY KEY (`snapshot_date`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb__pie_progress',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job6',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`giaoduc_chart_quy_mo_hoc_sinh`
SELECT `Khối lớp`, `Cấp học`, `Học sinh nam`, `Học sinh nữ`, `Tổng học sinh`
FROM default_catalog.default_database.`giaoduc_chart_quy_mo_hoc_sinh_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`giaoduc_chart_trinh_do_giao_vien`
SELECT `Loại trường`, `Trình độ`, `Số lượng`
FROM default_catalog.default_database.`giaoduc_chart_trinh_do_giao_vien_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`giaoduc_kpi_so_luong_truong`
SELECT `Loại trường`, `Chỉ tiêu`, `Giá trị`
FROM default_catalog.default_database.`giaoduc_kpi_so_luong_truong_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`giaoduc_kpi_tong_giao_vien`
SELECT `Loại trường`, `Chức danh`, `Tổng số`
FROM default_catalog.default_database.`giaoduc_kpi_tong_giao_vien_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb__pie_progress`
SELECT `snapshot_date`, `project_id`, `ten_du_an`, `object_id`, `planned_finish_date`, `actual_finish_date`, `progress_status`
FROM default_catalog.default_database.`gpmb__pie_progress_src`;
END;
