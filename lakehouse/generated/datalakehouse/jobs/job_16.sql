-- JOB 16 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`qldt_chart_loai_chung_cu_src` (
  `Loại chung cư` STRING
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Loại chung cư`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qldt_chart_loai_chung_cu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job16',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qldt_chart_luot_tham_gia_hoi_nghi_thang_src` (
  `ngay` INT
  `Tên cán bộ` STRING
  `Số hội nghị tham gia` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qldt_chart_luot_tham_gia_hoi_nghi_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job16',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qldt_chart_ti_le_ban_quan_ly_src` (
  `Ban quản lý` STRING
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản lý`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qldt_chart_ti_le_ban_quan_ly',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job16',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qldt_chart_ti_le_ban_quan_tri_src` (
  `Ban quản trị` STRING
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản trị`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qldt_chart_ti_le_ban_quan_tri',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job16',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qldt_kpi_tong_quan_src` (
  `Tổng số chung cư` BIGINT
  `Chung cư chưa có BQT` BIGINT
  `Số vụ tranh chấp` BIGINT
  `Tổng số hội nghị` BIGINT
  `Số cán bộ tham gia` BIGINT
  , PRIMARY KEY (`Tổng số chung cư`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qldt_kpi_tong_quan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job16',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qldt_chart_loai_chung_cu`
SELECT `Loại chung cư`, `Số lượng chung cư`
FROM default_catalog.default_database.`qldt_chart_loai_chung_cu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qldt_chart_luot_tham_gia_hoi_nghi_thang`
SELECT `ngay`, `Tên cán bộ`, `Số hội nghị tham gia`
FROM default_catalog.default_database.`qldt_chart_luot_tham_gia_hoi_nghi_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qldt_chart_ti_le_ban_quan_ly`
SELECT `Ban quản lý`, `Số lượng chung cư`
FROM default_catalog.default_database.`qldt_chart_ti_le_ban_quan_ly_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qldt_chart_ti_le_ban_quan_tri`
SELECT `Ban quản trị`, `Số lượng chung cư`
FROM default_catalog.default_database.`qldt_chart_ti_le_ban_quan_tri_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qldt_kpi_tong_quan`
SELECT `Tổng số chung cư`, `Chung cư chưa có BQT`, `Số vụ tranh chấp`, `Tổng số hội nghị`, `Số cán bộ tham gia`
FROM default_catalog.default_database.`qldt_kpi_tong_quan_src`;
END;
