-- JOB 20 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_loai_hinh_src` (
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_loai_hinh',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job20',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_pie_canhbao_src` (
  `Mức độ cảnh báo` STRING
  `count` BIGINT
  , PRIMARY KEY (`Mức độ cảnh báo`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_pie_canhbao',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job20',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_pie_trangthai_src` (
  `Trạng thái` STRING
  `count` BIGINT
  , PRIMARY KEY (`Trạng thái`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_pie_trangthai',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job20',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_pie_vipham_chitieu_src` (
  `Chỉ tiêu` STRING
  `Số lần vi phạm` BIGINT
  , PRIMARY KEY (`Chỉ tiêu`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_pie_vipham_chitieu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job20',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`yte_bar_loai_hinh_src` (
  `Loại hình` STRING
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.yte_bar_loai_hinh',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job20',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_loai_hinh`
SELECT `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_loai_hinh_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_pie_canhbao`
SELECT `Mức độ cảnh báo`, `count`
FROM default_catalog.default_database.`vsattp_pie_canhbao_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_pie_trangthai`
SELECT `Trạng thái`, `count`
FROM default_catalog.default_database.`vsattp_pie_trangthai_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_pie_vipham_chitieu`
SELECT `Chỉ tiêu`, `Số lần vi phạm`
FROM default_catalog.default_database.`vsattp_pie_vipham_chitieu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`yte_bar_loai_hinh`
SELECT `Loại hình`, `Số lượng cơ sở`
FROM default_catalog.default_database.`yte_bar_loai_hinh_src`;
END;
