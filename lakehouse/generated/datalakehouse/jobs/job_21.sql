-- JOB 21 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`yte_bar_phan_bo_canh_bao_src` (
  `Loại hình` STRING
  `Mức cảnh báo` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.yte_bar_phan_bo_canh_bao',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job21',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`yte_kpi_tong_hop_src` (
  `Tổng số cơ sở y tế ngoài công lập` BIGINT
  `Số cơ sở đạt chuẩn` BIGINT
  `Số cơ sở cần kiểm tra lại` BIGINT
  `Cơ sở dừng hoạt động` BIGINT
  , PRIMARY KEY (`Tổng số cơ sở y tế ngoài công lập`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.yte_kpi_tong_hop',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job21',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`yte_line_canh_bao_thang_src` (
  `Tháng` INT
  `Loại hình` STRING
  `Mức cảnh báo` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.yte_line_canh_bao_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job21',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`yte_pie_chat_luong_src` (
  `Trạng thái chất lượng` STRING
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Trạng thái chất lượng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.yte_pie_chat_luong',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job21',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`yte_bar_phan_bo_canh_bao`
SELECT `Loại hình`, `Mức cảnh báo`, `Số lượng`
FROM default_catalog.default_database.`yte_bar_phan_bo_canh_bao_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`yte_kpi_tong_hop`
SELECT `Tổng số cơ sở y tế ngoài công lập`, `Số cơ sở đạt chuẩn`, `Số cơ sở cần kiểm tra lại`, `Cơ sở dừng hoạt động`
FROM default_catalog.default_database.`yte_kpi_tong_hop_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`yte_line_canh_bao_thang`
SELECT `Tháng`, `Loại hình`, `Mức cảnh báo`, `Số lượng`
FROM default_catalog.default_database.`yte_line_canh_bao_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`yte_pie_chat_luong`
SELECT `Trạng thái chất lượng`, `Số lượng cơ sở`
FROM default_catalog.default_database.`yte_pie_chat_luong_src`;
END;
