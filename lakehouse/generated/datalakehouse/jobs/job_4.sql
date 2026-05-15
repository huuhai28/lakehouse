-- JOB 4 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`dash_tong_giai_ngan_thang_src` (
  `thang` STRING
  `tong_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dash_tong_giai_ngan_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job4',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`dtvh_chart_le_hoi_theo_thang_src` (
  `Tháng` INT
  `Số lượng lễ hội` INT
  `ngay` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dtvh_chart_le_hoi_theo_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job4',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`dtvh_chart_loai_hinh_di_tich_src` (
  `Loại hình di tích` STRING
  `Số lượng di tích` INT
  , PRIMARY KEY (`Loại hình di tích`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dtvh_chart_loai_hinh_di_tich',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job4',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`dtvh_chart_nghe_nhan_theo_loai_hinh_src` (
  `Loại hình di sản` STRING
  `Số lượng nghệ nhân` INT
  , PRIMARY KEY (`Loại hình di sản`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dtvh_chart_nghe_nhan_theo_loai_hinh',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job4',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`dtvh_chart_xep_hang_di_tich_src` (
  `Cấp xếp hạng` STRING
  `Số lượng di tích` INT
  , PRIMARY KEY (`Cấp xếp hạng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dtvh_chart_xep_hang_di_tich',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job4',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dash_tong_giai_ngan_thang`
SELECT `thang`, `tong_giai_ngan`
FROM default_catalog.default_database.`dash_tong_giai_ngan_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dtvh_chart_le_hoi_theo_thang`
SELECT `Tháng`, `Số lượng lễ hội`, `ngay`
FROM default_catalog.default_database.`dtvh_chart_le_hoi_theo_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dtvh_chart_loai_hinh_di_tich`
SELECT `Loại hình di tích`, `Số lượng di tích`
FROM default_catalog.default_database.`dtvh_chart_loai_hinh_di_tich_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dtvh_chart_nghe_nhan_theo_loai_hinh`
SELECT `Loại hình di sản`, `Số lượng nghệ nhân`
FROM default_catalog.default_database.`dtvh_chart_nghe_nhan_theo_loai_hinh_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dtvh_chart_xep_hang_di_tich`
SELECT `Cấp xếp hạng`, `Số lượng di tích`
FROM default_catalog.default_database.`dtvh_chart_xep_hang_di_tich_src`;
END;
