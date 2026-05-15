-- JOB 11 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_xu_huong_thu_chi_src` (
  `nam` INT
  `thang` INT
  `thu` DOUBLE
  `chi` DOUBLE
  `thoi_gian` INT
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_xu_huong_thu_chi',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job11',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`hcc_chart_chi_tieu_src` (
  `Mã chỉ tiêu` INT
  `Nhóm chỉ tiêu` STRING
  `UBND Tây Hồ` DOUBLE
  `Tp Hà Nội` DOUBLE
  `Tỷ lệ (%)` DOUBLE
  `Năm` STRING
  `ngày` INT
  , PRIMARY KEY (`Mã chỉ tiêu`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.hcc_chart_chi_tieu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job11',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`hcc_chart_ho_so_tiep_nhan_src` (
  `Hình thức nộp hồ sơ` STRING
  `Số lượng hồ sơ` INT
  `Tỷ lệ (%)` DOUBLE
  `ngày` INT
  , PRIMARY KEY (`Hình thức nộp hồ sơ`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.hcc_chart_ho_so_tiep_nhan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job11',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`hcc_chart_ket_qua_giai_quyet_src` (
  `Trạng thái hồ sơ` STRING
  `Số lượng hồ sơ` INT
  `Ghi chú` STRING
  `ngày` INT
  , PRIMARY KEY (`Trạng thái hồ sơ`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.hcc_chart_ket_qua_giai_quyet',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job11',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`hcc_chart_loai_hinh_dich_vu_cong_src` (
  `Loại hình dịch vụ công` STRING
  `Số thủ tục` INT
  `Ghi chú` STRING
  `ngày` INT
  , PRIMARY KEY (`Loại hình dịch vụ công`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.hcc_chart_loai_hinh_dich_vu_cong',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job11',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_xu_huong_thu_chi`
SELECT `nam`, `thang`, `thu`, `chi`, `thoi_gian`
FROM default_catalog.default_database.`gpmb_xu_huong_thu_chi_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`hcc_chart_chi_tieu`
SELECT `Mã chỉ tiêu`, `Nhóm chỉ tiêu`, `UBND Tây Hồ`, `Tp Hà Nội`, `Tỷ lệ (%)`, `Năm`, `ngày`
FROM default_catalog.default_database.`hcc_chart_chi_tieu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`hcc_chart_ho_so_tiep_nhan`
SELECT `Hình thức nộp hồ sơ`, `Số lượng hồ sơ`, `Tỷ lệ (%)`, `ngày`
FROM default_catalog.default_database.`hcc_chart_ho_so_tiep_nhan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`hcc_chart_ket_qua_giai_quyet`
SELECT `Trạng thái hồ sơ`, `Số lượng hồ sơ`, `Ghi chú`, `ngày`
FROM default_catalog.default_database.`hcc_chart_ket_qua_giai_quyet_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`hcc_chart_loai_hinh_dich_vu_cong`
SELECT `Loại hình dịch vụ công`, `Số thủ tục`, `Ghi chú`, `ngày`
FROM default_catalog.default_database.`hcc_chart_loai_hinh_dich_vu_cong_src`;
END;
