-- JOB 1 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`cdt_chart_bar_linh_vuc_src` (
  `Lĩnh vực` STRING
  `ky_yyyymm` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Tổng chi đầu tư (Triệu)` DOUBLE
  , PRIMARY KEY (`Lĩnh vực`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.cdt_chart_bar_linh_vuc',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job1',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`cdt_chart_bar_tong_dau_tu_src` (
  `Tên dự án` STRING
  `Tổng tiền đầu tư (Tỷ đồng)` DOUBLE
  `Trạng thái dự án` STRING
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Lĩnh vực` STRING
  `ky_yyyymm` INT
  , PRIMARY KEY (`Tên dự án`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.cdt_chart_bar_tong_dau_tu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job1',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`cdt_chart_danh_sach_src` (
  `Mã dự án` STRING
  `Tên dự án` STRING
  `ky_yyyymm` INT
  `ky_nam` INT
  `ky_thang` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Giải ngân KQ` DOUBLE
  `Giải ngân GPMB` DOUBLE
  `Lũy kế giải ngân` DOUBLE
  `Tỉ lệ giải ngân (%)` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.cdt_chart_danh_sach',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job1',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`cdt_chart_pie_giai_ngan_src` (
  `ky_yyyymm` INT
  `ky_nam` INT
  `ky_thang` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Loại giải ngân` STRING
  `Giá trị` DOUBLE
  , PRIMARY KEY (`ky_yyyymm`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.cdt_chart_pie_giai_ngan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job1',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`cdt_chart_pie_trang_thai_src` (
  `Mã dự án` STRING
  `Trạng thái dự án` STRING
  `ky_yyyymm` INT
  `ky_nam` INT
  `ky_thang` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.cdt_chart_pie_trang_thai',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job1',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`cdt_chart_bar_linh_vuc`
SELECT `Lĩnh vực`, `ky_yyyymm`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Tổng chi đầu tư (Triệu)`
FROM default_catalog.default_database.`cdt_chart_bar_linh_vuc_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`cdt_chart_bar_tong_dau_tu`
SELECT `Tên dự án`, `Tổng tiền đầu tư (Tỷ đồng)`, `Trạng thái dự án`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Lĩnh vực`, `ky_yyyymm`
FROM default_catalog.default_database.`cdt_chart_bar_tong_dau_tu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`cdt_chart_danh_sach`
SELECT `Mã dự án`, `Tên dự án`, `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Giải ngân KQ`, `Giải ngân GPMB`, `Lũy kế giải ngân`, `Tỉ lệ giải ngân (%)`
FROM default_catalog.default_database.`cdt_chart_danh_sach_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`cdt_chart_pie_giai_ngan`
SELECT `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Loại giải ngân`, `Giá trị`
FROM default_catalog.default_database.`cdt_chart_pie_giai_ngan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`cdt_chart_pie_trang_thai`
SELECT `Mã dự án`, `Trạng thái dự án`, `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`
FROM default_catalog.default_database.`cdt_chart_pie_trang_thai_src`;
END;
