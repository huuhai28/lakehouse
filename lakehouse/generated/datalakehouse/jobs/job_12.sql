-- JOB 12 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`hcc_kpi_tong_quan_src` (
  `Tổng số hồ sơ tiếp nhận` INT
  `Tỷ lệ đồng bộ lên Cổng DVC Quốc gia (%)` DOUBLE
  `Số lượt đánh giá hài lòng` INT
  `ngày` INT
  , PRIMARY KEY (`Tổng số hồ sơ tiếp nhận`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.hcc_kpi_tong_quan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job12',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`ktxh_an_ninh_quoc_phong_src` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.ktxh_an_ninh_quoc_phong',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job12',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`ktxh_bo_loc_trang_thai_src` (
  `Trạng thái (Quý)` STRING
  , PRIMARY KEY (`Trạng thái (Quý)`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.ktxh_bo_loc_trang_thai',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job12',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`ktxh_do_thi_src` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.ktxh_do_thi',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job12',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`ktxh_du_an_src` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.ktxh_du_an',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job12',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`hcc_kpi_tong_quan`
SELECT `Tổng số hồ sơ tiếp nhận`, `Tỷ lệ đồng bộ lên Cổng DVC Quốc gia (%)`, `Số lượt đánh giá hài lòng`, `ngày`
FROM default_catalog.default_database.`hcc_kpi_tong_quan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`ktxh_an_ninh_quoc_phong`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_an_ninh_quoc_phong_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`ktxh_bo_loc_trang_thai`
SELECT `Trạng thái (Quý)`
FROM default_catalog.default_database.`ktxh_bo_loc_trang_thai_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`ktxh_do_thi`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_do_thi_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`ktxh_du_an`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_du_an_src`;
END;
