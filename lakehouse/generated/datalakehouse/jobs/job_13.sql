-- JOB 13 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`ktxh_kinh_te_src` (
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
  'topic'='topic_datalakehouse.datalakehouse.ktxh_kinh_te',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job13',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`ktxh_kpi_tong_src` (
  `Kết quả đánh giá KPI` STRING
  `count` BIGINT
  , PRIMARY KEY (`Kết quả đánh giá KPI`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.ktxh_kpi_tong',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job13',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`ktxh_van_hoa_xa_hoi_src` (
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
  'topic'='topic_datalakehouse.datalakehouse.ktxh_van_hoa_xa_hoi',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job13',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`moitruong_kpi_tong_quan_src` (
  `Tổng rác thải thu gom (tấn)` DOUBLE
  `Tổng tiền xử phạt VPHC (đồng)` BIGINT
  `Tổng lượt tuyên truyền` INT
  `Số cơ sở cam kết an toàn PCCC` INT
  `ngày` INT
  , PRIMARY KEY (`Tổng tiền xử phạt VPHC (đồng)`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.moitruong_kpi_tong_quan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job13',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`moitruong_phan_anh_ihanoi_src` (
  `Loại phản ánh` STRING
  `Đã giải quyết` INT
  `Chưa giải quyết` INT
  `ngày` INT
  , PRIMARY KEY (`Loại phản ánh`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.moitruong_phan_anh_ihanoi',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job13',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`ktxh_kinh_te`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_kinh_te_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`ktxh_kpi_tong`
SELECT `Kết quả đánh giá KPI`, `count`
FROM default_catalog.default_database.`ktxh_kpi_tong_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`ktxh_van_hoa_xa_hoi`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_van_hoa_xa_hoi_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`moitruong_kpi_tong_quan`
SELECT `Tổng rác thải thu gom (tấn)`, `Tổng tiền xử phạt VPHC (đồng)`, `Tổng lượt tuyên truyền`, `Số cơ sở cam kết an toàn PCCC`, `ngày`
FROM default_catalog.default_database.`moitruong_kpi_tong_quan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`moitruong_phan_anh_ihanoi`
SELECT `Loại phản ánh`, `Đã giải quyết`, `Chưa giải quyết`, `ngày`
FROM default_catalog.default_database.`moitruong_phan_anh_ihanoi_src`;
END;
