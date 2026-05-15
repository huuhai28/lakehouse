-- JOB 19 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_chart6_loaihinh_nongnghiep_src` (
  `Phường` STRING
  `Cấp quản lý` STRING
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_chart6_loaihinh_nongnghiep',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job19',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_kpi_cards_src` (
  `Phường` STRING
  `Tổng Cơ sở có giấy chứng nhận` BIGINT
  `Tổng Cơ sở có giấy cam kết` BIGINT
  `Tổng Cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_kpi_cards',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job19',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_kpi_chitieu_src` (
  `Tổng đơn vị kiểm tra` BIGINT
  `Đơn vị chuẩn VSATTP` BIGINT
  `Đơn vị thanh tra lại` BIGINT
  `Đơn vị dừng kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng đơn vị kiểm tra`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_kpi_chitieu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job19',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_line_canhbao_thang_src` (
  `Tháng kiểm tra` INT
  `Mức độ cảnh báo` STRING
  `count` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_line_canhbao_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job19',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_line_vipham_thang_src` (
  `Tháng kiểm tra` INT
  `sum(Giấy ĐKKD)` BIGINT
  `sum(GCN ATTP)` BIGINT
  `sum(Giấy KSK)` BIGINT
  `sum(Tập huấn ATTP)` BIGINT
  `sum(KQ XN nhanh)` BIGINT
  `sum(Hợp đồng nguyên liệu)` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_line_vipham_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job19',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_chart6_loaihinh_nongnghiep`
SELECT `Phường`, `Cấp quản lý`, `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart6_loaihinh_nongnghiep_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_kpi_cards`
SELECT `Phường`, `Tổng Cơ sở có giấy chứng nhận`, `Tổng Cơ sở có giấy cam kết`, `Tổng Cơ sở kinh doanh`
FROM default_catalog.default_database.`vsattp_kpi_cards_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_kpi_chitieu`
SELECT `Tổng đơn vị kiểm tra`, `Đơn vị chuẩn VSATTP`, `Đơn vị thanh tra lại`, `Đơn vị dừng kinh doanh`
FROM default_catalog.default_database.`vsattp_kpi_chitieu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_line_canhbao_thang`
SELECT `Tháng kiểm tra`, `Mức độ cảnh báo`, `count`
FROM default_catalog.default_database.`vsattp_line_canhbao_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_line_vipham_thang`
SELECT `Tháng kiểm tra`, `sum(Giấy ĐKKD)`, `sum(GCN ATTP)`, `sum(Giấy KSK)`, `sum(Tập huấn ATTP)`, `sum(KQ XN nhanh)`, `sum(Hợp đồng nguyên liệu)`
FROM default_catalog.default_database.`vsattp_line_vipham_thang_src`;
END;
