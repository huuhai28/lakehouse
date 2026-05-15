-- JOB 17 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`qlvb_bang_chi_tiet_don_vi_src` (
  `Tháng` INT
  `Bộ phận` STRING
  `VB đến - Hoàn thành` INT
  `VB đến - Quá hạn hoàn thành` INT
  `VB đến - Chưa hoàn thành` INT
  `VB đến - Quá hạn chưa hoàn thành` INT
  `VB đi - Chờ xử lý` INT
  `VB đi - Đã ban hành` INT
  `HS công việc - Chưa xử lý` INT
  `HS công việc - Đang xử lý` INT
  `HS công việc - Hoàn thành` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qlvb_bang_chi_tiet_don_vi',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job17',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qlvb_chart_ho_so_cong_viec_src` (
  `Tháng` INT
  `Trạng thái` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qlvb_chart_ho_so_cong_viec',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job17',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qlvb_chart_van_ban_den_src` (
  `Tháng` INT
  `Vai trò xử lý` STRING
  `Số lượng` INT
  `Tỷ lệ (%)` DOUBLE
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qlvb_chart_van_ban_den',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job17',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qlvb_chart_van_ban_di_src` (
  `Tháng` INT
  `Trạng thái` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qlvb_chart_van_ban_di',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job17',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qlvb_kpi_tong_hop_src` (
  `Tháng` INT
  `Mã KPI` STRING
  `Chỉ tiêu` STRING
  `Giá trị` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qlvb_kpi_tong_hop',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job17',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qlvb_bang_chi_tiet_don_vi`
SELECT `Tháng`, `Bộ phận`, `VB đến - Hoàn thành`, `VB đến - Quá hạn hoàn thành`, `VB đến - Chưa hoàn thành`, `VB đến - Quá hạn chưa hoàn thành`, `VB đi - Chờ xử lý`, `VB đi - Đã ban hành`, `HS công việc - Chưa xử lý`, `HS công việc - Đang xử lý`, `HS công việc - Hoàn thành`
FROM default_catalog.default_database.`qlvb_bang_chi_tiet_don_vi_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qlvb_chart_ho_so_cong_viec`
SELECT `Tháng`, `Trạng thái`, `Số lượng`
FROM default_catalog.default_database.`qlvb_chart_ho_so_cong_viec_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qlvb_chart_van_ban_den`
SELECT `Tháng`, `Vai trò xử lý`, `Số lượng`, `Tỷ lệ (%)`
FROM default_catalog.default_database.`qlvb_chart_van_ban_den_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qlvb_chart_van_ban_di`
SELECT `Tháng`, `Trạng thái`, `Số lượng`
FROM default_catalog.default_database.`qlvb_chart_van_ban_di_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qlvb_kpi_tong_hop`
SELECT `Tháng`, `Mã KPI`, `Chỉ tiêu`, `Giá trị`
FROM default_catalog.default_database.`qlvb_kpi_tong_hop_src`;
END;
