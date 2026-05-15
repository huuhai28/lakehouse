-- JOB 3 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`congthuong_chart_nganh_nghe_kinh_doanh_src` (
  `Ngành_nghề` STRING
  `Số_lượng` BIGINT
  , PRIMARY KEY (`Ngành_nghề`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.congthuong_chart_nganh_nghe_kinh_doanh',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job3',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`congthuong_kpi_tong_quan_src` (
  `Tổng số Ki-ốt` BIGINT
  `Giấy phép kinh doanh rượu` BIGINT
  `Tổng số cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng số Ki-ốt`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.congthuong_kpi_tong_quan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job3',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`dash_chua_giai_ngan_thang_src` (
  `thang` STRING
  `tong_chua_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dash_chua_giai_ngan_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job3',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`dash_chua_gn_chi_linh_vuc_src` (
  `thang` STRING
  `tong_chua_giai_ngan` DOUBLE
  `tong_chua_gn_chi_linh_vuc` DOUBLE
  `tong_gn_linh_vuc` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dash_chua_gn_chi_linh_vuc',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job3',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`dash_co_cau_du_an_trang_thai_src` (
  `thang` STRING
  `trang_thai` STRING
  `so_du_an` BIGINT
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dash_co_cau_du_an_trang_thai',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job3',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`congthuong_chart_nganh_nghe_kinh_doanh`
SELECT `Ngành_nghề`, `Số_lượng`
FROM default_catalog.default_database.`congthuong_chart_nganh_nghe_kinh_doanh_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`congthuong_kpi_tong_quan`
SELECT `Tổng số Ki-ốt`, `Giấy phép kinh doanh rượu`, `Tổng số cơ sở kinh doanh`
FROM default_catalog.default_database.`congthuong_kpi_tong_quan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dash_chua_giai_ngan_thang`
SELECT `thang`, `tong_chua_giai_ngan`
FROM default_catalog.default_database.`dash_chua_giai_ngan_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dash_chua_gn_chi_linh_vuc`
SELECT `thang`, `tong_chua_giai_ngan`, `tong_chua_gn_chi_linh_vuc`, `tong_gn_linh_vuc`
FROM default_catalog.default_database.`dash_chua_gn_chi_linh_vuc_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dash_co_cau_du_an_trang_thai`
SELECT `thang`, `trang_thai`, `so_du_an`
FROM default_catalog.default_database.`dash_co_cau_du_an_trang_thai_src`;
END;
