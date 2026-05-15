-- JOB 10 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_ss_chi_dau_tu_src` (
  `nam` STRING
  `loai` STRING
  `du_toan` DOUBLE
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_ss_chi_dau_tu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job10',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_ss_chi_linh_vuc_src` (
  `nam` STRING
  `linh_vuc` STRING
  `du_toan` DOUBLE
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_ss_chi_linh_vuc',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job10',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_tong_so_ngan_sach_src` (
  `nam` STRING
  `tong_thu_ngan_sach` DOUBLE
  `thu_tren_dia_ban` DOUBLE
  `thu_tren_xa_phuong` DOUBLE
  `tong_chi_ngan_sach` DOUBLE
  `ty_le_thuc_hien_thu` DOUBLE
  `ty_le_giai_ngan` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_tong_so_ngan_sach',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job10',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_voluntary_structure_circle_src` (
  `project_id` STRING
  `Tên dự án` STRING
  `loai` STRING
  `so_luong` BIGINT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_voluntary_structure_circle',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job10',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_xu_huong_ngan_sach_src` (
  `nam` INT
  `thang` INT
  `trieu_vnd` DOUBLE
  `du_toan` DOUBLE
  `thuc_hien` DOUBLE
  `thoi_gian` INT
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_xu_huong_ngan_sach',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job10',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_ss_chi_dau_tu`
SELECT `nam`, `loai`, `du_toan`, `thuc_hien`
FROM default_catalog.default_database.`gpmb_ss_chi_dau_tu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_ss_chi_linh_vuc`
SELECT `nam`, `linh_vuc`, `du_toan`, `thuc_hien`
FROM default_catalog.default_database.`gpmb_ss_chi_linh_vuc_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_tong_so_ngan_sach`
SELECT `nam`, `tong_thu_ngan_sach`, `thu_tren_dia_ban`, `thu_tren_xa_phuong`, `tong_chi_ngan_sach`, `ty_le_thuc_hien_thu`, `ty_le_giai_ngan`
FROM default_catalog.default_database.`gpmb_tong_so_ngan_sach_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_voluntary_structure_circle`
SELECT `project_id`, `Tên dự án`, `loai`, `so_luong`
FROM default_catalog.default_database.`gpmb_voluntary_structure_circle_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_xu_huong_ngan_sach`
SELECT `nam`, `thang`, `trieu_vnd`, `du_toan`, `thuc_hien`, `thoi_gian`
FROM default_catalog.default_database.`gpmb_xu_huong_ngan_sach_src`;
END;
