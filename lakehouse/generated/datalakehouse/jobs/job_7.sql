-- JOB 7 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_chi_dau_tu_phat_trien_src` (
  `nam` INT
  `loai_chi_dau_tu` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_chi_dau_tu_phat_trien',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job7',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_chi_theo_linh_vuc_src` (
  `nam` INT
  `linh_vuc` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_chi_theo_linh_vuc',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job7',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_co_cau_chi_ngan_sach_src` (
  `nam` STRING
  `loai_chi` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_co_cau_chi_ngan_sach',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job7',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_co_cau_giai_ngan_src` (
  `nam` INT
  `trang_thai` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_co_cau_giai_ngan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job7',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_co_cau_nguon_thu_src` (
  `nam` STRING
  `loai_nguon_thu` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_co_cau_nguon_thu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job7',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_chi_dau_tu_phat_trien`
SELECT `nam`, `loai_chi_dau_tu`, `so_lieu`
FROM default_catalog.default_database.`gpmb_chi_dau_tu_phat_trien_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_chi_theo_linh_vuc`
SELECT `nam`, `linh_vuc`, `so_lieu`
FROM default_catalog.default_database.`gpmb_chi_theo_linh_vuc_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_co_cau_chi_ngan_sach`
SELECT `nam`, `loai_chi`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_chi_ngan_sach_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_co_cau_giai_ngan`
SELECT `nam`, `trang_thai`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_giai_ngan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_co_cau_nguon_thu`
SELECT `nam`, `loai_nguon_thu`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_nguon_thu_src`;
END;
