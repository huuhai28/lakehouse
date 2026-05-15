-- JOB 8 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_co_cau_nguon_thu_db_src` (
  `nam` STRING
  `loai_nguon_thu` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_co_cau_nguon_thu_db',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job8',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_dong_ngan_sach_src` (
  `nam` INT
  `loai_ngan_sach` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_dong_ngan_sach',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job8',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_du_toan_nam_src` (
  `nam` STRING
  `loai` STRING
  `du_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_du_toan_nam',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job8',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_giaingan_project_src` (
  `Mã dự án` STRING
  `Tên dự án` STRING
  `Số tiền giải ngân thực tế` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_giaingan_project',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job8',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_handover_structure_circle_src` (
  `project_id` STRING
  `Tên dự án` STRING
  `loai` STRING
  `gia_tri` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_handover_structure_circle',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job8',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_co_cau_nguon_thu_db`
SELECT `nam`, `loai_nguon_thu`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_nguon_thu_db_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_dong_ngan_sach`
SELECT `nam`, `loai_ngan_sach`, `so_lieu`
FROM default_catalog.default_database.`gpmb_dong_ngan_sach_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_du_toan_nam`
SELECT `nam`, `loai`, `du_lieu`
FROM default_catalog.default_database.`gpmb_du_toan_nam_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_giaingan_project`
SELECT `Mã dự án`, `Tên dự án`, `Số tiền giải ngân thực tế`
FROM default_catalog.default_database.`gpmb_giaingan_project_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_handover_structure_circle`
SELECT `project_id`, `Tên dự án`, `loai`, `gia_tri`
FROM default_catalog.default_database.`gpmb_handover_structure_circle_src`;
END;
