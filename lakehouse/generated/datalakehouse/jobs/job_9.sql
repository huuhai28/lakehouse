-- JOB 9 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_land_structure_src` (
  `project_id` STRING
  `Tên dự án` STRING
  `loai_dat` STRING
  `dien_tich` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_land_structure',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job9',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_line_timeline_src` (
  `project_id` STRING
  `ten_du_an` STRING
  `object_id` STRING
  `date` INT
  `step_no` INT
  `step_name` STRING
  `step_value` INT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_line_timeline',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job9',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_overview_src` (
  `project_id` STRING
  `Tên dự án` STRING
  `tong_du_an` INT
  `Số hộ GPMB (dự kiến)` INT
  `Diện tích GPMB (dự kiến)` DOUBLE
  `ten_doi_tuong` INT
  `Diện tích` DOUBLE
  `so_doi_tuong` STRING
  `Chi phí GPMB` STRING
  `TMĐT` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_overview',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job9',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_overview_2_src` (
  `project_id` STRING
  `Tên dự án` STRING
  `Chi phí GPMB` STRING
  `Diện tích thu hồi` DOUBLE
  `ten_doi_tuong` BIGINT
  `tong_du_an` INT
  `TMĐT` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_overview_2',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job9',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`gpmb_quy_mo_nguon_thu_src` (
  `nam` INT
  `thang` INT
  `trieu_vnd` DOUBLE
  `chuyen_nguon_nam_truoc` DOUBLE
  `thue_phi` DOUBLE
  `tien_su_dung_dat` DOUBLE
  `thu_bo_sung_cap_tren` DOUBLE
  `thu_khac` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.gpmb_quy_mo_nguon_thu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job9',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_land_structure`
SELECT `project_id`, `Tên dự án`, `loai_dat`, `dien_tich`
FROM default_catalog.default_database.`gpmb_land_structure_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_line_timeline`
SELECT `project_id`, `ten_du_an`, `object_id`, `date`, `step_no`, `step_name`, `step_value`
FROM default_catalog.default_database.`gpmb_line_timeline_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_overview`
SELECT `project_id`, `Tên dự án`, `tong_du_an`, `Số hộ GPMB (dự kiến)`, `Diện tích GPMB (dự kiến)`, `ten_doi_tuong`, `Diện tích`, `so_doi_tuong`, `Chi phí GPMB`, `TMĐT`
FROM default_catalog.default_database.`gpmb_overview_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_overview_2`
SELECT `project_id`, `Tên dự án`, `Chi phí GPMB`, `Diện tích thu hồi`, `ten_doi_tuong`, `tong_du_an`, `TMĐT`
FROM default_catalog.default_database.`gpmb_overview_2_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`gpmb_quy_mo_nguon_thu`
SELECT `nam`, `thang`, `trieu_vnd`, `chuyen_nguon_nam_truoc`, `thue_phi`, `tien_su_dung_dat`, `thu_bo_sung_cap_tren`, `thu_khac`
FROM default_catalog.default_database.`gpmb_quy_mo_nguon_thu_src`;
END;
