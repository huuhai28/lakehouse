-- JOB 18 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_chart1_cocau3nganh_src` (
  `Phường` STRING
  `Lĩnh vực` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_chart1_cocau3nganh',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job18',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_chart2_gcn_src` (
  `Phường` STRING
  `Lĩnh vực` STRING
  `Cấp quản lý` STRING
  `Có GCN` BIGINT
  `Không GCN` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_chart2_gcn',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job18',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_chart3_camket_src` (
  `Phường` STRING
  `Lĩnh vực` STRING
  `Cấp quản lý` STRING
  `Có cam kết` BIGINT
  `Không cam kết` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_chart3_camket',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job18',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_chart4_loaihinh_yte_src` (
  `Phường` STRING
  `Cấp quản lý` STRING
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_chart4_loaihinh_yte',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job18',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`vsattp_chart5_loaihinh_congthuong_src` (
  `Phường` STRING
  `Cấp quản lý` STRING
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.vsattp_chart5_loaihinh_congthuong',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job18',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_chart1_cocau3nganh`
SELECT `Phường`, `Lĩnh vực`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart1_cocau3nganh_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_chart2_gcn`
SELECT `Phường`, `Lĩnh vực`, `Cấp quản lý`, `Có GCN`, `Không GCN`
FROM default_catalog.default_database.`vsattp_chart2_gcn_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_chart3_camket`
SELECT `Phường`, `Lĩnh vực`, `Cấp quản lý`, `Có cam kết`, `Không cam kết`
FROM default_catalog.default_database.`vsattp_chart3_camket_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_chart4_loaihinh_yte`
SELECT `Phường`, `Cấp quản lý`, `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart4_loaihinh_yte_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`vsattp_chart5_loaihinh_congthuong`
SELECT `Phường`, `Cấp quản lý`, `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart5_loaihinh_congthuong_src`;
END;
