-- JOB 14 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`moitruong_thao_do_src` (
  `Hạng mục` STRING
  `Số lượng` INT
  `Đơn vị` STRING
  `ngày` INT
  , PRIMARY KEY (`Hạng mục`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.moitruong_thao_do',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job14',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`moitruong_xu_phat_vphc_src` (
  `Loại vi phạm` STRING
  `Số trường hợp (vụ)` INT
  `Số tiền phạt (đồng)` BIGINT
  `ngày` INT
  , PRIMARY KEY (`Loại vi phạm`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.moitruong_xu_phat_vphc',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job14',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`noivu_chart_biendong_nhansu_thang_src` (
  `Nhóm đối tượng` STRING
  `Tháng` INT
  `Hình thức quản lý` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.noivu_chart_biendong_nhansu_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job14',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`noivu_chart_cocau_nhansu_src` (
  `Nhóm đối tượng` STRING
  `Biên chế nhà nước` INT
  `Lao động hợp đồng` INT
  `Tổng cộng` INT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.noivu_chart_cocau_nhansu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job14',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`noivu_chart_doitruong_chinhsach_src` (
  `Loại đối tượng` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại đối tượng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.noivu_chart_doitruong_chinhsach',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job14',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`moitruong_thao_do`
SELECT `Hạng mục`, `Số lượng`, `Đơn vị`, `ngày`
FROM default_catalog.default_database.`moitruong_thao_do_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`moitruong_xu_phat_vphc`
SELECT `Loại vi phạm`, `Số trường hợp (vụ)`, `Số tiền phạt (đồng)`, `ngày`
FROM default_catalog.default_database.`moitruong_xu_phat_vphc_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`noivu_chart_biendong_nhansu_thang`
SELECT `Nhóm đối tượng`, `Tháng`, `Hình thức quản lý`, `Số lượng`
FROM default_catalog.default_database.`noivu_chart_biendong_nhansu_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`noivu_chart_cocau_nhansu`
SELECT `Nhóm đối tượng`, `Biên chế nhà nước`, `Lao động hợp đồng`, `Tổng cộng`
FROM default_catalog.default_database.`noivu_chart_cocau_nhansu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`noivu_chart_doitruong_chinhsach`
SELECT `Loại đối tượng`, `Số lượng`
FROM default_catalog.default_database.`noivu_chart_doitruong_chinhsach_src`;
END;
