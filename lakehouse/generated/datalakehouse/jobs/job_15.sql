-- JOB 15 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`noivu_chart_hinhthuc_quanly_src` (
  `hinh_thuc_quan_ly` STRING
  `so_luong` BIGINT
  , PRIMARY KEY (`hinh_thuc_quan_ly`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.noivu_chart_hinhthuc_quanly',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job15',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`noivu_kpi_chinhsach_src` (
  `Tổng đối tượng chính sách` BIGINT
  , PRIMARY KEY (`Tổng đối tượng chính sách`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.noivu_kpi_chinhsach',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job15',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`noivu_kpi_nhansu_src` (
  `Tổng số nhân sự quản lý` BIGINT
  `Tổng biên chế nhà nước` BIGINT
  `Tổng lao động hợp đồng` BIGINT
  , PRIMARY KEY (`Tổng số nhân sự quản lý`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.noivu_kpi_nhansu',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job15',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qldt_chart_can_bo_tham_du_hoi_nghi_src` (
  `Cán bộ tham gia` STRING
  `Lần đầu` BIGINT
  `Thường niên` BIGINT
  `Đột xuất` BIGINT
  , PRIMARY KEY (`Cán bộ tham gia`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qldt_chart_can_bo_tham_du_hoi_nghi',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job15',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`qldt_chart_hoat_dong_quan_ly_thang_src` (
  `ngay` INT
  `Loại tổ chức` STRING
  `Số lượng hội nghị` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.qldt_chart_hoat_dong_quan_ly_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job15',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`noivu_chart_hinhthuc_quanly`
SELECT `hinh_thuc_quan_ly`, `so_luong`
FROM default_catalog.default_database.`noivu_chart_hinhthuc_quanly_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`noivu_kpi_chinhsach`
SELECT `Tổng đối tượng chính sách`
FROM default_catalog.default_database.`noivu_kpi_chinhsach_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`noivu_kpi_nhansu`
SELECT `Tổng số nhân sự quản lý`, `Tổng biên chế nhà nước`, `Tổng lao động hợp đồng`
FROM default_catalog.default_database.`noivu_kpi_nhansu_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qldt_chart_can_bo_tham_du_hoi_nghi`
SELECT `Cán bộ tham gia`, `Lần đầu`, `Thường niên`, `Đột xuất`
FROM default_catalog.default_database.`qldt_chart_can_bo_tham_du_hoi_nghi_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`qldt_chart_hoat_dong_quan_ly_thang`
SELECT `ngay`, `Loại tổ chức`, `Số lượng hội nghị`
FROM default_catalog.default_database.`qldt_chart_hoat_dong_quan_ly_thang_src`;
END;
