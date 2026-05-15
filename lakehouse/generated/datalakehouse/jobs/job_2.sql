-- JOB 2 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`cdt_kpi_tong_quan_src` (
  `Mã dự án` STRING
  `ky_yyyymm` INT
  `ky_nam` INT
  `ky_thang` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Trạng thái dự án` STRING
  `Là tháng mới nhất trong năm` INT
  `Kế hoạch vốn` DOUBLE
  `Lũy kế giải ngân` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.cdt_kpi_tong_quan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job2',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`congthuong_chart_biendong_cap_phep_ruou_thang_src` (
  `Tháng` INT
  `Loại hình cấp phép` STRING
  `Số giấy phép` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.congthuong_chart_biendong_cap_phep_ruou_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job2',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`congthuong_chart_biendong_kios_thang_src` (
  `Tháng` INT
  `Đơn_vị_quản_lý` STRING
  `Lĩnh vực` STRING
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.congthuong_chart_biendong_kios_thang',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job2',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`congthuong_chart_cocau_cap_phep_ruou_src` (
  `Loại_hình_cấp_phép` STRING
  `Số_cơ_sở` BIGINT
  , PRIMARY KEY (`Loại_hình_cấp_phép`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.congthuong_chart_cocau_cap_phep_ruou',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job2',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`congthuong_chart_cocau_kios_src` (
  `Đơn_vị_quản_lý` STRING
  `Lĩnh vực` STRING
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Đơn_vị_quản_lý`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.congthuong_chart_cocau_kios',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job2',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`cdt_kpi_tong_quan`
SELECT `Mã dự án`, `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Trạng thái dự án`, `Là tháng mới nhất trong năm`, `Kế hoạch vốn`, `Lũy kế giải ngân`
FROM default_catalog.default_database.`cdt_kpi_tong_quan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`congthuong_chart_biendong_cap_phep_ruou_thang`
SELECT `Tháng`, `Loại hình cấp phép`, `Số giấy phép`
FROM default_catalog.default_database.`congthuong_chart_biendong_cap_phep_ruou_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`congthuong_chart_biendong_kios_thang`
SELECT `Tháng`, `Đơn_vị_quản_lý`, `Lĩnh vực`, `Số lượng ki-ốt`
FROM default_catalog.default_database.`congthuong_chart_biendong_kios_thang_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`congthuong_chart_cocau_cap_phep_ruou`
SELECT `Loại_hình_cấp_phép`, `Số_cơ_sở`
FROM default_catalog.default_database.`congthuong_chart_cocau_cap_phep_ruou_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`congthuong_chart_cocau_kios`
SELECT `Đơn_vị_quản_lý`, `Lĩnh vực`, `Số lượng ki-ốt`
FROM default_catalog.default_database.`congthuong_chart_cocau_kios_src`;
END;
