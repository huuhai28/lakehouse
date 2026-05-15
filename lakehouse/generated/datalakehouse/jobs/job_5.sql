-- JOB 5 OF 21
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

CREATE TEMPORARY TABLE default_catalog.default_database.`dtvh_kpi_tong_quan_src` (
  `Di tích` INT
  `Di tích đã xếp hạng` INT
  `Nghệ nhân` INT
  `Lễ hội` INT
  , PRIMARY KEY (`Di tích`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.dtvh_kpi_tong_quan',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job5',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`file_store_src` (
  `id` BIGINT
  `ward_code` STRING
  `year` INT
  `category` STRING
  `file_name` STRING
  `file_path` STRING
  `mime_type` STRING
  `file_size` BIGINT
  `checksum` STRING
  `created_at` BIGINT
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.file_store',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job5',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`files_baocao_src` (
  `projects_end_date` STRING
  `projects_gpmb_money` STRING
  `projects_invest_money` STRING
  `projects_projectId` STRING
  `projects_projectName` STRING
  `projects_projectType` STRING
  `projects_start_date` STRING
  `projects_status` STRING
  `projects_object_gpmb_adress` STRING
  `projects_object_gpmb_contact_name` STRING
  `projects_object_gpmb_gpmb_type` STRING
  `projects_object_gpmb_in_project` STRING
  `projects_object_gpmb_in_project_name` STRING
  `projects_object_gpmb_person_gpmb_id` STRING
  `projects_object_gpmb_person_gpmb_name` STRING
  `projects_object_gpmb_person_gpmb_type` STRING
  `projects_object_gpmb_phone_num` STRING
  `projects_object_gpmb_verify_land` STRING
  `projects_object_gpmb_area_land_recall_handed_over` STRING
  `projects_object_gpmb_area_land_recall_not_handed_over` STRING
  `projects_object_gpmb_area_land_reclaim_argi_land` STRING
  `projects_object_gpmb_area_land_reclaim_other_land` STRING
  `projects_object_gpmb_area_land_reclaim_resident_land` STRING
  `projects_object_gpmb_area_land_reclaim_total_area` STRING
  `projects_object_gpmb_indem_money_not_received` STRING
  `projects_object_gpmb_indem_money_received` STRING
  , PRIMARY KEY (`projects_end_date`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.files_baocao',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job5',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`giaoduc_chart_hinh_thuc_quan_ly_gv_src` (
  `Loại trường` STRING
  `Hình thức` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.giaoduc_chart_hinh_thuc_quan_ly_gv',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job5',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

CREATE TEMPORARY TABLE default_catalog.default_database.`giaoduc_chart_quan_ly_phong_hoc_src` (
  `Loại trường` STRING
  `Loại phòng` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'='kafka',
  'topic'='topic_datalakehouse.datalakehouse.giaoduc_chart_quan_ly_phong_hoc',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='flink-datalakehouse-job5',
  'scan.startup.mode'='earliest-offset',
  'format'='debezium-json',
  'debezium-json.schema-include'='true'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_datalakehouse.db_datalakehouse.`dtvh_kpi_tong_quan`
SELECT `Di tích`, `Di tích đã xếp hạng`, `Nghệ nhân`, `Lễ hội`
FROM default_catalog.default_database.`dtvh_kpi_tong_quan_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`file_store`
SELECT `id`, `ward_code`, `year`, `category`, `file_name`, `file_path`, `mime_type`, `file_size`, `checksum`, `created_at`
FROM default_catalog.default_database.`file_store_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`files_baocao`
SELECT `projects_end_date`, `projects_gpmb_money`, `projects_invest_money`, `projects_projectId`, `projects_projectName`, `projects_projectType`, `projects_start_date`, `projects_status`, `projects_object_gpmb_adress`, `projects_object_gpmb_contact_name`, `projects_object_gpmb_gpmb_type`, `projects_object_gpmb_in_project`, `projects_object_gpmb_in_project_name`, `projects_object_gpmb_person_gpmb_id`, `projects_object_gpmb_person_gpmb_name`, `projects_object_gpmb_person_gpmb_type`, `projects_object_gpmb_phone_num`, `projects_object_gpmb_verify_land`, `projects_object_gpmb_area_land_recall_handed_over`, `projects_object_gpmb_area_land_recall_not_handed_over`, `projects_object_gpmb_area_land_reclaim_argi_land`, `projects_object_gpmb_area_land_reclaim_other_land`, `projects_object_gpmb_area_land_reclaim_resident_land`, `projects_object_gpmb_area_land_reclaim_total_area`, `projects_object_gpmb_indem_money_not_received`, `projects_object_gpmb_indem_money_received`
FROM default_catalog.default_database.`files_baocao_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`giaoduc_chart_hinh_thuc_quan_ly_gv`
SELECT `Loại trường`, `Hình thức`, `Số lượng`
FROM default_catalog.default_database.`giaoduc_chart_hinh_thuc_quan_ly_gv_src`;
INSERT INTO catalog_datalakehouse.db_datalakehouse.`giaoduc_chart_quan_ly_phong_hoc`
SELECT `Loại trường`, `Loại phòng`, `Số lượng`
FROM default_catalog.default_database.`giaoduc_chart_quan_ly_phong_hoc_src`;
END;
