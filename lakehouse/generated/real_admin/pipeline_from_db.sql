-- GENERATED PIPELINE
SET 'execution.checkpointing.interval' = '60s';
SET 'table.exec.sink.upsert-materialize' = 'AUTO';

DROP CATALOG IF EXISTS catalog_admin;
CREATE CATALOG catalog_admin WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'hive',
  'uri'                  = 'thrift://172.18.0.6:9083',
  'warehouse'            = 's3a://catalog_admin/iceberg-data',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.region'            = 'us-east-1',
  's3.path-style-access' = 'true',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'password'
);
USE CATALOG catalog_admin;
CREATE DATABASE IF NOT EXISTS db_admin;

DROP TABLE IF EXISTS db_admin.`cdt_chart_bar_linh_vuc`;
CREATE TABLE IF NOT EXISTS db_admin.`cdt_chart_bar_linh_vuc` (
  `Lĩnh vực` STRING,
  `ky_yyyymm` DATE,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Tổng chi đầu tư (Triệu)` DOUBLE
  , PRIMARY KEY (`Lĩnh vực`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`cdt_chart_bar_linh_vuc_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`cdt_chart_bar_linh_vuc_src` (
  `Lĩnh vực` STRING,
  `ky_yyyymm` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Tổng chi đầu tư (Triệu)` DOUBLE
  , PRIMARY KEY (`Lĩnh vực`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.cdt_chart_bar_linh_vuc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-cdt_chart_bar_linh_vuc',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`cdt_chart_bar_tong_dau_tu`;
CREATE TABLE IF NOT EXISTS db_admin.`cdt_chart_bar_tong_dau_tu` (
  `Tên dự án` STRING,
  `Tổng tiền đầu tư (Tỷ đồng)` DOUBLE,
  `Trạng thái dự án` STRING,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Lĩnh vực` STRING,
  `ky_yyyymm` DATE
  , PRIMARY KEY (`Tên dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`cdt_chart_bar_tong_dau_tu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`cdt_chart_bar_tong_dau_tu_src` (
  `Tên dự án` STRING,
  `Tổng tiền đầu tư (Tỷ đồng)` DOUBLE,
  `Trạng thái dự án` STRING,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Lĩnh vực` STRING,
  `ky_yyyymm` INT
  , PRIMARY KEY (`Tên dự án`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.cdt_chart_bar_tong_dau_tu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-cdt_chart_bar_tong_dau_tu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`cdt_chart_danh_sach`;
CREATE TABLE IF NOT EXISTS db_admin.`cdt_chart_danh_sach` (
  `Mã dự án` STRING,
  `Tên dự án` STRING,
  `ky_yyyymm` DATE,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Giải ngân KQ` DOUBLE,
  `Giải ngân GPMB` DOUBLE,
  `Lũy kế giải ngân` DOUBLE,
  `Tỉ lệ giải ngân (%)` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`cdt_chart_danh_sach_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`cdt_chart_danh_sach_src` (
  `Mã dự án` STRING,
  `Tên dự án` STRING,
  `ky_yyyymm` INT,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Giải ngân KQ` DOUBLE,
  `Giải ngân GPMB` DOUBLE,
  `Lũy kế giải ngân` DOUBLE,
  `Tỉ lệ giải ngân (%)` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.cdt_chart_danh_sach',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-cdt_chart_danh_sach',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`cdt_chart_pie_giai_ngan`;
CREATE TABLE IF NOT EXISTS db_admin.`cdt_chart_pie_giai_ngan` (
  `ky_yyyymm` DATE,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Loại giải ngân` STRING,
  `Giá trị` DOUBLE
  , PRIMARY KEY (`ky_yyyymm`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`cdt_chart_pie_giai_ngan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`cdt_chart_pie_giai_ngan_src` (
  `ky_yyyymm` INT,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Loại giải ngân` STRING,
  `Giá trị` DOUBLE
  , PRIMARY KEY (`ky_yyyymm`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.cdt_chart_pie_giai_ngan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-cdt_chart_pie_giai_ngan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`cdt_chart_pie_trang_thai`;
CREATE TABLE IF NOT EXISTS db_admin.`cdt_chart_pie_trang_thai` (
  `Mã dự án` STRING,
  `Trạng thái dự án` STRING,
  `ky_yyyymm` DATE,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`cdt_chart_pie_trang_thai_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`cdt_chart_pie_trang_thai_src` (
  `Mã dự án` STRING,
  `Trạng thái dự án` STRING,
  `ky_yyyymm` INT,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.cdt_chart_pie_trang_thai',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-cdt_chart_pie_trang_thai',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`cdt_kpi_tong_quan`;
CREATE TABLE IF NOT EXISTS db_admin.`cdt_kpi_tong_quan` (
  `Mã dự án` STRING,
  `ky_yyyymm` DATE,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Trạng thái dự án` STRING,
  `Là tháng mới nhất trong năm` INT,
  `Kế hoạch vốn` DOUBLE,
  `Lũy kế giải ngân` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`cdt_kpi_tong_quan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`cdt_kpi_tong_quan_src` (
  `Mã dự án` STRING,
  `ky_yyyymm` INT,
  `ky_nam` INT,
  `ky_thang` INT,
  `Ban quản lý` STRING,
  `Ngân sách thuộc cấp` STRING,
  `Trạng thái dự án` STRING,
  `Là tháng mới nhất trong năm` INT,
  `Kế hoạch vốn` DOUBLE,
  `Lũy kế giải ngân` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.cdt_kpi_tong_quan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-cdt_kpi_tong_quan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`congthuong_chart_biendong_cap_phep_ruou_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`congthuong_chart_biendong_cap_phep_ruou_thang` (
  `Tháng` DATE,
  `Loại hình cấp phép` STRING,
  `Số giấy phép` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`congthuong_chart_biendong_cap_phep_ruou_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`congthuong_chart_biendong_cap_phep_ruou_thang_src` (
  `Tháng` INT,
  `Loại hình cấp phép` STRING,
  `Số giấy phép` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.congthuong_chart_biendong_cap_phep_ruou_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-congthuong_chart_biendong_cap_phep_ruou_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`congthuong_chart_biendong_kios_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`congthuong_chart_biendong_kios_thang` (
  `Tháng` DATE,
  `Đơn_vị_quản_lý` STRING,
  `Lĩnh vực` STRING,
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`congthuong_chart_biendong_kios_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`congthuong_chart_biendong_kios_thang_src` (
  `Tháng` INT,
  `Đơn_vị_quản_lý` STRING,
  `Lĩnh vực` STRING,
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.congthuong_chart_biendong_kios_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-congthuong_chart_biendong_kios_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`congthuong_chart_cocau_cap_phep_ruou`;
CREATE TABLE IF NOT EXISTS db_admin.`congthuong_chart_cocau_cap_phep_ruou` (
  `Loại_hình_cấp_phép` STRING,
  `Số_cơ_sở` BIGINT
  , PRIMARY KEY (`Loại_hình_cấp_phép`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`congthuong_chart_cocau_cap_phep_ruou_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`congthuong_chart_cocau_cap_phep_ruou_src` (
  `Loại_hình_cấp_phép` STRING,
  `Số_cơ_sở` BIGINT
  , PRIMARY KEY (`Loại_hình_cấp_phép`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.congthuong_chart_cocau_cap_phep_ruou',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-congthuong_chart_cocau_cap_phep_ruou',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`congthuong_chart_cocau_kios`;
CREATE TABLE IF NOT EXISTS db_admin.`congthuong_chart_cocau_kios` (
  `Đơn_vị_quản_lý` STRING,
  `Lĩnh vực` STRING,
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Đơn_vị_quản_lý`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`congthuong_chart_cocau_kios_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`congthuong_chart_cocau_kios_src` (
  `Đơn_vị_quản_lý` STRING,
  `Lĩnh vực` STRING,
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Đơn_vị_quản_lý`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.congthuong_chart_cocau_kios',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-congthuong_chart_cocau_kios',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`congthuong_chart_nganh_nghe_kinh_doanh`;
CREATE TABLE IF NOT EXISTS db_admin.`congthuong_chart_nganh_nghe_kinh_doanh` (
  `Ngành_nghề` STRING,
  `Số_lượng` BIGINT
  , PRIMARY KEY (`Ngành_nghề`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`congthuong_chart_nganh_nghe_kinh_doanh_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`congthuong_chart_nganh_nghe_kinh_doanh_src` (
  `Ngành_nghề` STRING,
  `Số_lượng` BIGINT
  , PRIMARY KEY (`Ngành_nghề`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.congthuong_chart_nganh_nghe_kinh_doanh',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-congthuong_chart_nganh_nghe_kinh_doanh',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`congthuong_kpi_tong_quan`;
CREATE TABLE IF NOT EXISTS db_admin.`congthuong_kpi_tong_quan` (
  `Tổng số Ki-ốt` BIGINT,
  `Giấy phép kinh doanh rượu` BIGINT,
  `Tổng số cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng số Ki-ốt`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`congthuong_kpi_tong_quan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`congthuong_kpi_tong_quan_src` (
  `Tổng số Ki-ốt` BIGINT,
  `Giấy phép kinh doanh rượu` BIGINT,
  `Tổng số cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng số Ki-ốt`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.congthuong_kpi_tong_quan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-congthuong_kpi_tong_quan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dash_chua_giai_ngan_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`dash_chua_giai_ngan_thang` (
  `thang` STRING,
  `tong_chua_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dash_chua_giai_ngan_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dash_chua_giai_ngan_thang_src` (
  `thang` STRING,
  `tong_chua_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dash_chua_giai_ngan_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dash_chua_giai_ngan_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dash_chua_gn_chi_linh_vuc`;
CREATE TABLE IF NOT EXISTS db_admin.`dash_chua_gn_chi_linh_vuc` (
  `thang` STRING,
  `tong_chua_giai_ngan` DOUBLE,
  `tong_chua_gn_chi_linh_vuc` DOUBLE,
  `tong_gn_linh_vuc` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dash_chua_gn_chi_linh_vuc_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dash_chua_gn_chi_linh_vuc_src` (
  `thang` STRING,
  `tong_chua_giai_ngan` DOUBLE,
  `tong_chua_gn_chi_linh_vuc` DOUBLE,
  `tong_gn_linh_vuc` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dash_chua_gn_chi_linh_vuc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dash_chua_gn_chi_linh_vuc',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dash_co_cau_du_an_trang_thai`;
CREATE TABLE IF NOT EXISTS db_admin.`dash_co_cau_du_an_trang_thai` (
  `thang` STRING,
  `trang_thai` STRING,
  `so_du_an` BIGINT
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dash_co_cau_du_an_trang_thai_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dash_co_cau_du_an_trang_thai_src` (
  `thang` STRING,
  `trang_thai` STRING,
  `so_du_an` BIGINT
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dash_co_cau_du_an_trang_thai',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dash_co_cau_du_an_trang_thai',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dash_tong_giai_ngan_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`dash_tong_giai_ngan_thang` (
  `thang` STRING,
  `tong_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dash_tong_giai_ngan_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dash_tong_giai_ngan_thang_src` (
  `thang` STRING,
  `tong_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dash_tong_giai_ngan_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dash_tong_giai_ngan_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dtvh_chart_le_hoi_theo_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`dtvh_chart_le_hoi_theo_thang` (
  `Tháng` INT,
  `Số lượng lễ hội` INT,
  `ngay` DATE
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dtvh_chart_le_hoi_theo_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dtvh_chart_le_hoi_theo_thang_src` (
  `Tháng` INT,
  `Số lượng lễ hội` INT,
  `ngay` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dtvh_chart_le_hoi_theo_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dtvh_chart_le_hoi_theo_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dtvh_chart_loai_hinh_di_tich`;
CREATE TABLE IF NOT EXISTS db_admin.`dtvh_chart_loai_hinh_di_tich` (
  `Loại hình di tích` STRING,
  `Số lượng di tích` INT
  , PRIMARY KEY (`Loại hình di tích`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dtvh_chart_loai_hinh_di_tich_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dtvh_chart_loai_hinh_di_tich_src` (
  `Loại hình di tích` STRING,
  `Số lượng di tích` INT
  , PRIMARY KEY (`Loại hình di tích`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dtvh_chart_loai_hinh_di_tich',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dtvh_chart_loai_hinh_di_tich',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dtvh_chart_nghe_nhan_theo_loai_hinh`;
CREATE TABLE IF NOT EXISTS db_admin.`dtvh_chart_nghe_nhan_theo_loai_hinh` (
  `Loại hình di sản` STRING,
  `Số lượng nghệ nhân` INT
  , PRIMARY KEY (`Loại hình di sản`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dtvh_chart_nghe_nhan_theo_loai_hinh_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dtvh_chart_nghe_nhan_theo_loai_hinh_src` (
  `Loại hình di sản` STRING,
  `Số lượng nghệ nhân` INT
  , PRIMARY KEY (`Loại hình di sản`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dtvh_chart_nghe_nhan_theo_loai_hinh',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dtvh_chart_nghe_nhan_theo_loai_hinh',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dtvh_chart_xep_hang_di_tich`;
CREATE TABLE IF NOT EXISTS db_admin.`dtvh_chart_xep_hang_di_tich` (
  `Cấp xếp hạng` STRING,
  `Số lượng di tích` INT
  , PRIMARY KEY (`Cấp xếp hạng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dtvh_chart_xep_hang_di_tich_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dtvh_chart_xep_hang_di_tich_src` (
  `Cấp xếp hạng` STRING,
  `Số lượng di tích` INT
  , PRIMARY KEY (`Cấp xếp hạng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dtvh_chart_xep_hang_di_tich',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dtvh_chart_xep_hang_di_tich',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`dtvh_kpi_tong_quan`;
CREATE TABLE IF NOT EXISTS db_admin.`dtvh_kpi_tong_quan` (
  `Di tích` INT,
  `Di tích đã xếp hạng` INT,
  `Nghệ nhân` INT,
  `Lễ hội` INT
  , PRIMARY KEY (`Di tích`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`dtvh_kpi_tong_quan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`dtvh_kpi_tong_quan_src` (
  `Di tích` INT,
  `Di tích đã xếp hạng` INT,
  `Nghệ nhân` INT,
  `Lễ hội` INT
  , PRIMARY KEY (`Di tích`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.dtvh_kpi_tong_quan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-dtvh_kpi_tong_quan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`file_store`;
CREATE TABLE IF NOT EXISTS db_admin.`file_store` (
  `id` BIGINT,
  `ward_code` STRING,
  `year` INT,
  `category` STRING,
  `file_name` STRING,
  `file_path` STRING,
  `mime_type` STRING,
  `file_size` BIGINT,
  `checksum` STRING,
  `created_at` TIMESTAMP(3)
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`file_store_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`file_store_src` (
  `id` BIGINT,
  `ward_code` STRING,
  `year` INT,
  `category` STRING,
  `file_name` STRING,
  `file_path` STRING,
  `mime_type` STRING,
  `file_size` BIGINT,
  `checksum` STRING,
  `created_at` BIGINT
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.file_store',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-file_store',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`files_baocao`;
CREATE TABLE IF NOT EXISTS db_admin.`files_baocao` (
  `projects_end_date` STRING,
  `projects_gpmb_money` STRING,
  `projects_invest_money` STRING,
  `projects_projectId` STRING,
  `projects_projectName` STRING,
  `projects_projectType` STRING,
  `projects_start_date` STRING,
  `projects_status` STRING,
  `projects_object_gpmb_adress` STRING,
  `projects_object_gpmb_contact_name` STRING,
  `projects_object_gpmb_gpmb_type` STRING,
  `projects_object_gpmb_in_project` STRING,
  `projects_object_gpmb_in_project_name` STRING,
  `projects_object_gpmb_person_gpmb_id` STRING,
  `projects_object_gpmb_person_gpmb_name` STRING,
  `projects_object_gpmb_person_gpmb_type` STRING,
  `projects_object_gpmb_phone_num` STRING,
  `projects_object_gpmb_verify_land` STRING,
  `projects_object_gpmb_area_land_recall_handed_over` STRING,
  `projects_object_gpmb_area_land_recall_not_handed_over` STRING,
  `projects_object_gpmb_area_land_reclaim_argi_land` STRING,
  `projects_object_gpmb_area_land_reclaim_other_land` STRING,
  `projects_object_gpmb_area_land_reclaim_resident_land` STRING,
  `projects_object_gpmb_area_land_reclaim_total_area` STRING,
  `projects_object_gpmb_indem_money_not_received` STRING,
  `projects_object_gpmb_indem_money_received` STRING
  , PRIMARY KEY (`projects_end_date`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`files_baocao_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`files_baocao_src` (
  `projects_end_date` STRING,
  `projects_gpmb_money` STRING,
  `projects_invest_money` STRING,
  `projects_projectId` STRING,
  `projects_projectName` STRING,
  `projects_projectType` STRING,
  `projects_start_date` STRING,
  `projects_status` STRING,
  `projects_object_gpmb_adress` STRING,
  `projects_object_gpmb_contact_name` STRING,
  `projects_object_gpmb_gpmb_type` STRING,
  `projects_object_gpmb_in_project` STRING,
  `projects_object_gpmb_in_project_name` STRING,
  `projects_object_gpmb_person_gpmb_id` STRING,
  `projects_object_gpmb_person_gpmb_name` STRING,
  `projects_object_gpmb_person_gpmb_type` STRING,
  `projects_object_gpmb_phone_num` STRING,
  `projects_object_gpmb_verify_land` STRING,
  `projects_object_gpmb_area_land_recall_handed_over` STRING,
  `projects_object_gpmb_area_land_recall_not_handed_over` STRING,
  `projects_object_gpmb_area_land_reclaim_argi_land` STRING,
  `projects_object_gpmb_area_land_reclaim_other_land` STRING,
  `projects_object_gpmb_area_land_reclaim_resident_land` STRING,
  `projects_object_gpmb_area_land_reclaim_total_area` STRING,
  `projects_object_gpmb_indem_money_not_received` STRING,
  `projects_object_gpmb_indem_money_received` STRING
  , PRIMARY KEY (`projects_end_date`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.files_baocao',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-files_baocao',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`giaoduc_chart_hinh_thuc_quan_ly_gv`;
CREATE TABLE IF NOT EXISTS db_admin.`giaoduc_chart_hinh_thuc_quan_ly_gv` (
  `Loại trường` STRING,
  `Hình thức` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`giaoduc_chart_hinh_thuc_quan_ly_gv_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`giaoduc_chart_hinh_thuc_quan_ly_gv_src` (
  `Loại trường` STRING,
  `Hình thức` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.giaoduc_chart_hinh_thuc_quan_ly_gv',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-giaoduc_chart_hinh_thuc_quan_ly_gv',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`giaoduc_chart_quan_ly_phong_hoc`;
CREATE TABLE IF NOT EXISTS db_admin.`giaoduc_chart_quan_ly_phong_hoc` (
  `Loại trường` STRING,
  `Loại phòng` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`giaoduc_chart_quan_ly_phong_hoc_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`giaoduc_chart_quan_ly_phong_hoc_src` (
  `Loại trường` STRING,
  `Loại phòng` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.giaoduc_chart_quan_ly_phong_hoc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-giaoduc_chart_quan_ly_phong_hoc',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`giaoduc_chart_quy_mo_hoc_sinh`;
CREATE TABLE IF NOT EXISTS db_admin.`giaoduc_chart_quy_mo_hoc_sinh` (
  `Khối lớp` STRING,
  `Cấp học` STRING,
  `Học sinh nam` INT,
  `Học sinh nữ` INT,
  `Tổng học sinh` INT
  , PRIMARY KEY (`Khối lớp`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`giaoduc_chart_quy_mo_hoc_sinh_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`giaoduc_chart_quy_mo_hoc_sinh_src` (
  `Khối lớp` STRING,
  `Cấp học` STRING,
  `Học sinh nam` INT,
  `Học sinh nữ` INT,
  `Tổng học sinh` INT
  , PRIMARY KEY (`Khối lớp`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.giaoduc_chart_quy_mo_hoc_sinh',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-giaoduc_chart_quy_mo_hoc_sinh',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`giaoduc_chart_trinh_do_giao_vien`;
CREATE TABLE IF NOT EXISTS db_admin.`giaoduc_chart_trinh_do_giao_vien` (
  `Loại trường` STRING,
  `Trình độ` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`giaoduc_chart_trinh_do_giao_vien_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`giaoduc_chart_trinh_do_giao_vien_src` (
  `Loại trường` STRING,
  `Trình độ` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.giaoduc_chart_trinh_do_giao_vien',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-giaoduc_chart_trinh_do_giao_vien',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`giaoduc_kpi_so_luong_truong`;
CREATE TABLE IF NOT EXISTS db_admin.`giaoduc_kpi_so_luong_truong` (
  `Loại trường` STRING,
  `Chỉ tiêu` STRING,
  `Giá trị` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`giaoduc_kpi_so_luong_truong_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`giaoduc_kpi_so_luong_truong_src` (
  `Loại trường` STRING,
  `Chỉ tiêu` STRING,
  `Giá trị` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.giaoduc_kpi_so_luong_truong',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-giaoduc_kpi_so_luong_truong',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`giaoduc_kpi_tong_giao_vien`;
CREATE TABLE IF NOT EXISTS db_admin.`giaoduc_kpi_tong_giao_vien` (
  `Loại trường` STRING,
  `Chức danh` STRING,
  `Tổng số` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`giaoduc_kpi_tong_giao_vien_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`giaoduc_kpi_tong_giao_vien_src` (
  `Loại trường` STRING,
  `Chức danh` STRING,
  `Tổng số` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.giaoduc_kpi_tong_giao_vien',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-giaoduc_kpi_tong_giao_vien',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb__pie_progress`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb__pie_progress` (
  `snapshot_date` DATE,
  `project_id` STRING,
  `ten_du_an` STRING,
  `object_id` STRING,
  `planned_finish_date` DATE,
  `actual_finish_date` DATE,
  `progress_status` STRING
  , PRIMARY KEY (`snapshot_date`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb__pie_progress_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb__pie_progress_src` (
  `snapshot_date` INT,
  `project_id` STRING,
  `ten_du_an` STRING,
  `object_id` STRING,
  `planned_finish_date` INT,
  `actual_finish_date` INT,
  `progress_status` STRING
  , PRIMARY KEY (`snapshot_date`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb__pie_progress',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb__pie_progress',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_chi_dau_tu_phat_trien`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_chi_dau_tu_phat_trien` (
  `nam` INT,
  `loai_chi_dau_tu` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_chi_dau_tu_phat_trien_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_chi_dau_tu_phat_trien_src` (
  `nam` INT,
  `loai_chi_dau_tu` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_chi_dau_tu_phat_trien',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_chi_dau_tu_phat_trien',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_chi_theo_linh_vuc`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_chi_theo_linh_vuc` (
  `nam` INT,
  `linh_vuc` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_chi_theo_linh_vuc_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_chi_theo_linh_vuc_src` (
  `nam` INT,
  `linh_vuc` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_chi_theo_linh_vuc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_chi_theo_linh_vuc',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_co_cau_chi_ngan_sach`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_co_cau_chi_ngan_sach` (
  `nam` STRING,
  `loai_chi` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_co_cau_chi_ngan_sach_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_co_cau_chi_ngan_sach_src` (
  `nam` STRING,
  `loai_chi` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_co_cau_chi_ngan_sach',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_co_cau_chi_ngan_sach',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_co_cau_giai_ngan`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_co_cau_giai_ngan` (
  `nam` INT,
  `trang_thai` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_co_cau_giai_ngan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_co_cau_giai_ngan_src` (
  `nam` INT,
  `trang_thai` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_co_cau_giai_ngan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_co_cau_giai_ngan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_co_cau_nguon_thu`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_co_cau_nguon_thu` (
  `nam` STRING,
  `loai_nguon_thu` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_co_cau_nguon_thu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_co_cau_nguon_thu_src` (
  `nam` STRING,
  `loai_nguon_thu` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_co_cau_nguon_thu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_co_cau_nguon_thu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_co_cau_nguon_thu_db`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_co_cau_nguon_thu_db` (
  `nam` STRING,
  `loai_nguon_thu` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_co_cau_nguon_thu_db_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_co_cau_nguon_thu_db_src` (
  `nam` STRING,
  `loai_nguon_thu` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_co_cau_nguon_thu_db',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_co_cau_nguon_thu_db',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_dong_ngan_sach`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_dong_ngan_sach` (
  `nam` INT,
  `loai_ngan_sach` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_dong_ngan_sach_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_dong_ngan_sach_src` (
  `nam` INT,
  `loai_ngan_sach` STRING,
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_dong_ngan_sach',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_dong_ngan_sach',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_du_toan_nam`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_du_toan_nam` (
  `nam` STRING,
  `loai` STRING,
  `du_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_du_toan_nam_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_du_toan_nam_src` (
  `nam` STRING,
  `loai` STRING,
  `du_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_du_toan_nam',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_du_toan_nam',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_giaingan_project`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_giaingan_project` (
  `Mã dự án` STRING,
  `Tên dự án` STRING,
  `Số tiền giải ngân thực tế` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_giaingan_project_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_giaingan_project_src` (
  `Mã dự án` STRING,
  `Tên dự án` STRING,
  `Số tiền giải ngân thực tế` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_giaingan_project',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_giaingan_project',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_handover_structure_circle`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_handover_structure_circle` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `loai` STRING,
  `gia_tri` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_handover_structure_circle_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_handover_structure_circle_src` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `loai` STRING,
  `gia_tri` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_handover_structure_circle',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_handover_structure_circle',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_land_structure`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_land_structure` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `loai_dat` STRING,
  `dien_tich` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_land_structure_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_land_structure_src` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `loai_dat` STRING,
  `dien_tich` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_land_structure',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_land_structure',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_line_timeline`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_line_timeline` (
  `project_id` STRING,
  `ten_du_an` STRING,
  `object_id` STRING,
  `date` DATE,
  `step_no` INT,
  `step_name` STRING,
  `step_value` INT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_line_timeline_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_line_timeline_src` (
  `project_id` STRING,
  `ten_du_an` STRING,
  `object_id` STRING,
  `date` INT,
  `step_no` INT,
  `step_name` STRING,
  `step_value` INT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_line_timeline',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_line_timeline',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_overview`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_overview` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `tong_du_an` INT,
  `Số hộ GPMB (dự kiến)` INT,
  `Diện tích GPMB (dự kiến)` DOUBLE,
  `ten_doi_tuong` INT,
  `Diện tích` DOUBLE,
  `so_doi_tuong` STRING,
  `Chi phí GPMB` STRING,
  `TMĐT` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_overview_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_overview_src` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `tong_du_an` INT,
  `Số hộ GPMB (dự kiến)` INT,
  `Diện tích GPMB (dự kiến)` DOUBLE,
  `ten_doi_tuong` INT,
  `Diện tích` DOUBLE,
  `so_doi_tuong` STRING,
  `Chi phí GPMB` STRING,
  `TMĐT` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_overview',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_overview',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_overview_2`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_overview_2` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `Chi phí GPMB` STRING,
  `Diện tích thu hồi` DOUBLE,
  `ten_doi_tuong` BIGINT,
  `tong_du_an` INT,
  `TMĐT` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_overview_2_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_overview_2_src` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `Chi phí GPMB` STRING,
  `Diện tích thu hồi` DOUBLE,
  `ten_doi_tuong` BIGINT,
  `tong_du_an` INT,
  `TMĐT` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_overview_2',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_overview_2',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_quy_mo_nguon_thu`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_quy_mo_nguon_thu` (
  `nam` INT,
  `thang` INT,
  `trieu_vnd` DOUBLE,
  `chuyen_nguon_nam_truoc` DOUBLE,
  `thue_phi` DOUBLE,
  `tien_su_dung_dat` DOUBLE,
  `thu_bo_sung_cap_tren` DOUBLE,
  `thu_khac` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_quy_mo_nguon_thu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_quy_mo_nguon_thu_src` (
  `nam` INT,
  `thang` INT,
  `trieu_vnd` DOUBLE,
  `chuyen_nguon_nam_truoc` DOUBLE,
  `thue_phi` DOUBLE,
  `tien_su_dung_dat` DOUBLE,
  `thu_bo_sung_cap_tren` DOUBLE,
  `thu_khac` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_quy_mo_nguon_thu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_quy_mo_nguon_thu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_ss_chi_dau_tu`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_ss_chi_dau_tu` (
  `nam` STRING,
  `loai` STRING,
  `du_toan` DOUBLE,
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_ss_chi_dau_tu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_ss_chi_dau_tu_src` (
  `nam` STRING,
  `loai` STRING,
  `du_toan` DOUBLE,
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_ss_chi_dau_tu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_ss_chi_dau_tu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_ss_chi_linh_vuc`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_ss_chi_linh_vuc` (
  `nam` STRING,
  `linh_vuc` STRING,
  `du_toan` DOUBLE,
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_ss_chi_linh_vuc_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_ss_chi_linh_vuc_src` (
  `nam` STRING,
  `linh_vuc` STRING,
  `du_toan` DOUBLE,
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_ss_chi_linh_vuc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_ss_chi_linh_vuc',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_tong_so_ngan_sach`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_tong_so_ngan_sach` (
  `nam` STRING,
  `tong_thu_ngan_sach` DOUBLE,
  `thu_tren_dia_ban` DOUBLE,
  `thu_tren_xa_phuong` DOUBLE,
  `tong_chi_ngan_sach` DOUBLE,
  `ty_le_thuc_hien_thu` DOUBLE,
  `ty_le_giai_ngan` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_tong_so_ngan_sach_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_tong_so_ngan_sach_src` (
  `nam` STRING,
  `tong_thu_ngan_sach` DOUBLE,
  `thu_tren_dia_ban` DOUBLE,
  `thu_tren_xa_phuong` DOUBLE,
  `tong_chi_ngan_sach` DOUBLE,
  `ty_le_thuc_hien_thu` DOUBLE,
  `ty_le_giai_ngan` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_tong_so_ngan_sach',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_tong_so_ngan_sach',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_voluntary_structure_circle`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_voluntary_structure_circle` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `loai` STRING,
  `so_luong` BIGINT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_voluntary_structure_circle_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_voluntary_structure_circle_src` (
  `project_id` STRING,
  `Tên dự án` STRING,
  `loai` STRING,
  `so_luong` BIGINT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_voluntary_structure_circle',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_voluntary_structure_circle',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_xu_huong_ngan_sach`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_xu_huong_ngan_sach` (
  `nam` INT,
  `thang` INT,
  `trieu_vnd` DOUBLE,
  `du_toan` DOUBLE,
  `thuc_hien` DOUBLE,
  `thoi_gian` DATE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_xu_huong_ngan_sach_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_xu_huong_ngan_sach_src` (
  `nam` INT,
  `thang` INT,
  `trieu_vnd` DOUBLE,
  `du_toan` DOUBLE,
  `thuc_hien` DOUBLE,
  `thoi_gian` INT
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_xu_huong_ngan_sach',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_xu_huong_ngan_sach',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`gpmb_xu_huong_thu_chi`;
CREATE TABLE IF NOT EXISTS db_admin.`gpmb_xu_huong_thu_chi` (
  `nam` INT,
  `thang` INT,
  `thu` DOUBLE,
  `chi` DOUBLE,
  `thoi_gian` DATE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`gpmb_xu_huong_thu_chi_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`gpmb_xu_huong_thu_chi_src` (
  `nam` INT,
  `thang` INT,
  `thu` DOUBLE,
  `chi` DOUBLE,
  `thoi_gian` INT
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.gpmb_xu_huong_thu_chi',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-gpmb_xu_huong_thu_chi',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`hcc_chart_chi_tieu`;
CREATE TABLE IF NOT EXISTS db_admin.`hcc_chart_chi_tieu` (
  `Mã chỉ tiêu` INT,
  `Nhóm chỉ tiêu` STRING,
  `UBND Tây Hồ` DOUBLE,
  `Tp Hà Nội` DOUBLE,
  `Tỷ lệ (%)` DOUBLE,
  `Năm` STRING,
  `ngày` DATE
  , PRIMARY KEY (`Mã chỉ tiêu`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`hcc_chart_chi_tieu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`hcc_chart_chi_tieu_src` (
  `Mã chỉ tiêu` INT,
  `Nhóm chỉ tiêu` STRING,
  `UBND Tây Hồ` DOUBLE,
  `Tp Hà Nội` DOUBLE,
  `Tỷ lệ (%)` DOUBLE,
  `Năm` STRING,
  `ngày` INT
  , PRIMARY KEY (`Mã chỉ tiêu`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.hcc_chart_chi_tieu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-hcc_chart_chi_tieu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`hcc_chart_ho_so_tiep_nhan`;
CREATE TABLE IF NOT EXISTS db_admin.`hcc_chart_ho_so_tiep_nhan` (
  `Hình thức nộp hồ sơ` STRING,
  `Số lượng hồ sơ` INT,
  `Tỷ lệ (%)` DOUBLE,
  `ngày` DATE
  , PRIMARY KEY (`Hình thức nộp hồ sơ`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`hcc_chart_ho_so_tiep_nhan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`hcc_chart_ho_so_tiep_nhan_src` (
  `Hình thức nộp hồ sơ` STRING,
  `Số lượng hồ sơ` INT,
  `Tỷ lệ (%)` DOUBLE,
  `ngày` INT
  , PRIMARY KEY (`Hình thức nộp hồ sơ`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.hcc_chart_ho_so_tiep_nhan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-hcc_chart_ho_so_tiep_nhan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`hcc_chart_ket_qua_giai_quyet`;
CREATE TABLE IF NOT EXISTS db_admin.`hcc_chart_ket_qua_giai_quyet` (
  `Trạng thái hồ sơ` STRING,
  `Số lượng hồ sơ` INT,
  `Ghi chú` STRING,
  `ngày` DATE
  , PRIMARY KEY (`Trạng thái hồ sơ`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`hcc_chart_ket_qua_giai_quyet_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`hcc_chart_ket_qua_giai_quyet_src` (
  `Trạng thái hồ sơ` STRING,
  `Số lượng hồ sơ` INT,
  `Ghi chú` STRING,
  `ngày` INT
  , PRIMARY KEY (`Trạng thái hồ sơ`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.hcc_chart_ket_qua_giai_quyet',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-hcc_chart_ket_qua_giai_quyet',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`hcc_chart_loai_hinh_dich_vu_cong`;
CREATE TABLE IF NOT EXISTS db_admin.`hcc_chart_loai_hinh_dich_vu_cong` (
  `Loại hình dịch vụ công` STRING,
  `Số thủ tục` INT,
  `Ghi chú` STRING,
  `ngày` DATE
  , PRIMARY KEY (`Loại hình dịch vụ công`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`hcc_chart_loai_hinh_dich_vu_cong_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`hcc_chart_loai_hinh_dich_vu_cong_src` (
  `Loại hình dịch vụ công` STRING,
  `Số thủ tục` INT,
  `Ghi chú` STRING,
  `ngày` INT
  , PRIMARY KEY (`Loại hình dịch vụ công`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.hcc_chart_loai_hinh_dich_vu_cong',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-hcc_chart_loai_hinh_dich_vu_cong',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`hcc_kpi_tong_quan`;
CREATE TABLE IF NOT EXISTS db_admin.`hcc_kpi_tong_quan` (
  `Tổng số hồ sơ tiếp nhận` INT,
  `Tỷ lệ đồng bộ lên Cổng DVC Quốc gia (%)` DOUBLE,
  `Số lượt đánh giá hài lòng` INT,
  `ngày` DATE
  , PRIMARY KEY (`Tổng số hồ sơ tiếp nhận`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`hcc_kpi_tong_quan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`hcc_kpi_tong_quan_src` (
  `Tổng số hồ sơ tiếp nhận` INT,
  `Tỷ lệ đồng bộ lên Cổng DVC Quốc gia (%)` DOUBLE,
  `Số lượt đánh giá hài lòng` INT,
  `ngày` INT
  , PRIMARY KEY (`Tổng số hồ sơ tiếp nhận`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.hcc_kpi_tong_quan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-hcc_kpi_tong_quan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`ktxh_an_ninh_quoc_phong`;
CREATE TABLE IF NOT EXISTS db_admin.`ktxh_an_ninh_quoc_phong` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`ktxh_an_ninh_quoc_phong_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ktxh_an_ninh_quoc_phong_src` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.ktxh_an_ninh_quoc_phong',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-ktxh_an_ninh_quoc_phong',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`ktxh_bo_loc_trang_thai`;
CREATE TABLE IF NOT EXISTS db_admin.`ktxh_bo_loc_trang_thai` (
  `Trạng thái (Quý)` STRING
  , PRIMARY KEY (`Trạng thái (Quý)`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`ktxh_bo_loc_trang_thai_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ktxh_bo_loc_trang_thai_src` (
  `Trạng thái (Quý)` STRING
  , PRIMARY KEY (`Trạng thái (Quý)`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.ktxh_bo_loc_trang_thai',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-ktxh_bo_loc_trang_thai',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`ktxh_do_thi`;
CREATE TABLE IF NOT EXISTS db_admin.`ktxh_do_thi` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`ktxh_do_thi_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ktxh_do_thi_src` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.ktxh_do_thi',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-ktxh_do_thi',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`ktxh_du_an`;
CREATE TABLE IF NOT EXISTS db_admin.`ktxh_du_an` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`ktxh_du_an_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ktxh_du_an_src` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.ktxh_du_an',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-ktxh_du_an',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`ktxh_kinh_te`;
CREATE TABLE IF NOT EXISTS db_admin.`ktxh_kinh_te` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`ktxh_kinh_te_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ktxh_kinh_te_src` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.ktxh_kinh_te',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-ktxh_kinh_te',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`ktxh_kpi_tong`;
CREATE TABLE IF NOT EXISTS db_admin.`ktxh_kpi_tong` (
  `Kết quả đánh giá KPI` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Kết quả đánh giá KPI`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`ktxh_kpi_tong_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ktxh_kpi_tong_src` (
  `Kết quả đánh giá KPI` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Kết quả đánh giá KPI`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.ktxh_kpi_tong',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-ktxh_kpi_tong',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`ktxh_van_hoa_xa_hoi`;
CREATE TABLE IF NOT EXISTS db_admin.`ktxh_van_hoa_xa_hoi` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`ktxh_van_hoa_xa_hoi_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ktxh_van_hoa_xa_hoi_src` (
  `STT` STRING,
  `Tên chỉ tiêu` STRING,
  `Đơn vị` STRING,
  `Chỉ tiêu (Năm)` STRING,
  `Quý` STRING,
  `Chỉ tiêu (Quý)` STRING,
  `Thực hiện (Quý)` STRING,
  `Tỷ lệ (%) (Quý)` STRING,
  `Trạng thái (Quý)` STRING,
  `Tổng thực hiện (Năm)` STRING,
  `Tỷ lệ (%) (Năm)` STRING,
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.ktxh_van_hoa_xa_hoi',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-ktxh_van_hoa_xa_hoi',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`moitruong_kpi_tong_quan`;
CREATE TABLE IF NOT EXISTS db_admin.`moitruong_kpi_tong_quan` (
  `Tổng rác thải thu gom (tấn)` DOUBLE,
  `Tổng tiền xử phạt VPHC (đồng)` BIGINT,
  `Tổng lượt tuyên truyền` INT,
  `Số cơ sở cam kết an toàn PCCC` INT,
  `ngày` DATE
  , PRIMARY KEY (`Tổng tiền xử phạt VPHC (đồng)`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`moitruong_kpi_tong_quan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`moitruong_kpi_tong_quan_src` (
  `Tổng rác thải thu gom (tấn)` DOUBLE,
  `Tổng tiền xử phạt VPHC (đồng)` BIGINT,
  `Tổng lượt tuyên truyền` INT,
  `Số cơ sở cam kết an toàn PCCC` INT,
  `ngày` INT
  , PRIMARY KEY (`Tổng tiền xử phạt VPHC (đồng)`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.moitruong_kpi_tong_quan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-moitruong_kpi_tong_quan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`moitruong_phan_anh_ihanoi`;
CREATE TABLE IF NOT EXISTS db_admin.`moitruong_phan_anh_ihanoi` (
  `Loại phản ánh` STRING,
  `Đã giải quyết` INT,
  `Chưa giải quyết` INT,
  `ngày` DATE
  , PRIMARY KEY (`Loại phản ánh`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`moitruong_phan_anh_ihanoi_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`moitruong_phan_anh_ihanoi_src` (
  `Loại phản ánh` STRING,
  `Đã giải quyết` INT,
  `Chưa giải quyết` INT,
  `ngày` INT
  , PRIMARY KEY (`Loại phản ánh`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.moitruong_phan_anh_ihanoi',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-moitruong_phan_anh_ihanoi',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`moitruong_thao_do`;
CREATE TABLE IF NOT EXISTS db_admin.`moitruong_thao_do` (
  `Hạng mục` STRING,
  `Số lượng` INT,
  `Đơn vị` STRING,
  `ngày` DATE
  , PRIMARY KEY (`Hạng mục`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`moitruong_thao_do_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`moitruong_thao_do_src` (
  `Hạng mục` STRING,
  `Số lượng` INT,
  `Đơn vị` STRING,
  `ngày` INT
  , PRIMARY KEY (`Hạng mục`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.moitruong_thao_do',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-moitruong_thao_do',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`moitruong_xu_phat_vphc`;
CREATE TABLE IF NOT EXISTS db_admin.`moitruong_xu_phat_vphc` (
  `Loại vi phạm` STRING,
  `Số trường hợp (vụ)` INT,
  `Số tiền phạt (đồng)` BIGINT,
  `ngày` DATE
  , PRIMARY KEY (`Loại vi phạm`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`moitruong_xu_phat_vphc_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`moitruong_xu_phat_vphc_src` (
  `Loại vi phạm` STRING,
  `Số trường hợp (vụ)` INT,
  `Số tiền phạt (đồng)` BIGINT,
  `ngày` INT
  , PRIMARY KEY (`Loại vi phạm`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.moitruong_xu_phat_vphc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-moitruong_xu_phat_vphc',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`noivu_chart_biendong_nhansu_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`noivu_chart_biendong_nhansu_thang` (
  `Nhóm đối tượng` STRING,
  `Tháng` DATE,
  `Hình thức quản lý` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`noivu_chart_biendong_nhansu_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`noivu_chart_biendong_nhansu_thang_src` (
  `Nhóm đối tượng` STRING,
  `Tháng` INT,
  `Hình thức quản lý` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.noivu_chart_biendong_nhansu_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-noivu_chart_biendong_nhansu_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`noivu_chart_cocau_nhansu`;
CREATE TABLE IF NOT EXISTS db_admin.`noivu_chart_cocau_nhansu` (
  `Nhóm đối tượng` STRING,
  `Biên chế nhà nước` INT,
  `Lao động hợp đồng` INT,
  `Tổng cộng` INT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`noivu_chart_cocau_nhansu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`noivu_chart_cocau_nhansu_src` (
  `Nhóm đối tượng` STRING,
  `Biên chế nhà nước` INT,
  `Lao động hợp đồng` INT,
  `Tổng cộng` INT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.noivu_chart_cocau_nhansu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-noivu_chart_cocau_nhansu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`noivu_chart_doitruong_chinhsach`;
CREATE TABLE IF NOT EXISTS db_admin.`noivu_chart_doitruong_chinhsach` (
  `Loại đối tượng` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại đối tượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`noivu_chart_doitruong_chinhsach_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`noivu_chart_doitruong_chinhsach_src` (
  `Loại đối tượng` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Loại đối tượng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.noivu_chart_doitruong_chinhsach',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-noivu_chart_doitruong_chinhsach',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`noivu_chart_hinhthuc_quanly`;
CREATE TABLE IF NOT EXISTS db_admin.`noivu_chart_hinhthuc_quanly` (
  `hinh_thuc_quan_ly` STRING,
  `so_luong` BIGINT
  , PRIMARY KEY (`hinh_thuc_quan_ly`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`noivu_chart_hinhthuc_quanly_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`noivu_chart_hinhthuc_quanly_src` (
  `hinh_thuc_quan_ly` STRING,
  `so_luong` BIGINT
  , PRIMARY KEY (`hinh_thuc_quan_ly`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.noivu_chart_hinhthuc_quanly',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-noivu_chart_hinhthuc_quanly',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`noivu_kpi_chinhsach`;
CREATE TABLE IF NOT EXISTS db_admin.`noivu_kpi_chinhsach` (
  `Tổng đối tượng chính sách` BIGINT
  , PRIMARY KEY (`Tổng đối tượng chính sách`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`noivu_kpi_chinhsach_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`noivu_kpi_chinhsach_src` (
  `Tổng đối tượng chính sách` BIGINT
  , PRIMARY KEY (`Tổng đối tượng chính sách`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.noivu_kpi_chinhsach',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-noivu_kpi_chinhsach',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`noivu_kpi_nhansu`;
CREATE TABLE IF NOT EXISTS db_admin.`noivu_kpi_nhansu` (
  `Tổng số nhân sự quản lý` BIGINT,
  `Tổng biên chế nhà nước` BIGINT,
  `Tổng lao động hợp đồng` BIGINT
  , PRIMARY KEY (`Tổng số nhân sự quản lý`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`noivu_kpi_nhansu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`noivu_kpi_nhansu_src` (
  `Tổng số nhân sự quản lý` BIGINT,
  `Tổng biên chế nhà nước` BIGINT,
  `Tổng lao động hợp đồng` BIGINT
  , PRIMARY KEY (`Tổng số nhân sự quản lý`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.noivu_kpi_nhansu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-noivu_kpi_nhansu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qldt_chart_can_bo_tham_du_hoi_nghi`;
CREATE TABLE IF NOT EXISTS db_admin.`qldt_chart_can_bo_tham_du_hoi_nghi` (
  `Cán bộ tham gia` STRING,
  `Lần đầu` BIGINT,
  `Thường niên` BIGINT,
  `Đột xuất` BIGINT
  , PRIMARY KEY (`Cán bộ tham gia`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qldt_chart_can_bo_tham_du_hoi_nghi_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qldt_chart_can_bo_tham_du_hoi_nghi_src` (
  `Cán bộ tham gia` STRING,
  `Lần đầu` BIGINT,
  `Thường niên` BIGINT,
  `Đột xuất` BIGINT
  , PRIMARY KEY (`Cán bộ tham gia`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qldt_chart_can_bo_tham_du_hoi_nghi',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qldt_chart_can_bo_tham_du_hoi_nghi',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qldt_chart_hoat_dong_quan_ly_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`qldt_chart_hoat_dong_quan_ly_thang` (
  `ngay` DATE,
  `Loại tổ chức` STRING,
  `Số lượng hội nghị` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qldt_chart_hoat_dong_quan_ly_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qldt_chart_hoat_dong_quan_ly_thang_src` (
  `ngay` INT,
  `Loại tổ chức` STRING,
  `Số lượng hội nghị` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qldt_chart_hoat_dong_quan_ly_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qldt_chart_hoat_dong_quan_ly_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qldt_chart_loai_chung_cu`;
CREATE TABLE IF NOT EXISTS db_admin.`qldt_chart_loai_chung_cu` (
  `Loại chung cư` STRING,
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Loại chung cư`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qldt_chart_loai_chung_cu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qldt_chart_loai_chung_cu_src` (
  `Loại chung cư` STRING,
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Loại chung cư`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qldt_chart_loai_chung_cu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qldt_chart_loai_chung_cu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qldt_chart_luot_tham_gia_hoi_nghi_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`qldt_chart_luot_tham_gia_hoi_nghi_thang` (
  `ngay` DATE,
  `Tên cán bộ` STRING,
  `Số hội nghị tham gia` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qldt_chart_luot_tham_gia_hoi_nghi_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qldt_chart_luot_tham_gia_hoi_nghi_thang_src` (
  `ngay` INT,
  `Tên cán bộ` STRING,
  `Số hội nghị tham gia` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qldt_chart_luot_tham_gia_hoi_nghi_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qldt_chart_luot_tham_gia_hoi_nghi_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qldt_chart_ti_le_ban_quan_ly`;
CREATE TABLE IF NOT EXISTS db_admin.`qldt_chart_ti_le_ban_quan_ly` (
  `Ban quản lý` STRING,
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản lý`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qldt_chart_ti_le_ban_quan_ly_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qldt_chart_ti_le_ban_quan_ly_src` (
  `Ban quản lý` STRING,
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản lý`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qldt_chart_ti_le_ban_quan_ly',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qldt_chart_ti_le_ban_quan_ly',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qldt_chart_ti_le_ban_quan_tri`;
CREATE TABLE IF NOT EXISTS db_admin.`qldt_chart_ti_le_ban_quan_tri` (
  `Ban quản trị` STRING,
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản trị`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qldt_chart_ti_le_ban_quan_tri_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qldt_chart_ti_le_ban_quan_tri_src` (
  `Ban quản trị` STRING,
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản trị`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qldt_chart_ti_le_ban_quan_tri',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qldt_chart_ti_le_ban_quan_tri',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qldt_kpi_tong_quan`;
CREATE TABLE IF NOT EXISTS db_admin.`qldt_kpi_tong_quan` (
  `Tổng số chung cư` BIGINT,
  `Chung cư chưa có BQT` BIGINT,
  `Số vụ tranh chấp` BIGINT,
  `Tổng số hội nghị` BIGINT,
  `Số cán bộ tham gia` BIGINT
  , PRIMARY KEY (`Tổng số chung cư`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qldt_kpi_tong_quan_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qldt_kpi_tong_quan_src` (
  `Tổng số chung cư` BIGINT,
  `Chung cư chưa có BQT` BIGINT,
  `Số vụ tranh chấp` BIGINT,
  `Tổng số hội nghị` BIGINT,
  `Số cán bộ tham gia` BIGINT
  , PRIMARY KEY (`Tổng số chung cư`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qldt_kpi_tong_quan',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qldt_kpi_tong_quan',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qlvb_bang_chi_tiet_don_vi`;
CREATE TABLE IF NOT EXISTS db_admin.`qlvb_bang_chi_tiet_don_vi` (
  `Tháng` DATE,
  `Bộ phận` STRING,
  `VB đến - Hoàn thành` INT,
  `VB đến - Quá hạn hoàn thành` INT,
  `VB đến - Chưa hoàn thành` INT,
  `VB đến - Quá hạn chưa hoàn thành` INT,
  `VB đi - Chờ xử lý` INT,
  `VB đi - Đã ban hành` INT,
  `HS công việc - Chưa xử lý` INT,
  `HS công việc - Đang xử lý` INT,
  `HS công việc - Hoàn thành` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qlvb_bang_chi_tiet_don_vi_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qlvb_bang_chi_tiet_don_vi_src` (
  `Tháng` INT,
  `Bộ phận` STRING,
  `VB đến - Hoàn thành` INT,
  `VB đến - Quá hạn hoàn thành` INT,
  `VB đến - Chưa hoàn thành` INT,
  `VB đến - Quá hạn chưa hoàn thành` INT,
  `VB đi - Chờ xử lý` INT,
  `VB đi - Đã ban hành` INT,
  `HS công việc - Chưa xử lý` INT,
  `HS công việc - Đang xử lý` INT,
  `HS công việc - Hoàn thành` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qlvb_bang_chi_tiet_don_vi',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qlvb_bang_chi_tiet_don_vi',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qlvb_chart_ho_so_cong_viec`;
CREATE TABLE IF NOT EXISTS db_admin.`qlvb_chart_ho_so_cong_viec` (
  `Tháng` DATE,
  `Trạng thái` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qlvb_chart_ho_so_cong_viec_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qlvb_chart_ho_so_cong_viec_src` (
  `Tháng` INT,
  `Trạng thái` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qlvb_chart_ho_so_cong_viec',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qlvb_chart_ho_so_cong_viec',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qlvb_chart_van_ban_den`;
CREATE TABLE IF NOT EXISTS db_admin.`qlvb_chart_van_ban_den` (
  `Tháng` DATE,
  `Vai trò xử lý` STRING,
  `Số lượng` INT,
  `Tỷ lệ (%)` DOUBLE
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qlvb_chart_van_ban_den_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qlvb_chart_van_ban_den_src` (
  `Tháng` INT,
  `Vai trò xử lý` STRING,
  `Số lượng` INT,
  `Tỷ lệ (%)` DOUBLE
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qlvb_chart_van_ban_den',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qlvb_chart_van_ban_den',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qlvb_chart_van_ban_di`;
CREATE TABLE IF NOT EXISTS db_admin.`qlvb_chart_van_ban_di` (
  `Tháng` DATE,
  `Trạng thái` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qlvb_chart_van_ban_di_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qlvb_chart_van_ban_di_src` (
  `Tháng` INT,
  `Trạng thái` STRING,
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qlvb_chart_van_ban_di',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qlvb_chart_van_ban_di',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`qlvb_kpi_tong_hop`;
CREATE TABLE IF NOT EXISTS db_admin.`qlvb_kpi_tong_hop` (
  `Tháng` DATE,
  `Mã KPI` STRING,
  `Chỉ tiêu` STRING,
  `Giá trị` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`qlvb_kpi_tong_hop_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`qlvb_kpi_tong_hop_src` (
  `Tháng` INT,
  `Mã KPI` STRING,
  `Chỉ tiêu` STRING,
  `Giá trị` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.qlvb_kpi_tong_hop',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-qlvb_kpi_tong_hop',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_chart1_cocau3nganh`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_chart1_cocau3nganh` (
  `Phường` STRING,
  `Lĩnh vực` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_chart1_cocau3nganh_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_chart1_cocau3nganh_src` (
  `Phường` STRING,
  `Lĩnh vực` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_chart1_cocau3nganh',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_chart1_cocau3nganh',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_chart2_gcn`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_chart2_gcn` (
  `Phường` STRING,
  `Lĩnh vực` STRING,
  `Cấp quản lý` STRING,
  `Có GCN` BIGINT,
  `Không GCN` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_chart2_gcn_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_chart2_gcn_src` (
  `Phường` STRING,
  `Lĩnh vực` STRING,
  `Cấp quản lý` STRING,
  `Có GCN` BIGINT,
  `Không GCN` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_chart2_gcn',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_chart2_gcn',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_chart3_camket`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_chart3_camket` (
  `Phường` STRING,
  `Lĩnh vực` STRING,
  `Cấp quản lý` STRING,
  `Có cam kết` BIGINT,
  `Không cam kết` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_chart3_camket_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_chart3_camket_src` (
  `Phường` STRING,
  `Lĩnh vực` STRING,
  `Cấp quản lý` STRING,
  `Có cam kết` BIGINT,
  `Không cam kết` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_chart3_camket',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_chart3_camket',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_chart4_loaihinh_yte`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_chart4_loaihinh_yte` (
  `Phường` STRING,
  `Cấp quản lý` STRING,
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_chart4_loaihinh_yte_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_chart4_loaihinh_yte_src` (
  `Phường` STRING,
  `Cấp quản lý` STRING,
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_chart4_loaihinh_yte',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_chart4_loaihinh_yte',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_chart5_loaihinh_congthuong`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_chart5_loaihinh_congthuong` (
  `Phường` STRING,
  `Cấp quản lý` STRING,
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_chart5_loaihinh_congthuong_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_chart5_loaihinh_congthuong_src` (
  `Phường` STRING,
  `Cấp quản lý` STRING,
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_chart5_loaihinh_congthuong',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_chart5_loaihinh_congthuong',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_chart6_loaihinh_nongnghiep`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_chart6_loaihinh_nongnghiep` (
  `Phường` STRING,
  `Cấp quản lý` STRING,
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_chart6_loaihinh_nongnghiep_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_chart6_loaihinh_nongnghiep_src` (
  `Phường` STRING,
  `Cấp quản lý` STRING,
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_chart6_loaihinh_nongnghiep',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_chart6_loaihinh_nongnghiep',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_kpi_cards`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_kpi_cards` (
  `Phường` STRING,
  `Tổng Cơ sở có giấy chứng nhận` BIGINT,
  `Tổng Cơ sở có giấy cam kết` BIGINT,
  `Tổng Cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_kpi_cards_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_kpi_cards_src` (
  `Phường` STRING,
  `Tổng Cơ sở có giấy chứng nhận` BIGINT,
  `Tổng Cơ sở có giấy cam kết` BIGINT,
  `Tổng Cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_kpi_cards',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_kpi_cards',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_kpi_chitieu`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_kpi_chitieu` (
  `Tổng đơn vị kiểm tra` BIGINT,
  `Đơn vị chuẩn VSATTP` BIGINT,
  `Đơn vị thanh tra lại` BIGINT,
  `Đơn vị dừng kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng đơn vị kiểm tra`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_kpi_chitieu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_kpi_chitieu_src` (
  `Tổng đơn vị kiểm tra` BIGINT,
  `Đơn vị chuẩn VSATTP` BIGINT,
  `Đơn vị thanh tra lại` BIGINT,
  `Đơn vị dừng kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng đơn vị kiểm tra`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_kpi_chitieu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_kpi_chitieu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_line_canhbao_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_line_canhbao_thang` (
  `Tháng kiểm tra` DATE,
  `Mức độ cảnh báo` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_line_canhbao_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_line_canhbao_thang_src` (
  `Tháng kiểm tra` INT,
  `Mức độ cảnh báo` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_line_canhbao_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_line_canhbao_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_line_vipham_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_line_vipham_thang` (
  `Tháng kiểm tra` DATE,
  `sum(Giấy ĐKKD)` BIGINT,
  `sum(GCN ATTP)` BIGINT,
  `sum(Giấy KSK)` BIGINT,
  `sum(Tập huấn ATTP)` BIGINT,
  `sum(KQ XN nhanh)` BIGINT,
  `sum(Hợp đồng nguyên liệu)` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_line_vipham_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_line_vipham_thang_src` (
  `Tháng kiểm tra` INT,
  `sum(Giấy ĐKKD)` BIGINT,
  `sum(GCN ATTP)` BIGINT,
  `sum(Giấy KSK)` BIGINT,
  `sum(Tập huấn ATTP)` BIGINT,
  `sum(KQ XN nhanh)` BIGINT,
  `sum(Hợp đồng nguyên liệu)` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_line_vipham_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_line_vipham_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_loai_hinh`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_loai_hinh` (
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_loai_hinh_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_loai_hinh_src` (
  `Loại hình` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_loai_hinh',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_loai_hinh',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_pie_canhbao`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_pie_canhbao` (
  `Mức độ cảnh báo` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Mức độ cảnh báo`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_pie_canhbao_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_pie_canhbao_src` (
  `Mức độ cảnh báo` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Mức độ cảnh báo`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_pie_canhbao',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_pie_canhbao',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_pie_trangthai`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_pie_trangthai` (
  `Trạng thái` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Trạng thái`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_pie_trangthai_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_pie_trangthai_src` (
  `Trạng thái` STRING,
  `count` BIGINT
  , PRIMARY KEY (`Trạng thái`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_pie_trangthai',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_pie_trangthai',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`vsattp_pie_vipham_chitieu`;
CREATE TABLE IF NOT EXISTS db_admin.`vsattp_pie_vipham_chitieu` (
  `Chỉ tiêu` STRING,
  `Số lần vi phạm` BIGINT
  , PRIMARY KEY (`Chỉ tiêu`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`vsattp_pie_vipham_chitieu_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`vsattp_pie_vipham_chitieu_src` (
  `Chỉ tiêu` STRING,
  `Số lần vi phạm` BIGINT
  , PRIMARY KEY (`Chỉ tiêu`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.vsattp_pie_vipham_chitieu',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-vsattp_pie_vipham_chitieu',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`yte_bar_loai_hinh`;
CREATE TABLE IF NOT EXISTS db_admin.`yte_bar_loai_hinh` (
  `Loại hình` STRING,
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`yte_bar_loai_hinh_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`yte_bar_loai_hinh_src` (
  `Loại hình` STRING,
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.yte_bar_loai_hinh',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-yte_bar_loai_hinh',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`yte_bar_phan_bo_canh_bao`;
CREATE TABLE IF NOT EXISTS db_admin.`yte_bar_phan_bo_canh_bao` (
  `Loại hình` STRING,
  `Mức cảnh báo` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`yte_bar_phan_bo_canh_bao_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`yte_bar_phan_bo_canh_bao_src` (
  `Loại hình` STRING,
  `Mức cảnh báo` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.yte_bar_phan_bo_canh_bao',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-yte_bar_phan_bo_canh_bao',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`yte_kpi_tong_hop`;
CREATE TABLE IF NOT EXISTS db_admin.`yte_kpi_tong_hop` (
  `Tổng số cơ sở y tế ngoài công lập` BIGINT,
  `Số cơ sở đạt chuẩn` BIGINT,
  `Số cơ sở cần kiểm tra lại` BIGINT,
  `Cơ sở dừng hoạt động` BIGINT
  , PRIMARY KEY (`Tổng số cơ sở y tế ngoài công lập`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`yte_kpi_tong_hop_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`yte_kpi_tong_hop_src` (
  `Tổng số cơ sở y tế ngoài công lập` BIGINT,
  `Số cơ sở đạt chuẩn` BIGINT,
  `Số cơ sở cần kiểm tra lại` BIGINT,
  `Cơ sở dừng hoạt động` BIGINT
  , PRIMARY KEY (`Tổng số cơ sở y tế ngoài công lập`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.yte_kpi_tong_hop',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-yte_kpi_tong_hop',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`yte_line_canh_bao_thang`;
CREATE TABLE IF NOT EXISTS db_admin.`yte_line_canh_bao_thang` (
  `Tháng` DATE,
  `Loại hình` STRING,
  `Mức cảnh báo` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`yte_line_canh_bao_thang_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`yte_line_canh_bao_thang_src` (
  `Tháng` INT,
  `Loại hình` STRING,
  `Mức cảnh báo` STRING,
  `Số lượng` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.yte_line_canh_bao_thang',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-yte_line_canh_bao_thang',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

DROP TABLE IF EXISTS db_admin.`yte_pie_chat_luong`;
CREATE TABLE IF NOT EXISTS db_admin.`yte_pie_chat_luong` (
  `Trạng thái chất lượng` STRING,
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Trạng thái chất lượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`yte_pie_chat_luong_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`yte_pie_chat_luong_src` (
  `Trạng thái chất lượng` STRING,
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Trạng thái chất lượng`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'real_topic.datalakehouse.yte_pie_chat_luong',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-real_admin-yte_pie_chat_luong',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

-- JOB 1/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`cdt_chart_bar_linh_vuc`
SELECT `Lĩnh vực`, DATE '1970-01-01' + CAST(`ky_yyyymm` AS INTERVAL DAY) AS `ky_yyyymm`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Tổng chi đầu tư (Triệu)`
FROM default_catalog.default_database.`cdt_chart_bar_linh_vuc_src`;
INSERT INTO catalog_admin.db_admin.`cdt_chart_bar_tong_dau_tu`
SELECT `Tên dự án`, `Tổng tiền đầu tư (Tỷ đồng)`, `Trạng thái dự án`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Lĩnh vực`, DATE '1970-01-01' + CAST(`ky_yyyymm` AS INTERVAL DAY) AS `ky_yyyymm`
FROM default_catalog.default_database.`cdt_chart_bar_tong_dau_tu_src`;
INSERT INTO catalog_admin.db_admin.`cdt_chart_danh_sach`
SELECT `Mã dự án`, `Tên dự án`, DATE '1970-01-01' + CAST(`ky_yyyymm` AS INTERVAL DAY) AS `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Giải ngân KQ`, `Giải ngân GPMB`, `Lũy kế giải ngân`, `Tỉ lệ giải ngân (%)`
FROM default_catalog.default_database.`cdt_chart_danh_sach_src`;
INSERT INTO catalog_admin.db_admin.`cdt_chart_pie_giai_ngan`
SELECT DATE '1970-01-01' + CAST(`ky_yyyymm` AS INTERVAL DAY) AS `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Loại giải ngân`, `Giá trị`
FROM default_catalog.default_database.`cdt_chart_pie_giai_ngan_src`;
INSERT INTO catalog_admin.db_admin.`cdt_chart_pie_trang_thai`
SELECT `Mã dự án`, `Trạng thái dự án`, DATE '1970-01-01' + CAST(`ky_yyyymm` AS INTERVAL DAY) AS `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`
FROM default_catalog.default_database.`cdt_chart_pie_trang_thai_src`;
INSERT INTO catalog_admin.db_admin.`cdt_kpi_tong_quan`
SELECT `Mã dự án`, DATE '1970-01-01' + CAST(`ky_yyyymm` AS INTERVAL DAY) AS `ky_yyyymm`, `ky_nam`, `ky_thang`, `Ban quản lý`, `Ngân sách thuộc cấp`, `Trạng thái dự án`, `Là tháng mới nhất trong năm`, `Kế hoạch vốn`, `Lũy kế giải ngân`
FROM default_catalog.default_database.`cdt_kpi_tong_quan_src`;
INSERT INTO catalog_admin.db_admin.`congthuong_chart_biendong_cap_phep_ruou_thang`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Loại hình cấp phép`, `Số giấy phép`
FROM default_catalog.default_database.`congthuong_chart_biendong_cap_phep_ruou_thang_src`;
INSERT INTO catalog_admin.db_admin.`congthuong_chart_biendong_kios_thang`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Đơn_vị_quản_lý`, `Lĩnh vực`, `Số lượng ki-ốt`
FROM default_catalog.default_database.`congthuong_chart_biendong_kios_thang_src`;
INSERT INTO catalog_admin.db_admin.`congthuong_chart_cocau_cap_phep_ruou`
SELECT `Loại_hình_cấp_phép`, `Số_cơ_sở`
FROM default_catalog.default_database.`congthuong_chart_cocau_cap_phep_ruou_src`;
INSERT INTO catalog_admin.db_admin.`congthuong_chart_cocau_kios`
SELECT `Đơn_vị_quản_lý`, `Lĩnh vực`, `Số lượng ki-ốt`
FROM default_catalog.default_database.`congthuong_chart_cocau_kios_src`;
END;

-- JOB 2/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`congthuong_chart_nganh_nghe_kinh_doanh`
SELECT `Ngành_nghề`, `Số_lượng`
FROM default_catalog.default_database.`congthuong_chart_nganh_nghe_kinh_doanh_src`;
INSERT INTO catalog_admin.db_admin.`congthuong_kpi_tong_quan`
SELECT `Tổng số Ki-ốt`, `Giấy phép kinh doanh rượu`, `Tổng số cơ sở kinh doanh`
FROM default_catalog.default_database.`congthuong_kpi_tong_quan_src`;
INSERT INTO catalog_admin.db_admin.`dash_chua_giai_ngan_thang`
SELECT `thang`, `tong_chua_giai_ngan`
FROM default_catalog.default_database.`dash_chua_giai_ngan_thang_src`;
INSERT INTO catalog_admin.db_admin.`dash_chua_gn_chi_linh_vuc`
SELECT `thang`, `tong_chua_giai_ngan`, `tong_chua_gn_chi_linh_vuc`, `tong_gn_linh_vuc`
FROM default_catalog.default_database.`dash_chua_gn_chi_linh_vuc_src`;
INSERT INTO catalog_admin.db_admin.`dash_co_cau_du_an_trang_thai`
SELECT `thang`, `trang_thai`, `so_du_an`
FROM default_catalog.default_database.`dash_co_cau_du_an_trang_thai_src`;
INSERT INTO catalog_admin.db_admin.`dash_tong_giai_ngan_thang`
SELECT `thang`, `tong_giai_ngan`
FROM default_catalog.default_database.`dash_tong_giai_ngan_thang_src`;
INSERT INTO catalog_admin.db_admin.`dtvh_chart_le_hoi_theo_thang`
SELECT `Tháng`, `Số lượng lễ hội`, DATE '1970-01-01' + CAST(`ngay` AS INTERVAL DAY) AS `ngay`
FROM default_catalog.default_database.`dtvh_chart_le_hoi_theo_thang_src`;
INSERT INTO catalog_admin.db_admin.`dtvh_chart_loai_hinh_di_tich`
SELECT `Loại hình di tích`, `Số lượng di tích`
FROM default_catalog.default_database.`dtvh_chart_loai_hinh_di_tich_src`;
INSERT INTO catalog_admin.db_admin.`dtvh_chart_nghe_nhan_theo_loai_hinh`
SELECT `Loại hình di sản`, `Số lượng nghệ nhân`
FROM default_catalog.default_database.`dtvh_chart_nghe_nhan_theo_loai_hinh_src`;
INSERT INTO catalog_admin.db_admin.`dtvh_chart_xep_hang_di_tich`
SELECT `Cấp xếp hạng`, `Số lượng di tích`
FROM default_catalog.default_database.`dtvh_chart_xep_hang_di_tich_src`;
END;

-- JOB 3/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`dtvh_kpi_tong_quan`
SELECT `Di tích`, `Di tích đã xếp hạng`, `Nghệ nhân`, `Lễ hội`
FROM default_catalog.default_database.`dtvh_kpi_tong_quan_src`;
INSERT INTO catalog_admin.db_admin.`file_store`
SELECT `id`, `ward_code`, `year`, `category`, `file_name`, `file_path`, `mime_type`, `file_size`, `checksum`, TO_TIMESTAMP_LTZ(`created_at`, 3) AS `created_at`
FROM default_catalog.default_database.`file_store_src`;
INSERT INTO catalog_admin.db_admin.`files_baocao`
SELECT `projects_end_date`, `projects_gpmb_money`, `projects_invest_money`, `projects_projectId`, `projects_projectName`, `projects_projectType`, `projects_start_date`, `projects_status`, `projects_object_gpmb_adress`, `projects_object_gpmb_contact_name`, `projects_object_gpmb_gpmb_type`, `projects_object_gpmb_in_project`, `projects_object_gpmb_in_project_name`, `projects_object_gpmb_person_gpmb_id`, `projects_object_gpmb_person_gpmb_name`, `projects_object_gpmb_person_gpmb_type`, `projects_object_gpmb_phone_num`, `projects_object_gpmb_verify_land`, `projects_object_gpmb_area_land_recall_handed_over`, `projects_object_gpmb_area_land_recall_not_handed_over`, `projects_object_gpmb_area_land_reclaim_argi_land`, `projects_object_gpmb_area_land_reclaim_other_land`, `projects_object_gpmb_area_land_reclaim_resident_land`, `projects_object_gpmb_area_land_reclaim_total_area`, `projects_object_gpmb_indem_money_not_received`, `projects_object_gpmb_indem_money_received`
FROM default_catalog.default_database.`files_baocao_src`;
INSERT INTO catalog_admin.db_admin.`giaoduc_chart_hinh_thuc_quan_ly_gv`
SELECT `Loại trường`, `Hình thức`, `Số lượng`
FROM default_catalog.default_database.`giaoduc_chart_hinh_thuc_quan_ly_gv_src`;
INSERT INTO catalog_admin.db_admin.`giaoduc_chart_quan_ly_phong_hoc`
SELECT `Loại trường`, `Loại phòng`, `Số lượng`
FROM default_catalog.default_database.`giaoduc_chart_quan_ly_phong_hoc_src`;
INSERT INTO catalog_admin.db_admin.`giaoduc_chart_quy_mo_hoc_sinh`
SELECT `Khối lớp`, `Cấp học`, `Học sinh nam`, `Học sinh nữ`, `Tổng học sinh`
FROM default_catalog.default_database.`giaoduc_chart_quy_mo_hoc_sinh_src`;
INSERT INTO catalog_admin.db_admin.`giaoduc_chart_trinh_do_giao_vien`
SELECT `Loại trường`, `Trình độ`, `Số lượng`
FROM default_catalog.default_database.`giaoduc_chart_trinh_do_giao_vien_src`;
INSERT INTO catalog_admin.db_admin.`giaoduc_kpi_so_luong_truong`
SELECT `Loại trường`, `Chỉ tiêu`, `Giá trị`
FROM default_catalog.default_database.`giaoduc_kpi_so_luong_truong_src`;
INSERT INTO catalog_admin.db_admin.`giaoduc_kpi_tong_giao_vien`
SELECT `Loại trường`, `Chức danh`, `Tổng số`
FROM default_catalog.default_database.`giaoduc_kpi_tong_giao_vien_src`;
INSERT INTO catalog_admin.db_admin.`gpmb__pie_progress`
SELECT DATE '1970-01-01' + CAST(`snapshot_date` AS INTERVAL DAY) AS `snapshot_date`, `project_id`, `ten_du_an`, `object_id`, DATE '1970-01-01' + CAST(`planned_finish_date` AS INTERVAL DAY) AS `planned_finish_date`, DATE '1970-01-01' + CAST(`actual_finish_date` AS INTERVAL DAY) AS `actual_finish_date`, `progress_status`
FROM default_catalog.default_database.`gpmb__pie_progress_src`;
END;

-- JOB 4/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`gpmb_chi_dau_tu_phat_trien`
SELECT `nam`, `loai_chi_dau_tu`, `so_lieu`
FROM default_catalog.default_database.`gpmb_chi_dau_tu_phat_trien_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_chi_theo_linh_vuc`
SELECT `nam`, `linh_vuc`, `so_lieu`
FROM default_catalog.default_database.`gpmb_chi_theo_linh_vuc_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_co_cau_chi_ngan_sach`
SELECT `nam`, `loai_chi`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_chi_ngan_sach_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_co_cau_giai_ngan`
SELECT `nam`, `trang_thai`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_giai_ngan_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_co_cau_nguon_thu`
SELECT `nam`, `loai_nguon_thu`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_nguon_thu_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_co_cau_nguon_thu_db`
SELECT `nam`, `loai_nguon_thu`, `so_lieu`
FROM default_catalog.default_database.`gpmb_co_cau_nguon_thu_db_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_dong_ngan_sach`
SELECT `nam`, `loai_ngan_sach`, `so_lieu`
FROM default_catalog.default_database.`gpmb_dong_ngan_sach_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_du_toan_nam`
SELECT `nam`, `loai`, `du_lieu`
FROM default_catalog.default_database.`gpmb_du_toan_nam_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_giaingan_project`
SELECT `Mã dự án`, `Tên dự án`, `Số tiền giải ngân thực tế`
FROM default_catalog.default_database.`gpmb_giaingan_project_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_handover_structure_circle`
SELECT `project_id`, `Tên dự án`, `loai`, `gia_tri`
FROM default_catalog.default_database.`gpmb_handover_structure_circle_src`;
END;

-- JOB 5/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`gpmb_land_structure`
SELECT `project_id`, `Tên dự án`, `loai_dat`, `dien_tich`
FROM default_catalog.default_database.`gpmb_land_structure_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_line_timeline`
SELECT `project_id`, `ten_du_an`, `object_id`, DATE '1970-01-01' + CAST(`date` AS INTERVAL DAY) AS `date`, `step_no`, `step_name`, `step_value`
FROM default_catalog.default_database.`gpmb_line_timeline_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_overview`
SELECT `project_id`, `Tên dự án`, `tong_du_an`, `Số hộ GPMB (dự kiến)`, `Diện tích GPMB (dự kiến)`, `ten_doi_tuong`, `Diện tích`, `so_doi_tuong`, `Chi phí GPMB`, `TMĐT`
FROM default_catalog.default_database.`gpmb_overview_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_overview_2`
SELECT `project_id`, `Tên dự án`, `Chi phí GPMB`, `Diện tích thu hồi`, `ten_doi_tuong`, `tong_du_an`, `TMĐT`
FROM default_catalog.default_database.`gpmb_overview_2_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_quy_mo_nguon_thu`
SELECT `nam`, `thang`, `trieu_vnd`, `chuyen_nguon_nam_truoc`, `thue_phi`, `tien_su_dung_dat`, `thu_bo_sung_cap_tren`, `thu_khac`
FROM default_catalog.default_database.`gpmb_quy_mo_nguon_thu_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_ss_chi_dau_tu`
SELECT `nam`, `loai`, `du_toan`, `thuc_hien`
FROM default_catalog.default_database.`gpmb_ss_chi_dau_tu_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_ss_chi_linh_vuc`
SELECT `nam`, `linh_vuc`, `du_toan`, `thuc_hien`
FROM default_catalog.default_database.`gpmb_ss_chi_linh_vuc_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_tong_so_ngan_sach`
SELECT `nam`, `tong_thu_ngan_sach`, `thu_tren_dia_ban`, `thu_tren_xa_phuong`, `tong_chi_ngan_sach`, `ty_le_thuc_hien_thu`, `ty_le_giai_ngan`
FROM default_catalog.default_database.`gpmb_tong_so_ngan_sach_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_voluntary_structure_circle`
SELECT `project_id`, `Tên dự án`, `loai`, `so_luong`
FROM default_catalog.default_database.`gpmb_voluntary_structure_circle_src`;
INSERT INTO catalog_admin.db_admin.`gpmb_xu_huong_ngan_sach`
SELECT `nam`, `thang`, `trieu_vnd`, `du_toan`, `thuc_hien`, DATE '1970-01-01' + CAST(`thoi_gian` AS INTERVAL DAY) AS `thoi_gian`
FROM default_catalog.default_database.`gpmb_xu_huong_ngan_sach_src`;
END;

-- JOB 6/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`gpmb_xu_huong_thu_chi`
SELECT `nam`, `thang`, `thu`, `chi`, DATE '1970-01-01' + CAST(`thoi_gian` AS INTERVAL DAY) AS `thoi_gian`
FROM default_catalog.default_database.`gpmb_xu_huong_thu_chi_src`;
INSERT INTO catalog_admin.db_admin.`hcc_chart_chi_tieu`
SELECT `Mã chỉ tiêu`, `Nhóm chỉ tiêu`, `UBND Tây Hồ`, `Tp Hà Nội`, `Tỷ lệ (%)`, `Năm`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`hcc_chart_chi_tieu_src`;
INSERT INTO catalog_admin.db_admin.`hcc_chart_ho_so_tiep_nhan`
SELECT `Hình thức nộp hồ sơ`, `Số lượng hồ sơ`, `Tỷ lệ (%)`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`hcc_chart_ho_so_tiep_nhan_src`;
INSERT INTO catalog_admin.db_admin.`hcc_chart_ket_qua_giai_quyet`
SELECT `Trạng thái hồ sơ`, `Số lượng hồ sơ`, `Ghi chú`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`hcc_chart_ket_qua_giai_quyet_src`;
INSERT INTO catalog_admin.db_admin.`hcc_chart_loai_hinh_dich_vu_cong`
SELECT `Loại hình dịch vụ công`, `Số thủ tục`, `Ghi chú`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`hcc_chart_loai_hinh_dich_vu_cong_src`;
INSERT INTO catalog_admin.db_admin.`hcc_kpi_tong_quan`
SELECT `Tổng số hồ sơ tiếp nhận`, `Tỷ lệ đồng bộ lên Cổng DVC Quốc gia (%)`, `Số lượt đánh giá hài lòng`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`hcc_kpi_tong_quan_src`;
INSERT INTO catalog_admin.db_admin.`ktxh_an_ninh_quoc_phong`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_an_ninh_quoc_phong_src`;
INSERT INTO catalog_admin.db_admin.`ktxh_bo_loc_trang_thai`
SELECT `Trạng thái (Quý)`
FROM default_catalog.default_database.`ktxh_bo_loc_trang_thai_src`;
INSERT INTO catalog_admin.db_admin.`ktxh_do_thi`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_do_thi_src`;
INSERT INTO catalog_admin.db_admin.`ktxh_du_an`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_du_an_src`;
END;

-- JOB 7/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`ktxh_kinh_te`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_kinh_te_src`;
INSERT INTO catalog_admin.db_admin.`ktxh_kpi_tong`
SELECT `Kết quả đánh giá KPI`, `count`
FROM default_catalog.default_database.`ktxh_kpi_tong_src`;
INSERT INTO catalog_admin.db_admin.`ktxh_van_hoa_xa_hoi`
SELECT `STT`, `Tên chỉ tiêu`, `Đơn vị`, `Chỉ tiêu (Năm)`, `Quý`, `Chỉ tiêu (Quý)`, `Thực hiện (Quý)`, `Tỷ lệ (%) (Quý)`, `Trạng thái (Quý)`, `Tổng thực hiện (Năm)`, `Tỷ lệ (%) (Năm)`, `Trạng thái (Năm)`
FROM default_catalog.default_database.`ktxh_van_hoa_xa_hoi_src`;
INSERT INTO catalog_admin.db_admin.`moitruong_kpi_tong_quan`
SELECT `Tổng rác thải thu gom (tấn)`, `Tổng tiền xử phạt VPHC (đồng)`, `Tổng lượt tuyên truyền`, `Số cơ sở cam kết an toàn PCCC`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`moitruong_kpi_tong_quan_src`;
INSERT INTO catalog_admin.db_admin.`moitruong_phan_anh_ihanoi`
SELECT `Loại phản ánh`, `Đã giải quyết`, `Chưa giải quyết`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`moitruong_phan_anh_ihanoi_src`;
INSERT INTO catalog_admin.db_admin.`moitruong_thao_do`
SELECT `Hạng mục`, `Số lượng`, `Đơn vị`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`moitruong_thao_do_src`;
INSERT INTO catalog_admin.db_admin.`moitruong_xu_phat_vphc`
SELECT `Loại vi phạm`, `Số trường hợp (vụ)`, `Số tiền phạt (đồng)`, DATE '1970-01-01' + CAST(`ngày` AS INTERVAL DAY) AS `ngày`
FROM default_catalog.default_database.`moitruong_xu_phat_vphc_src`;
INSERT INTO catalog_admin.db_admin.`noivu_chart_biendong_nhansu_thang`
SELECT `Nhóm đối tượng`, DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Hình thức quản lý`, `Số lượng`
FROM default_catalog.default_database.`noivu_chart_biendong_nhansu_thang_src`;
INSERT INTO catalog_admin.db_admin.`noivu_chart_cocau_nhansu`
SELECT `Nhóm đối tượng`, `Biên chế nhà nước`, `Lao động hợp đồng`, `Tổng cộng`
FROM default_catalog.default_database.`noivu_chart_cocau_nhansu_src`;
INSERT INTO catalog_admin.db_admin.`noivu_chart_doitruong_chinhsach`
SELECT `Loại đối tượng`, `Số lượng`
FROM default_catalog.default_database.`noivu_chart_doitruong_chinhsach_src`;
END;

-- JOB 8/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`noivu_chart_hinhthuc_quanly`
SELECT `hinh_thuc_quan_ly`, `so_luong`
FROM default_catalog.default_database.`noivu_chart_hinhthuc_quanly_src`;
INSERT INTO catalog_admin.db_admin.`noivu_kpi_chinhsach`
SELECT `Tổng đối tượng chính sách`
FROM default_catalog.default_database.`noivu_kpi_chinhsach_src`;
INSERT INTO catalog_admin.db_admin.`noivu_kpi_nhansu`
SELECT `Tổng số nhân sự quản lý`, `Tổng biên chế nhà nước`, `Tổng lao động hợp đồng`
FROM default_catalog.default_database.`noivu_kpi_nhansu_src`;
INSERT INTO catalog_admin.db_admin.`qldt_chart_can_bo_tham_du_hoi_nghi`
SELECT `Cán bộ tham gia`, `Lần đầu`, `Thường niên`, `Đột xuất`
FROM default_catalog.default_database.`qldt_chart_can_bo_tham_du_hoi_nghi_src`;
INSERT INTO catalog_admin.db_admin.`qldt_chart_hoat_dong_quan_ly_thang`
SELECT DATE '1970-01-01' + CAST(`ngay` AS INTERVAL DAY) AS `ngay`, `Loại tổ chức`, `Số lượng hội nghị`
FROM default_catalog.default_database.`qldt_chart_hoat_dong_quan_ly_thang_src`;
INSERT INTO catalog_admin.db_admin.`qldt_chart_loai_chung_cu`
SELECT `Loại chung cư`, `Số lượng chung cư`
FROM default_catalog.default_database.`qldt_chart_loai_chung_cu_src`;
INSERT INTO catalog_admin.db_admin.`qldt_chart_luot_tham_gia_hoi_nghi_thang`
SELECT DATE '1970-01-01' + CAST(`ngay` AS INTERVAL DAY) AS `ngay`, `Tên cán bộ`, `Số hội nghị tham gia`
FROM default_catalog.default_database.`qldt_chart_luot_tham_gia_hoi_nghi_thang_src`;
INSERT INTO catalog_admin.db_admin.`qldt_chart_ti_le_ban_quan_ly`
SELECT `Ban quản lý`, `Số lượng chung cư`
FROM default_catalog.default_database.`qldt_chart_ti_le_ban_quan_ly_src`;
INSERT INTO catalog_admin.db_admin.`qldt_chart_ti_le_ban_quan_tri`
SELECT `Ban quản trị`, `Số lượng chung cư`
FROM default_catalog.default_database.`qldt_chart_ti_le_ban_quan_tri_src`;
INSERT INTO catalog_admin.db_admin.`qldt_kpi_tong_quan`
SELECT `Tổng số chung cư`, `Chung cư chưa có BQT`, `Số vụ tranh chấp`, `Tổng số hội nghị`, `Số cán bộ tham gia`
FROM default_catalog.default_database.`qldt_kpi_tong_quan_src`;
END;

-- JOB 9/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`qlvb_bang_chi_tiet_don_vi`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Bộ phận`, `VB đến - Hoàn thành`, `VB đến - Quá hạn hoàn thành`, `VB đến - Chưa hoàn thành`, `VB đến - Quá hạn chưa hoàn thành`, `VB đi - Chờ xử lý`, `VB đi - Đã ban hành`, `HS công việc - Chưa xử lý`, `HS công việc - Đang xử lý`, `HS công việc - Hoàn thành`
FROM default_catalog.default_database.`qlvb_bang_chi_tiet_don_vi_src`;
INSERT INTO catalog_admin.db_admin.`qlvb_chart_ho_so_cong_viec`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Trạng thái`, `Số lượng`
FROM default_catalog.default_database.`qlvb_chart_ho_so_cong_viec_src`;
INSERT INTO catalog_admin.db_admin.`qlvb_chart_van_ban_den`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Vai trò xử lý`, `Số lượng`, `Tỷ lệ (%)`
FROM default_catalog.default_database.`qlvb_chart_van_ban_den_src`;
INSERT INTO catalog_admin.db_admin.`qlvb_chart_van_ban_di`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Trạng thái`, `Số lượng`
FROM default_catalog.default_database.`qlvb_chart_van_ban_di_src`;
INSERT INTO catalog_admin.db_admin.`qlvb_kpi_tong_hop`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Mã KPI`, `Chỉ tiêu`, `Giá trị`
FROM default_catalog.default_database.`qlvb_kpi_tong_hop_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_chart1_cocau3nganh`
SELECT `Phường`, `Lĩnh vực`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart1_cocau3nganh_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_chart2_gcn`
SELECT `Phường`, `Lĩnh vực`, `Cấp quản lý`, `Có GCN`, `Không GCN`
FROM default_catalog.default_database.`vsattp_chart2_gcn_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_chart3_camket`
SELECT `Phường`, `Lĩnh vực`, `Cấp quản lý`, `Có cam kết`, `Không cam kết`
FROM default_catalog.default_database.`vsattp_chart3_camket_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_chart4_loaihinh_yte`
SELECT `Phường`, `Cấp quản lý`, `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart4_loaihinh_yte_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_chart5_loaihinh_congthuong`
SELECT `Phường`, `Cấp quản lý`, `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart5_loaihinh_congthuong_src`;
END;

-- JOB 10/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`vsattp_chart6_loaihinh_nongnghiep`
SELECT `Phường`, `Cấp quản lý`, `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_chart6_loaihinh_nongnghiep_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_kpi_cards`
SELECT `Phường`, `Tổng Cơ sở có giấy chứng nhận`, `Tổng Cơ sở có giấy cam kết`, `Tổng Cơ sở kinh doanh`
FROM default_catalog.default_database.`vsattp_kpi_cards_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_kpi_chitieu`
SELECT `Tổng đơn vị kiểm tra`, `Đơn vị chuẩn VSATTP`, `Đơn vị thanh tra lại`, `Đơn vị dừng kinh doanh`
FROM default_catalog.default_database.`vsattp_kpi_chitieu_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_line_canhbao_thang`
SELECT DATE '1970-01-01' + CAST(`Tháng kiểm tra` AS INTERVAL DAY) AS `Tháng kiểm tra`, `Mức độ cảnh báo`, `count`
FROM default_catalog.default_database.`vsattp_line_canhbao_thang_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_line_vipham_thang`
SELECT DATE '1970-01-01' + CAST(`Tháng kiểm tra` AS INTERVAL DAY) AS `Tháng kiểm tra`, `sum(Giấy ĐKKD)`, `sum(GCN ATTP)`, `sum(Giấy KSK)`, `sum(Tập huấn ATTP)`, `sum(KQ XN nhanh)`, `sum(Hợp đồng nguyên liệu)`
FROM default_catalog.default_database.`vsattp_line_vipham_thang_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_loai_hinh`
SELECT `Loại hình`, `Số lượng`
FROM default_catalog.default_database.`vsattp_loai_hinh_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_pie_canhbao`
SELECT `Mức độ cảnh báo`, `count`
FROM default_catalog.default_database.`vsattp_pie_canhbao_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_pie_trangthai`
SELECT `Trạng thái`, `count`
FROM default_catalog.default_database.`vsattp_pie_trangthai_src`;
INSERT INTO catalog_admin.db_admin.`vsattp_pie_vipham_chitieu`
SELECT `Chỉ tiêu`, `Số lần vi phạm`
FROM default_catalog.default_database.`vsattp_pie_vipham_chitieu_src`;
INSERT INTO catalog_admin.db_admin.`yte_bar_loai_hinh`
SELECT `Loại hình`, `Số lượng cơ sở`
FROM default_catalog.default_database.`yte_bar_loai_hinh_src`;
END;

-- JOB 11/11
EXECUTE STATEMENT SET BEGIN
INSERT INTO catalog_admin.db_admin.`yte_bar_phan_bo_canh_bao`
SELECT `Loại hình`, `Mức cảnh báo`, `Số lượng`
FROM default_catalog.default_database.`yte_bar_phan_bo_canh_bao_src`;
INSERT INTO catalog_admin.db_admin.`yte_kpi_tong_hop`
SELECT `Tổng số cơ sở y tế ngoài công lập`, `Số cơ sở đạt chuẩn`, `Số cơ sở cần kiểm tra lại`, `Cơ sở dừng hoạt động`
FROM default_catalog.default_database.`yte_kpi_tong_hop_src`;
INSERT INTO catalog_admin.db_admin.`yte_line_canh_bao_thang`
SELECT DATE '1970-01-01' + CAST(`Tháng` AS INTERVAL DAY) AS `Tháng`, `Loại hình`, `Mức cảnh báo`, `Số lượng`
FROM default_catalog.default_database.`yte_line_canh_bao_thang_src`;
INSERT INTO catalog_admin.db_admin.`yte_pie_chat_luong`
SELECT `Trạng thái chất lượng`, `Số lượng cơ sở`
FROM default_catalog.default_database.`yte_pie_chat_luong_src`;
END;

