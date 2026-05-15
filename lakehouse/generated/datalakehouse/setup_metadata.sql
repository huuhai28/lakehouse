-- SETUP METADATA FOR datalakehouse
CREATE CATALOG IF NOT EXISTS catalog_datalakehouse WITH (
  'type'='iceberg', 'catalog-type'='hive', 'uri'='thrift://hive-metastore:9083',
  'warehouse'='s3a://catalog_datalakehouse/iceberg-data', 'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000', 's3.path-style-access'='true',
  's3.access-key-id'='admin', 's3.secret-access-key'='password'
);
USE CATALOG catalog_datalakehouse;
CREATE DATABASE IF NOT EXISTS db_datalakehouse;
SET 'parallelism.default' = '1';

CREATE TABLE IF NOT EXISTS db_datalakehouse.`cdt_chart_bar_linh_vuc` (
  `Lĩnh vực` STRING
  `ky_yyyymm` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Tổng chi đầu tư (Triệu)` DOUBLE
  , PRIMARY KEY (`Lĩnh vực`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`cdt_chart_bar_tong_dau_tu` (
  `Tên dự án` STRING
  `Tổng tiền đầu tư (Tỷ đồng)` DOUBLE
  `Trạng thái dự án` STRING
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Lĩnh vực` STRING
  `ky_yyyymm` INT
  , PRIMARY KEY (`Tên dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`cdt_chart_danh_sach` (
  `Mã dự án` STRING
  `Tên dự án` STRING
  `ky_yyyymm` INT
  `ky_nam` INT
  `ky_thang` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Giải ngân KQ` DOUBLE
  `Giải ngân GPMB` DOUBLE
  `Lũy kế giải ngân` DOUBLE
  `Tỉ lệ giải ngân (%)` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`cdt_chart_pie_giai_ngan` (
  `ky_yyyymm` INT
  `ky_nam` INT
  `ky_thang` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  `Loại giải ngân` STRING
  `Giá trị` DOUBLE
  , PRIMARY KEY (`ky_yyyymm`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`cdt_chart_pie_trang_thai` (
  `Mã dự án` STRING
  `Trạng thái dự án` STRING
  `ky_yyyymm` INT
  `ky_nam` INT
  `ky_thang` INT
  `Ban quản lý` STRING
  `Ngân sách thuộc cấp` STRING
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`cdt_kpi_tong_quan` (
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
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`congthuong_chart_biendong_cap_phep_ruou_thang` (
  `Tháng` INT
  `Loại hình cấp phép` STRING
  `Số giấy phép` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`congthuong_chart_biendong_kios_thang` (
  `Tháng` INT
  `Đơn_vị_quản_lý` STRING
  `Lĩnh vực` STRING
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`congthuong_chart_cocau_cap_phep_ruou` (
  `Loại_hình_cấp_phép` STRING
  `Số_cơ_sở` BIGINT
  , PRIMARY KEY (`Loại_hình_cấp_phép`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`congthuong_chart_cocau_kios` (
  `Đơn_vị_quản_lý` STRING
  `Lĩnh vực` STRING
  `Số lượng ki-ốt` BIGINT
  , PRIMARY KEY (`Đơn_vị_quản_lý`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`congthuong_chart_nganh_nghe_kinh_doanh` (
  `Ngành_nghề` STRING
  `Số_lượng` BIGINT
  , PRIMARY KEY (`Ngành_nghề`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`congthuong_kpi_tong_quan` (
  `Tổng số Ki-ốt` BIGINT
  `Giấy phép kinh doanh rượu` BIGINT
  `Tổng số cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng số Ki-ốt`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dash_chua_giai_ngan_thang` (
  `thang` STRING
  `tong_chua_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dash_chua_gn_chi_linh_vuc` (
  `thang` STRING
  `tong_chua_giai_ngan` DOUBLE
  `tong_chua_gn_chi_linh_vuc` DOUBLE
  `tong_gn_linh_vuc` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dash_co_cau_du_an_trang_thai` (
  `thang` STRING
  `trang_thai` STRING
  `so_du_an` BIGINT
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dash_tong_giai_ngan_thang` (
  `thang` STRING
  `tong_giai_ngan` DOUBLE
  , PRIMARY KEY (`thang`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dtvh_chart_le_hoi_theo_thang` (
  `Tháng` INT
  `Số lượng lễ hội` INT
  `ngay` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dtvh_chart_loai_hinh_di_tich` (
  `Loại hình di tích` STRING
  `Số lượng di tích` INT
  , PRIMARY KEY (`Loại hình di tích`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dtvh_chart_nghe_nhan_theo_loai_hinh` (
  `Loại hình di sản` STRING
  `Số lượng nghệ nhân` INT
  , PRIMARY KEY (`Loại hình di sản`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dtvh_chart_xep_hang_di_tich` (
  `Cấp xếp hạng` STRING
  `Số lượng di tích` INT
  , PRIMARY KEY (`Cấp xếp hạng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`dtvh_kpi_tong_quan` (
  `Di tích` INT
  `Di tích đã xếp hạng` INT
  `Nghệ nhân` INT
  `Lễ hội` INT
  , PRIMARY KEY (`Di tích`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`file_store` (
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
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`files_baocao` (
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
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`giaoduc_chart_hinh_thuc_quan_ly_gv` (
  `Loại trường` STRING
  `Hình thức` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`giaoduc_chart_quan_ly_phong_hoc` (
  `Loại trường` STRING
  `Loại phòng` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`giaoduc_chart_quy_mo_hoc_sinh` (
  `Khối lớp` STRING
  `Cấp học` STRING
  `Học sinh nam` INT
  `Học sinh nữ` INT
  `Tổng học sinh` INT
  , PRIMARY KEY (`Khối lớp`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`giaoduc_chart_trinh_do_giao_vien` (
  `Loại trường` STRING
  `Trình độ` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`giaoduc_kpi_so_luong_truong` (
  `Loại trường` STRING
  `Chỉ tiêu` STRING
  `Giá trị` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`giaoduc_kpi_tong_giao_vien` (
  `Loại trường` STRING
  `Chức danh` STRING
  `Tổng số` INT
  , PRIMARY KEY (`Loại trường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb__pie_progress` (
  `snapshot_date` INT
  `project_id` STRING
  `ten_du_an` STRING
  `object_id` STRING
  `planned_finish_date` INT
  `actual_finish_date` INT
  `progress_status` STRING
  , PRIMARY KEY (`snapshot_date`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_chi_dau_tu_phat_trien` (
  `nam` INT
  `loai_chi_dau_tu` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_chi_theo_linh_vuc` (
  `nam` INT
  `linh_vuc` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_co_cau_chi_ngan_sach` (
  `nam` STRING
  `loai_chi` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_co_cau_giai_ngan` (
  `nam` INT
  `trang_thai` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_co_cau_nguon_thu` (
  `nam` STRING
  `loai_nguon_thu` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_co_cau_nguon_thu_db` (
  `nam` STRING
  `loai_nguon_thu` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_dong_ngan_sach` (
  `nam` INT
  `loai_ngan_sach` STRING
  `so_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_du_toan_nam` (
  `nam` STRING
  `loai` STRING
  `du_lieu` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_giaingan_project` (
  `Mã dự án` STRING
  `Tên dự án` STRING
  `Số tiền giải ngân thực tế` DOUBLE
  , PRIMARY KEY (`Mã dự án`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_handover_structure_circle` (
  `project_id` STRING
  `Tên dự án` STRING
  `loai` STRING
  `gia_tri` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_land_structure` (
  `project_id` STRING
  `Tên dự án` STRING
  `loai_dat` STRING
  `dien_tich` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_line_timeline` (
  `project_id` STRING
  `ten_du_an` STRING
  `object_id` STRING
  `date` INT
  `step_no` INT
  `step_name` STRING
  `step_value` INT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_overview` (
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
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_overview_2` (
  `project_id` STRING
  `Tên dự án` STRING
  `Chi phí GPMB` STRING
  `Diện tích thu hồi` DOUBLE
  `ten_doi_tuong` BIGINT
  `tong_du_an` INT
  `TMĐT` DOUBLE
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_quy_mo_nguon_thu` (
  `nam` INT
  `thang` INT
  `trieu_vnd` DOUBLE
  `chuyen_nguon_nam_truoc` DOUBLE
  `thue_phi` DOUBLE
  `tien_su_dung_dat` DOUBLE
  `thu_bo_sung_cap_tren` DOUBLE
  `thu_khac` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_ss_chi_dau_tu` (
  `nam` STRING
  `loai` STRING
  `du_toan` DOUBLE
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_ss_chi_linh_vuc` (
  `nam` STRING
  `linh_vuc` STRING
  `du_toan` DOUBLE
  `thuc_hien` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_tong_so_ngan_sach` (
  `nam` STRING
  `tong_thu_ngan_sach` DOUBLE
  `thu_tren_dia_ban` DOUBLE
  `thu_tren_xa_phuong` DOUBLE
  `tong_chi_ngan_sach` DOUBLE
  `ty_le_thuc_hien_thu` DOUBLE
  `ty_le_giai_ngan` DOUBLE
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_voluntary_structure_circle` (
  `project_id` STRING
  `Tên dự án` STRING
  `loai` STRING
  `so_luong` BIGINT
  , PRIMARY KEY (`project_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_xu_huong_ngan_sach` (
  `nam` INT
  `thang` INT
  `trieu_vnd` DOUBLE
  `du_toan` DOUBLE
  `thuc_hien` DOUBLE
  `thoi_gian` INT
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`gpmb_xu_huong_thu_chi` (
  `nam` INT
  `thang` INT
  `thu` DOUBLE
  `chi` DOUBLE
  `thoi_gian` INT
  , PRIMARY KEY (`nam`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`hcc_chart_chi_tieu` (
  `Mã chỉ tiêu` INT
  `Nhóm chỉ tiêu` STRING
  `UBND Tây Hồ` DOUBLE
  `Tp Hà Nội` DOUBLE
  `Tỷ lệ (%)` DOUBLE
  `Năm` STRING
  `ngày` INT
  , PRIMARY KEY (`Mã chỉ tiêu`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`hcc_chart_ho_so_tiep_nhan` (
  `Hình thức nộp hồ sơ` STRING
  `Số lượng hồ sơ` INT
  `Tỷ lệ (%)` DOUBLE
  `ngày` INT
  , PRIMARY KEY (`Hình thức nộp hồ sơ`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`hcc_chart_ket_qua_giai_quyet` (
  `Trạng thái hồ sơ` STRING
  `Số lượng hồ sơ` INT
  `Ghi chú` STRING
  `ngày` INT
  , PRIMARY KEY (`Trạng thái hồ sơ`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`hcc_chart_loai_hinh_dich_vu_cong` (
  `Loại hình dịch vụ công` STRING
  `Số thủ tục` INT
  `Ghi chú` STRING
  `ngày` INT
  , PRIMARY KEY (`Loại hình dịch vụ công`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`hcc_kpi_tong_quan` (
  `Tổng số hồ sơ tiếp nhận` INT
  `Tỷ lệ đồng bộ lên Cổng DVC Quốc gia (%)` DOUBLE
  `Số lượt đánh giá hài lòng` INT
  `ngày` INT
  , PRIMARY KEY (`Tổng số hồ sơ tiếp nhận`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`ktxh_an_ninh_quoc_phong` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`ktxh_bo_loc_trang_thai` (
  `Trạng thái (Quý)` STRING
  , PRIMARY KEY (`Trạng thái (Quý)`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`ktxh_do_thi` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`ktxh_du_an` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`ktxh_kinh_te` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`ktxh_kpi_tong` (
  `Kết quả đánh giá KPI` STRING
  `count` BIGINT
  , PRIMARY KEY (`Kết quả đánh giá KPI`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`ktxh_van_hoa_xa_hoi` (
  `STT` STRING
  `Tên chỉ tiêu` STRING
  `Đơn vị` STRING
  `Chỉ tiêu (Năm)` STRING
  `Quý` STRING
  `Chỉ tiêu (Quý)` STRING
  `Thực hiện (Quý)` STRING
  `Tỷ lệ (%) (Quý)` STRING
  `Trạng thái (Quý)` STRING
  `Tổng thực hiện (Năm)` STRING
  `Tỷ lệ (%) (Năm)` STRING
  `Trạng thái (Năm)` STRING
  , PRIMARY KEY (`STT`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`moitruong_kpi_tong_quan` (
  `Tổng rác thải thu gom (tấn)` DOUBLE
  `Tổng tiền xử phạt VPHC (đồng)` BIGINT
  `Tổng lượt tuyên truyền` INT
  `Số cơ sở cam kết an toàn PCCC` INT
  `ngày` INT
  , PRIMARY KEY (`Tổng tiền xử phạt VPHC (đồng)`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`moitruong_phan_anh_ihanoi` (
  `Loại phản ánh` STRING
  `Đã giải quyết` INT
  `Chưa giải quyết` INT
  `ngày` INT
  , PRIMARY KEY (`Loại phản ánh`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`moitruong_thao_do` (
  `Hạng mục` STRING
  `Số lượng` INT
  `Đơn vị` STRING
  `ngày` INT
  , PRIMARY KEY (`Hạng mục`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`moitruong_xu_phat_vphc` (
  `Loại vi phạm` STRING
  `Số trường hợp (vụ)` INT
  `Số tiền phạt (đồng)` BIGINT
  `ngày` INT
  , PRIMARY KEY (`Loại vi phạm`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`noivu_chart_biendong_nhansu_thang` (
  `Nhóm đối tượng` STRING
  `Tháng` INT
  `Hình thức quản lý` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`noivu_chart_cocau_nhansu` (
  `Nhóm đối tượng` STRING
  `Biên chế nhà nước` INT
  `Lao động hợp đồng` INT
  `Tổng cộng` INT
  , PRIMARY KEY (`Nhóm đối tượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`noivu_chart_doitruong_chinhsach` (
  `Loại đối tượng` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Loại đối tượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`noivu_chart_hinhthuc_quanly` (
  `hinh_thuc_quan_ly` STRING
  `so_luong` BIGINT
  , PRIMARY KEY (`hinh_thuc_quan_ly`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`noivu_kpi_chinhsach` (
  `Tổng đối tượng chính sách` BIGINT
  , PRIMARY KEY (`Tổng đối tượng chính sách`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`noivu_kpi_nhansu` (
  `Tổng số nhân sự quản lý` BIGINT
  `Tổng biên chế nhà nước` BIGINT
  `Tổng lao động hợp đồng` BIGINT
  , PRIMARY KEY (`Tổng số nhân sự quản lý`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qldt_chart_can_bo_tham_du_hoi_nghi` (
  `Cán bộ tham gia` STRING
  `Lần đầu` BIGINT
  `Thường niên` BIGINT
  `Đột xuất` BIGINT
  , PRIMARY KEY (`Cán bộ tham gia`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qldt_chart_hoat_dong_quan_ly_thang` (
  `ngay` INT
  `Loại tổ chức` STRING
  `Số lượng hội nghị` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qldt_chart_loai_chung_cu` (
  `Loại chung cư` STRING
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Loại chung cư`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qldt_chart_luot_tham_gia_hoi_nghi_thang` (
  `ngay` INT
  `Tên cán bộ` STRING
  `Số hội nghị tham gia` BIGINT
  , PRIMARY KEY (`ngay`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qldt_chart_ti_le_ban_quan_ly` (
  `Ban quản lý` STRING
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản lý`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qldt_chart_ti_le_ban_quan_tri` (
  `Ban quản trị` STRING
  `Số lượng chung cư` BIGINT
  , PRIMARY KEY (`Ban quản trị`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qldt_kpi_tong_quan` (
  `Tổng số chung cư` BIGINT
  `Chung cư chưa có BQT` BIGINT
  `Số vụ tranh chấp` BIGINT
  `Tổng số hội nghị` BIGINT
  `Số cán bộ tham gia` BIGINT
  , PRIMARY KEY (`Tổng số chung cư`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qlvb_bang_chi_tiet_don_vi` (
  `Tháng` INT
  `Bộ phận` STRING
  `VB đến - Hoàn thành` INT
  `VB đến - Quá hạn hoàn thành` INT
  `VB đến - Chưa hoàn thành` INT
  `VB đến - Quá hạn chưa hoàn thành` INT
  `VB đi - Chờ xử lý` INT
  `VB đi - Đã ban hành` INT
  `HS công việc - Chưa xử lý` INT
  `HS công việc - Đang xử lý` INT
  `HS công việc - Hoàn thành` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qlvb_chart_ho_so_cong_viec` (
  `Tháng` INT
  `Trạng thái` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qlvb_chart_van_ban_den` (
  `Tháng` INT
  `Vai trò xử lý` STRING
  `Số lượng` INT
  `Tỷ lệ (%)` DOUBLE
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qlvb_chart_van_ban_di` (
  `Tháng` INT
  `Trạng thái` STRING
  `Số lượng` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`qlvb_kpi_tong_hop` (
  `Tháng` INT
  `Mã KPI` STRING
  `Chỉ tiêu` STRING
  `Giá trị` INT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_chart1_cocau3nganh` (
  `Phường` STRING
  `Lĩnh vực` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_chart2_gcn` (
  `Phường` STRING
  `Lĩnh vực` STRING
  `Cấp quản lý` STRING
  `Có GCN` BIGINT
  `Không GCN` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_chart3_camket` (
  `Phường` STRING
  `Lĩnh vực` STRING
  `Cấp quản lý` STRING
  `Có cam kết` BIGINT
  `Không cam kết` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_chart4_loaihinh_yte` (
  `Phường` STRING
  `Cấp quản lý` STRING
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_chart5_loaihinh_congthuong` (
  `Phường` STRING
  `Cấp quản lý` STRING
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_chart6_loaihinh_nongnghiep` (
  `Phường` STRING
  `Cấp quản lý` STRING
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_kpi_cards` (
  `Phường` STRING
  `Tổng Cơ sở có giấy chứng nhận` BIGINT
  `Tổng Cơ sở có giấy cam kết` BIGINT
  `Tổng Cơ sở kinh doanh` BIGINT
  , PRIMARY KEY (`Phường`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_kpi_chitieu` (
  `Tổng đơn vị kiểm tra` BIGINT
  `Đơn vị chuẩn VSATTP` BIGINT
  `Đơn vị thanh tra lại` BIGINT
  `Đơn vị dừng kinh doanh` BIGINT
  , PRIMARY KEY (`Tổng đơn vị kiểm tra`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_line_canhbao_thang` (
  `Tháng kiểm tra` INT
  `Mức độ cảnh báo` STRING
  `count` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_line_vipham_thang` (
  `Tháng kiểm tra` INT
  `sum(Giấy ĐKKD)` BIGINT
  `sum(GCN ATTP)` BIGINT
  `sum(Giấy KSK)` BIGINT
  `sum(Tập huấn ATTP)` BIGINT
  `sum(KQ XN nhanh)` BIGINT
  `sum(Hợp đồng nguyên liệu)` BIGINT
  , PRIMARY KEY (`Tháng kiểm tra`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_loai_hinh` (
  `Loại hình` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_pie_canhbao` (
  `Mức độ cảnh báo` STRING
  `count` BIGINT
  , PRIMARY KEY (`Mức độ cảnh báo`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_pie_trangthai` (
  `Trạng thái` STRING
  `count` BIGINT
  , PRIMARY KEY (`Trạng thái`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`vsattp_pie_vipham_chitieu` (
  `Chỉ tiêu` STRING
  `Số lần vi phạm` BIGINT
  , PRIMARY KEY (`Chỉ tiêu`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`yte_bar_loai_hinh` (
  `Loại hình` STRING
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`yte_bar_phan_bo_canh_bao` (
  `Loại hình` STRING
  `Mức cảnh báo` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Loại hình`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`yte_kpi_tong_hop` (
  `Tổng số cơ sở y tế ngoài công lập` BIGINT
  `Số cơ sở đạt chuẩn` BIGINT
  `Số cơ sở cần kiểm tra lại` BIGINT
  `Cơ sở dừng hoạt động` BIGINT
  , PRIMARY KEY (`Tổng số cơ sở y tế ngoài công lập`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`yte_line_canh_bao_thang` (
  `Tháng` INT
  `Loại hình` STRING
  `Mức cảnh báo` STRING
  `Số lượng` BIGINT
  , PRIMARY KEY (`Tháng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

CREATE TABLE IF NOT EXISTS db_datalakehouse.`yte_pie_chat_luong` (
  `Trạng thái chất lượng` STRING
  `Số lượng cơ sở` BIGINT
  , PRIMARY KEY (`Trạng thái chất lượng`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

