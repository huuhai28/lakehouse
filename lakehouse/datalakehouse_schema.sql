CREATE TABLE `cdt_chart_bar_linh_vuc` (
  `Lĩnh vực` longtext,
  `ky_yyyymm` date DEFAULT NULL,
  `Ban quản lý` longtext,
  `Ngân sách thuộc cấp` longtext,
  `Tổng chi đầu tư (Triệu)` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `cdt_chart_bar_tong_dau_tu` (
  `Tên dự án` longtext,
  `Tổng tiền đầu tư (Tỷ đồng)` double DEFAULT NULL,
  `Trạng thái dự án` longtext,
  `Ban quản lý` longtext,
  `Ngân sách thuộc cấp` longtext,
  `Lĩnh vực` longtext,
  `ky_yyyymm` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `cdt_chart_danh_sach` (
  `Mã dự án` longtext,
  `Tên dự án` longtext,
  `ky_yyyymm` date DEFAULT NULL,
  `ky_nam` int DEFAULT NULL,
  `ky_thang` int DEFAULT NULL,
  `Ban quản lý` longtext,
  `Ngân sách thuộc cấp` longtext,
  `Giải ngân KQ` double NOT NULL,
  `Giải ngân GPMB` double NOT NULL,
  `Lũy kế giải ngân` double NOT NULL,
  `Tỉ lệ giải ngân (%)` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `cdt_chart_pie_giai_ngan` (
  `ky_yyyymm` date DEFAULT NULL,
  `ky_nam` int DEFAULT NULL,
  `ky_thang` int DEFAULT NULL,
  `Ban quản lý` longtext,
  `Ngân sách thuộc cấp` longtext,
  `Loại giải ngân` longtext,
  `Giá trị` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `cdt_chart_pie_trang_thai` (
  `Mã dự án` longtext,
  `Trạng thái dự án` longtext,
  `ky_yyyymm` date DEFAULT NULL,
  `ky_nam` int DEFAULT NULL,
  `ky_thang` int DEFAULT NULL,
  `Ban quản lý` longtext,
  `Ngân sách thuộc cấp` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `cdt_kpi_tong_quan` (
  `Mã dự án` longtext,
  `ky_yyyymm` date DEFAULT NULL,
  `ky_nam` int DEFAULT NULL,
  `ky_thang` int DEFAULT NULL,
  `Ban quản lý` longtext,
  `Ngân sách thuộc cấp` longtext,
  `Trạng thái dự án` longtext,
  `Là tháng mới nhất trong năm` int NOT NULL,
  `Kế hoạch vốn` double NOT NULL,
  `Lũy kế giải ngân` double NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `congthuong_chart_biendong_cap_phep_ruou_thang` (
  `Tháng` date DEFAULT NULL,
  `Loại hình cấp phép` longtext,
  `Số giấy phép` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `congthuong_chart_biendong_kios_thang` (
  `Tháng` date DEFAULT NULL,
  `Đơn_vị_quản_lý` longtext NOT NULL,
  `Lĩnh vực` longtext,
  `Số lượng ki-ốt` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `congthuong_chart_cocau_cap_phep_ruou` (
  `Loại_hình_cấp_phép` longtext,
  `Số_cơ_sở` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `congthuong_chart_cocau_kios` (
  `Đơn_vị_quản_lý` longtext,
  `Lĩnh vực` longtext,
  `Số lượng ki-ốt` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `congthuong_chart_nganh_nghe_kinh_doanh` (
  `Ngành_nghề` longtext,
  `Số_lượng` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `congthuong_kpi_tong_quan` (
  `Tổng số Ki-ốt` bigint DEFAULT NULL,
  `Giấy phép kinh doanh rượu` bigint DEFAULT NULL,
  `Tổng số cơ sở kinh doanh` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dash_chua_giai_ngan_thang` (
  `thang` longtext,
  `tong_chua_giai_ngan` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dash_chua_gn_chi_linh_vuc` (
  `thang` longtext,
  `tong_chua_giai_ngan` double DEFAULT NULL,
  `tong_chua_gn_chi_linh_vuc` double DEFAULT NULL,
  `tong_gn_linh_vuc` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dash_co_cau_du_an_trang_thai` (
  `thang` longtext,
  `trang_thai` longtext NOT NULL,
  `so_du_an` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dash_tong_giai_ngan_thang` (
  `thang` longtext,
  `tong_giai_ngan` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dtvh_chart_le_hoi_theo_thang` (
  `Tháng` int NOT NULL,
  `Số lượng lễ hội` int NOT NULL,
  `ngay` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dtvh_chart_loai_hinh_di_tich` (
  `Loại hình di tích` longtext NOT NULL,
  `Số lượng di tích` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dtvh_chart_nghe_nhan_theo_loai_hinh` (
  `Loại hình di sản` longtext NOT NULL,
  `Số lượng nghệ nhân` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dtvh_chart_xep_hang_di_tich` (
  `Cấp xếp hạng` longtext NOT NULL,
  `Số lượng di tích` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dtvh_kpi_tong_quan` (
  `Di tích` int NOT NULL,
  `Di tích đã xếp hạng` int NOT NULL,
  `Nghệ nhân` int NOT NULL,
  `Lễ hội` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `file_store` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `ward_code` varchar(100) NOT NULL,
  `year` int NOT NULL,
  `category` varchar(50) NOT NULL,
  `file_name` varchar(255) NOT NULL,
  `file_path` text NOT NULL,
  `mime_type` varchar(100) DEFAULT NULL,
  `file_size` bigint DEFAULT NULL,
  `checksum` varchar(64) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `files_baocao` (
  `projects_end_date` longtext,
  `projects_gpmb_money` longtext,
  `projects_invest_money` longtext,
  `projects_projectId` longtext,
  `projects_projectName` longtext,
  `projects_projectType` longtext,
  `projects_start_date` longtext,
  `projects_status` longtext,
  `projects_object_gpmb_adress` longtext,
  `projects_object_gpmb_contact_name` longtext,
  `projects_object_gpmb_gpmb_type` longtext,
  `projects_object_gpmb_in_project` longtext,
  `projects_object_gpmb_in_project_name` longtext,
  `projects_object_gpmb_person_gpmb_id` longtext,
  `projects_object_gpmb_person_gpmb_name` longtext,
  `projects_object_gpmb_person_gpmb_type` longtext,
  `projects_object_gpmb_phone_num` longtext,
  `projects_object_gpmb_verify_land` longtext,
  `projects_object_gpmb_area_land_recall_handed_over` longtext,
  `projects_object_gpmb_area_land_recall_not_handed_over` longtext,
  `projects_object_gpmb_area_land_reclaim_argi_land` longtext,
  `projects_object_gpmb_area_land_reclaim_other_land` longtext,
  `projects_object_gpmb_area_land_reclaim_resident_land` longtext,
  `projects_object_gpmb_area_land_reclaim_total_area` longtext,
  `projects_object_gpmb_indem_money_not_received` longtext,
  `projects_object_gpmb_indem_money_received` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `giaoduc_chart_hinh_thuc_quan_ly_gv` (
  `Loại trường` longtext NOT NULL,
  `Hình thức` longtext,
  `Số lượng` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `giaoduc_chart_quan_ly_phong_hoc` (
  `Loại trường` longtext NOT NULL,
  `Loại phòng` longtext,
  `Số lượng` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `giaoduc_chart_quy_mo_hoc_sinh` (
  `Khối lớp` longtext NOT NULL,
  `Cấp học` longtext NOT NULL,
  `Học sinh nam` int DEFAULT NULL,
  `Học sinh nữ` int DEFAULT NULL,
  `Tổng học sinh` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `giaoduc_chart_trinh_do_giao_vien` (
  `Loại trường` longtext NOT NULL,
  `Trình độ` longtext NOT NULL,
  `Số lượng` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `giaoduc_kpi_so_luong_truong` (
  `Loại trường` longtext NOT NULL,
  `Chỉ tiêu` longtext,
  `Giá trị` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `giaoduc_kpi_tong_giao_vien` (
  `Loại trường` longtext NOT NULL,
  `Chức danh` longtext NOT NULL,
  `Tổng số` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb__pie_progress` (
  `snapshot_date` date NOT NULL,
  `project_id` longtext NOT NULL,
  `ten_du_an` longtext NOT NULL,
  `object_id` longtext,
  `planned_finish_date` date DEFAULT NULL,
  `actual_finish_date` date DEFAULT NULL,
  `progress_status` longtext NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_chi_dau_tu_phat_trien` (
  `nam` int DEFAULT NULL,
  `loai_chi_dau_tu` longtext,
  `so_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_chi_theo_linh_vuc` (
  `nam` int DEFAULT NULL,
  `linh_vuc` longtext,
  `so_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_co_cau_chi_ngan_sach` (
  `nam` varchar(10) DEFAULT NULL,
  `loai_chi` longtext,
  `so_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_co_cau_giai_ngan` (
  `nam` int DEFAULT NULL,
  `trang_thai` longtext,
  `so_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_co_cau_nguon_thu` (
  `nam` varchar(10) DEFAULT NULL,
  `loai_nguon_thu` longtext,
  `so_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_co_cau_nguon_thu_db` (
  `nam` varchar(10) DEFAULT NULL,
  `loai_nguon_thu` longtext,
  `so_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_dong_ngan_sach` (
  `nam` int DEFAULT NULL,
  `loai_ngan_sach` longtext,
  `so_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_du_toan_nam` (
  `nam` varchar(10) DEFAULT NULL,
  `loai` varchar(100) DEFAULT NULL,
  `du_lieu` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_giaingan_project` (
  `Mã dự án` longtext NOT NULL,
  `Tên dự án` longtext NOT NULL,
  `Số tiền giải ngân thực tế` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_handover_structure_circle` (
  `project_id` longtext NOT NULL,
  `Tên dự án` longtext NOT NULL,
  `loai` longtext,
  `gia_tri` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_land_structure` (
  `project_id` longtext NOT NULL,
  `Tên dự án` longtext NOT NULL,
  `loai_dat` longtext,
  `dien_tich` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_line_timeline` (
  `project_id` longtext,
  `ten_du_an` longtext,
  `object_id` longtext,
  `date` date NOT NULL,
  `step_no` int NOT NULL,
  `step_name` longtext NOT NULL,
  `step_value` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_overview` (
  `project_id` longtext NOT NULL,
  `Tên dự án` longtext,
  `tong_du_an` int NOT NULL,
  `Số hộ GPMB (dự kiến)` int DEFAULT NULL,
  `Diện tích GPMB (dự kiến)` double DEFAULT NULL,
  `ten_doi_tuong` int DEFAULT NULL,
  `Diện tích` double DEFAULT NULL,
  `so_doi_tuong` longtext NOT NULL,
  `Chi phí GPMB` longtext,
  `TMĐT` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_overview_2` (
  `project_id` longtext NOT NULL,
  `Tên dự án` longtext,
  `Chi phí GPMB` longtext,
  `Diện tích thu hồi` double DEFAULT NULL,
  `ten_doi_tuong` bigint NOT NULL,
  `tong_du_an` int NOT NULL,
  `TMĐT` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_quy_mo_nguon_thu` (
  `nam` int DEFAULT NULL,
  `thang` int DEFAULT NULL,
  `trieu_vnd` double DEFAULT NULL,
  `chuyen_nguon_nam_truoc` double DEFAULT NULL,
  `thue_phi` double DEFAULT NULL,
  `tien_su_dung_dat` double DEFAULT NULL,
  `thu_bo_sung_cap_tren` double DEFAULT NULL,
  `thu_khac` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_ss_chi_dau_tu` (
  `nam` varchar(10) DEFAULT NULL,
  `loai` longtext,
  `du_toan` double DEFAULT NULL,
  `thuc_hien` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_ss_chi_linh_vuc` (
  `nam` varchar(10) DEFAULT NULL,
  `linh_vuc` longtext,
  `du_toan` double DEFAULT NULL,
  `thuc_hien` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_tong_so_ngan_sach` (
  `nam` varchar(10) DEFAULT NULL,
  `tong_thu_ngan_sach` double DEFAULT NULL,
  `thu_tren_dia_ban` double DEFAULT NULL,
  `thu_tren_xa_phuong` double DEFAULT NULL,
  `tong_chi_ngan_sach` double DEFAULT NULL,
  `ty_le_thuc_hien_thu` double DEFAULT NULL,
  `ty_le_giai_ngan` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_voluntary_structure_circle` (
  `project_id` longtext NOT NULL,
  `Tên dự án` longtext NOT NULL,
  `loai` longtext,
  `so_luong` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_xu_huong_ngan_sach` (
  `nam` int DEFAULT NULL,
  `thang` int DEFAULT NULL,
  `trieu_vnd` double DEFAULT NULL,
  `du_toan` double DEFAULT NULL,
  `thuc_hien` double DEFAULT NULL,
  `thoi_gian` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `gpmb_xu_huong_thu_chi` (
  `nam` int DEFAULT NULL,
  `thang` int DEFAULT NULL,
  `thu` double DEFAULT NULL,
  `chi` double DEFAULT NULL,
  `thoi_gian` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `hcc_chart_chi_tieu` (
  `Mã chỉ tiêu` int DEFAULT NULL,
  `Nhóm chỉ tiêu` longtext,
  `UBND Tây Hồ` double DEFAULT NULL,
  `Tp Hà Nội` double DEFAULT NULL,
  `Tỷ lệ (%)` double DEFAULT NULL,
  `Năm` longtext,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `hcc_chart_ho_so_tiep_nhan` (
  `Hình thức nộp hồ sơ` longtext NOT NULL,
  `Số lượng hồ sơ` int NOT NULL,
  `Tỷ lệ (%)` double NOT NULL,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `hcc_chart_ket_qua_giai_quyet` (
  `Trạng thái hồ sơ` longtext NOT NULL,
  `Số lượng hồ sơ` int NOT NULL,
  `Ghi chú` longtext,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `hcc_chart_loai_hinh_dich_vu_cong` (
  `Loại hình dịch vụ công` longtext NOT NULL,
  `Số thủ tục` int NOT NULL,
  `Ghi chú` longtext,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `hcc_kpi_tong_quan` (
  `Tổng số hồ sơ tiếp nhận` int NOT NULL,
  `Tỷ lệ đồng bộ lên Cổng DVC Quốc gia (%)` double NOT NULL,
  `Số lượt đánh giá hài lòng` int NOT NULL,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ktxh_an_ninh_quoc_phong` (
  `STT` longtext NOT NULL,
  `Tên chỉ tiêu` longtext NOT NULL,
  `Đơn vị` longtext,
  `Chỉ tiêu (Năm)` longtext,
  `Quý` longtext,
  `Chỉ tiêu (Quý)` longtext,
  `Thực hiện (Quý)` longtext,
  `Tỷ lệ (%) (Quý)` longtext,
  `Trạng thái (Quý)` longtext,
  `Tổng thực hiện (Năm)` longtext,
  `Tỷ lệ (%) (Năm)` longtext,
  `Trạng thái (Năm)` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ktxh_bo_loc_trang_thai` (
  `Trạng thái (Quý)` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ktxh_do_thi` (
  `STT` longtext NOT NULL,
  `Tên chỉ tiêu` longtext NOT NULL,
  `Đơn vị` longtext,
  `Chỉ tiêu (Năm)` longtext,
  `Quý` longtext,
  `Chỉ tiêu (Quý)` longtext,
  `Thực hiện (Quý)` longtext,
  `Tỷ lệ (%) (Quý)` longtext,
  `Trạng thái (Quý)` longtext,
  `Tổng thực hiện (Năm)` longtext,
  `Tỷ lệ (%) (Năm)` longtext,
  `Trạng thái (Năm)` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ktxh_du_an` (
  `STT` longtext NOT NULL,
  `Tên chỉ tiêu` longtext NOT NULL,
  `Đơn vị` longtext,
  `Chỉ tiêu (Năm)` longtext,
  `Quý` longtext,
  `Chỉ tiêu (Quý)` longtext,
  `Thực hiện (Quý)` longtext,
  `Tỷ lệ (%) (Quý)` longtext,
  `Trạng thái (Quý)` longtext,
  `Tổng thực hiện (Năm)` longtext,
  `Tỷ lệ (%) (Năm)` longtext,
  `Trạng thái (Năm)` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ktxh_kinh_te` (
  `STT` longtext NOT NULL,
  `Tên chỉ tiêu` longtext NOT NULL,
  `Đơn vị` longtext,
  `Chỉ tiêu (Năm)` longtext,
  `Quý` longtext,
  `Chỉ tiêu (Quý)` longtext,
  `Thực hiện (Quý)` longtext,
  `Tỷ lệ (%) (Quý)` longtext,
  `Trạng thái (Quý)` longtext,
  `Tổng thực hiện (Năm)` longtext,
  `Tỷ lệ (%) (Năm)` longtext,
  `Trạng thái (Năm)` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ktxh_kpi_tong` (
  `Kết quả đánh giá KPI` longtext,
  `count` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ktxh_van_hoa_xa_hoi` (
  `STT` longtext NOT NULL,
  `Tên chỉ tiêu` longtext NOT NULL,
  `Đơn vị` longtext,
  `Chỉ tiêu (Năm)` longtext,
  `Quý` longtext,
  `Chỉ tiêu (Quý)` longtext,
  `Thực hiện (Quý)` longtext,
  `Tỷ lệ (%) (Quý)` longtext,
  `Trạng thái (Quý)` longtext,
  `Tổng thực hiện (Năm)` longtext,
  `Tỷ lệ (%) (Năm)` longtext,
  `Trạng thái (Năm)` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `moitruong_kpi_tong_quan` (
  `Tổng rác thải thu gom (tấn)` double NOT NULL,
  `Tổng tiền xử phạt VPHC (đồng)` bigint NOT NULL,
  `Tổng lượt tuyên truyền` int NOT NULL,
  `Số cơ sở cam kết an toàn PCCC` int NOT NULL,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `moitruong_phan_anh_ihanoi` (
  `Loại phản ánh` longtext NOT NULL,
  `Đã giải quyết` int NOT NULL,
  `Chưa giải quyết` int NOT NULL,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `moitruong_thao_do` (
  `Hạng mục` longtext NOT NULL,
  `Số lượng` int NOT NULL,
  `Đơn vị` longtext NOT NULL,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `moitruong_xu_phat_vphc` (
  `Loại vi phạm` longtext NOT NULL,
  `Số trường hợp (vụ)` int NOT NULL,
  `Số tiền phạt (đồng)` bigint NOT NULL,
  `ngày` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `noivu_chart_biendong_nhansu_thang` (
  `Nhóm đối tượng` longtext,
  `Tháng` date DEFAULT NULL,
  `Hình thức quản lý` longtext,
  `Số lượng` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `noivu_chart_cocau_nhansu` (
  `Nhóm đối tượng` longtext,
  `Biên chế nhà nước` int DEFAULT NULL,
  `Lao động hợp đồng` int DEFAULT NULL,
  `Tổng cộng` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `noivu_chart_doitruong_chinhsach` (
  `Loại đối tượng` longtext,
  `Số lượng` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `noivu_chart_hinhthuc_quanly` (
  `hinh_thuc_quan_ly` longtext,
  `so_luong` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `noivu_kpi_chinhsach` (
  `Tổng đối tượng chính sách` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `noivu_kpi_nhansu` (
  `Tổng số nhân sự quản lý` bigint DEFAULT NULL,
  `Tổng biên chế nhà nước` bigint DEFAULT NULL,
  `Tổng lao động hợp đồng` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qldt_chart_can_bo_tham_du_hoi_nghi` (
  `Cán bộ tham gia` longtext,
  `Lần đầu` bigint DEFAULT NULL,
  `Thường niên` bigint DEFAULT NULL,
  `Đột xuất` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qldt_chart_hoat_dong_quan_ly_thang` (
  `ngay` date DEFAULT NULL,
  `Loại tổ chức` longtext,
  `Số lượng hội nghị` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qldt_chart_loai_chung_cu` (
  `Loại chung cư` longtext NOT NULL,
  `Số lượng chung cư` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qldt_chart_luot_tham_gia_hoi_nghi_thang` (
  `ngay` date DEFAULT NULL,
  `Tên cán bộ` longtext,
  `Số hội nghị tham gia` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qldt_chart_ti_le_ban_quan_ly` (
  `Ban quản lý` longtext,
  `Số lượng chung cư` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qldt_chart_ti_le_ban_quan_tri` (
  `Ban quản trị` longtext NOT NULL,
  `Số lượng chung cư` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qldt_kpi_tong_quan` (
  `Tổng số chung cư` bigint NOT NULL,
  `Chung cư chưa có BQT` bigint DEFAULT NULL,
  `Số vụ tranh chấp` bigint DEFAULT NULL,
  `Tổng số hội nghị` bigint NOT NULL,
  `Số cán bộ tham gia` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qlvb_bang_chi_tiet_don_vi` (
  `Tháng` date DEFAULT NULL,
  `Bộ phận` longtext NOT NULL,
  `VB đến - Hoàn thành` int NOT NULL,
  `VB đến - Quá hạn hoàn thành` int NOT NULL,
  `VB đến - Chưa hoàn thành` int NOT NULL,
  `VB đến - Quá hạn chưa hoàn thành` int NOT NULL,
  `VB đi - Chờ xử lý` int NOT NULL,
  `VB đi - Đã ban hành` int NOT NULL,
  `HS công việc - Chưa xử lý` int NOT NULL,
  `HS công việc - Đang xử lý` int NOT NULL,
  `HS công việc - Hoàn thành` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qlvb_chart_ho_so_cong_viec` (
  `Tháng` date DEFAULT NULL,
  `Trạng thái` longtext NOT NULL,
  `Số lượng` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qlvb_chart_van_ban_den` (
  `Tháng` date DEFAULT NULL,
  `Vai trò xử lý` longtext NOT NULL,
  `Số lượng` int NOT NULL,
  `Tỷ lệ (%)` double NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qlvb_chart_van_ban_di` (
  `Tháng` date DEFAULT NULL,
  `Trạng thái` longtext NOT NULL,
  `Số lượng` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `qlvb_kpi_tong_hop` (
  `Tháng` date DEFAULT NULL,
  `Mã KPI` longtext NOT NULL,
  `Chỉ tiêu` longtext NOT NULL,
  `Giá trị` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_chart1_cocau3nganh` (
  `Phường` longtext,
  `Lĩnh vực` longtext,
  `Số lượng` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_chart2_gcn` (
  `Phường` longtext,
  `Lĩnh vực` longtext,
  `Cấp quản lý` longtext,
  `Có GCN` bigint DEFAULT NULL,
  `Không GCN` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_chart3_camket` (
  `Phường` longtext,
  `Lĩnh vực` longtext,
  `Cấp quản lý` longtext,
  `Có cam kết` bigint DEFAULT NULL,
  `Không cam kết` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_chart4_loaihinh_yte` (
  `Phường` longtext,
  `Cấp quản lý` longtext,
  `Loại hình` longtext,
  `Số lượng` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_chart5_loaihinh_congthuong` (
  `Phường` longtext,
  `Cấp quản lý` longtext,
  `Loại hình` longtext,
  `Số lượng` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_chart6_loaihinh_nongnghiep` (
  `Phường` longtext,
  `Cấp quản lý` longtext,
  `Loại hình` longtext,
  `Số lượng` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_kpi_cards` (
  `Phường` longtext,
  `Tổng Cơ sở có giấy chứng nhận` bigint DEFAULT NULL,
  `Tổng Cơ sở có giấy cam kết` bigint DEFAULT NULL,
  `Tổng Cơ sở kinh doanh` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_kpi_chitieu` (
  `Tổng đơn vị kiểm tra` bigint NOT NULL,
  `Đơn vị chuẩn VSATTP` bigint DEFAULT NULL,
  `Đơn vị thanh tra lại` bigint DEFAULT NULL,
  `Đơn vị dừng kinh doanh` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_line_canhbao_thang` (
  `Tháng kiểm tra` date DEFAULT NULL,
  `Mức độ cảnh báo` longtext NOT NULL,
  `count` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_line_vipham_thang` (
  `Tháng kiểm tra` date DEFAULT NULL,
  `sum(Giấy ĐKKD)` bigint DEFAULT NULL,
  `sum(GCN ATTP)` bigint DEFAULT NULL,
  `sum(Giấy KSK)` bigint DEFAULT NULL,
  `sum(Tập huấn ATTP)` bigint DEFAULT NULL,
  `sum(KQ XN nhanh)` bigint DEFAULT NULL,
  `sum(Hợp đồng nguyên liệu)` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_loai_hinh` (
  `Loại hình` longtext,
  `Số lượng` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_pie_canhbao` (
  `Mức độ cảnh báo` longtext NOT NULL,
  `count` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_pie_trangthai` (
  `Trạng thái` longtext NOT NULL,
  `count` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `vsattp_pie_vipham_chitieu` (
  `Chỉ tiêu` longtext,
  `Số lần vi phạm` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `yte_bar_loai_hinh` (
  `Loại hình` longtext NOT NULL,
  `Số lượng cơ sở` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `yte_bar_phan_bo_canh_bao` (
  `Loại hình` longtext NOT NULL,
  `Mức cảnh báo` longtext NOT NULL,
  `Số lượng` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `yte_kpi_tong_hop` (
  `Tổng số cơ sở y tế ngoài công lập` bigint NOT NULL,
  `Số cơ sở đạt chuẩn` bigint DEFAULT NULL,
  `Số cơ sở cần kiểm tra lại` bigint DEFAULT NULL,
  `Cơ sở dừng hoạt động` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `yte_line_canh_bao_thang` (
  `Tháng` date DEFAULT NULL,
  `Loại hình` longtext NOT NULL,
  `Mức cảnh báo` longtext NOT NULL,
  `Số lượng` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `yte_pie_chat_luong` (
  `Trạng thái chất lượng` longtext NOT NULL,
  `Số lượng cơ sở` bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

