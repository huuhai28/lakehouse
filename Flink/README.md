# 🔄 Flink Analytics Pipeline — Luồng 2

Thư mục này chứa các **Flink SQL Jobs** để xử lý, transform và tạo bảng tổng hợp analytics từ dữ liệu CDC của MySQL.

Khác với Luồng 1 (Iceberg Sink đơn thuần), Luồng 2 dùng **Flink SQL** để thực hiện aggregation, enrichment và tạo ra các bảng phân tích theo thời gian thực.

---

## 🗂️ Cấu trúc

```
flink/
├── README.md                   ← file này
├── setup_hdfs_flink.sh         ← Deploy pipeline cho DB hdfs
└── jobs/
    └── hdfs_analytics.sql      ← Flink SQL: 5 jobs tổng hợp dân số
```

---

## 📊 Pipeline: HDFS Citizen Analytics

**Nguồn dữ liệu**: MySQL DB `hdfs`
- `Bang_tong_hop_cong_dan` — dữ liệu dân số theo huyện/xã/giới tính
- `Phan_tich_log` — log phân tích hệ thống
- `table_stats` — thống kê bảng

**Đích**: MinIO bucket `hdfs-lake` → Iceberg namespace `iceberg.db_hdfs`

### Bảng Iceberg được tạo ra

| Bảng | Mô tả |
|------|-------|
| `Bang_tong_hop_cong_dan` | Bảng gốc (upsert từ CDC) |
| `Phan_tich_log` | Log phân tích (upsert từ CDC) |
| `stats_dan_so_theo_huyen` | 📊 **Tổng dân số, tỉ lệ nam/nữ, tuổi TB theo huyện** |
| `stats_gioi_tinh` | 📊 **Phân bố giới tính toàn tỉnh** |
| `stats_nghe_nghiep` | 📊 **Top nghề nghiệp phổ biến + tỉ lệ %** |

---

## 🚀 Cách chạy

```bash
# Vào thư mục flink
cd ~/Lakehouse/flink

# Lần đầu: reset + deploy
bash setup_hdfs_flink.sh --reset

# Lần sau (không xóa data cũ):
bash setup_hdfs_flink.sh
```

---

## 🎯 Truy vấn kiểm tra

```sql
-- Xem toàn bộ bảng đã tạo
SHOW TABLES IN iceberg.db_hdfs;

-- Thống kê dân số theo huyện (sắp xếp theo dân số)
SELECT * FROM iceberg.db_hdfs.stats_dan_so_theo_huyen
ORDER BY tong_dan_so DESC;

-- Phân bố giới tính
SELECT * FROM iceberg.db_hdfs.stats_gioi_tinh;

-- Top nghề nghiệp phổ biến
SELECT * FROM iceberg.db_hdfs.stats_nghe_nghiep
ORDER BY ty_le_tren_tong_dan DESC;
```

Chạy qua CLI:
```bash
docker exec trino trino --user admin \
  --execute "SELECT * FROM iceberg.db_hdfs.stats_dan_so_theo_huyen ORDER BY tong_dan_so DESC"
```

---

## ⚙️ Flink Jobs chạy bên trong

File `jobs/hdfs_analytics.sql` submit **5 jobs song song** qua `STATEMENT SET`:

| Job | Mô tả |
|-----|-------|
| Job 1 | Ingest bảng gốc `Bang_tong_hop_cong_dan` → Iceberg |
| Job 2 | Ingest `Phan_tich_log` → Iceberg |
| Job 3 | Aggregate: thống kê dân số **theo huyện** |
| Job 4 | Aggregate: thống kê **theo giới tính** |
| Job 5 | Aggregate: **top nghề nghiệp** + tỉ lệ % |

---

*Thêm project mới: tạo file `jobs/<project>_analytics.sql` + script `setup_<project>_flink.sh` theo cùng mẫu.*
