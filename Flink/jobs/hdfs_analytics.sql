-- =============================================================
--  Flink SQL Job: HDFS Citizen Analytics Pipeline
--  Luồng: MySQL(hdfs) → Kafka(Debezium) → Flink → Iceberg(MinIO)
--  Bucket MinIO : hdfs-lake
--  Catalog      : hive (HMS thrift://hive-metastore:9083)
--  Namespace    : db_hdfs
-- =============================================================

-- ─── 1. CATALOG ──────────────────────────────────────────────
CREATE CATALOG hdfs_catalog WITH (
    'type'                = 'iceberg',
    'catalog-type'        = 'hive',
    'uri'                 = 'thrift://hive-metastore:9083',
    'warehouse'           = 's3a://hdfs-lake/iceberg-data',
    'io-impl'             = 'org.apache.iceberg.aws.s3.S3FileIO',
    'property-version'    = '1',
    's3.endpoint'         = 'http://minio:9000',
    's3.path-style-access'= 'true',
    's3.access-key-id'    = 'admin',
    's3.secret-access-key'= 'password',
    'client.region'       = 'us-east-1'
);

USE CATALOG hdfs_catalog;

CREATE DATABASE IF NOT EXISTS db_hdfs;
USE db_hdfs;

-- ─── 2. ICEBERG DESTINATION TABLES ───────────────────────────

-- Bảng gốc: tổng hợp dân số
CREATE TABLE IF NOT EXISTS Bang_tong_hop_cong_dan (
    huyen_thanh_pho         STRING,
    xa_phuong               STRING,
    gioi_tinh               STRING,
    so_luong                BIGINT,
    tuoi_trung_binh         INT,
    nghe_nghiep_pho_bien    STRING,
    so_nghe_nghiep_khac     BIGINT,
    ingested_at             TIMESTAMP(3),
    PRIMARY KEY (huyen_thanh_pho, xa_phuong, gioi_tinh) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

-- Bảng phân tích log hệ thống
CREATE TABLE IF NOT EXISTS Phan_tich_log (
    id                  BIGINT,
    log_time            TIMESTAMP(3),
    log_level           STRING,
    source_table        STRING,
    action_type         STRING,
    record_count        BIGINT,
    message             STRING,
    ingested_at         TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

-- Bảng analytics: thống kê dân số theo huyện (aggregated)
CREATE TABLE IF NOT EXISTS stats_dan_so_theo_huyen (
    huyen_thanh_pho         STRING,
    tong_dan_so             BIGINT,
    ti_le_nam               DOUBLE,
    ti_le_nu                DOUBLE,
    tuoi_tb_chung           DOUBLE,
    so_xa_phuong            BIGINT,
    nghe_pho_bien_nhat      STRING,
    cap_nhat_luc            TIMESTAMP(3),
    PRIMARY KEY (huyen_thanh_pho) NOT ENFORCED
) WITH (
    'format-version'       = '2',
    'write.upsert.enabled' = 'true'
);

-- Bảng analytics: phân tích theo giới tính
CREATE TABLE IF NOT EXISTS stats_gioi_tinh (
    gioi_tinh               STRING,
    tong_dan_so             BIGINT,
    tuoi_trung_binh         DOUBLE,
    so_nghe_nghiep_avg      DOUBLE,
    cap_nhat_luc            TIMESTAMP(3),
    PRIMARY KEY (gioi_tinh) NOT ENFORCED
) WITH (
    'format-version'       = '2',
    'write.upsert.enabled' = 'true'
);

-- Bảng analytics: top nghề nghiệp phổ biến
CREATE TABLE IF NOT EXISTS stats_nghe_nghiep (
    nghe_nghiep             STRING,
    so_xa_co_nghe_nay       BIGINT,
    tong_dan_lam_nghe       BIGINT,
    ty_le_tren_tong_dan     DOUBLE,
    cap_nhat_luc            TIMESTAMP(3),
    PRIMARY KEY (nghe_nghiep) NOT ENFORCED
) WITH (
    'format-version'       = '2',
    'write.upsert.enabled' = 'true'
);

-- ─── 3. KAFKA SOURCE TABLES (Debezium CDC) ────────────────────

CREATE TABLE IF NOT EXISTS kafka_cdc_cong_dan (
    huyen_thanh_pho         STRING,
    xa_phuong               STRING,
    gioi_tinh               STRING,
    so_luong                BIGINT,
    tuoi_trung_binh         INT,
    nghe_nghiep_pho_bien    STRING,
    so_nghe_nghiep_khac     BIGINT,
    PRIMARY KEY (huyen_thanh_pho, xa_phuong, gioi_tinh) NOT ENFORCED
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'topic_hdfs.hdfs.Bang_tong_hop_cong_dan',
    'properties.bootstrap.servers'  = 'kafka:9092',
    'properties.group.id'           = 'flink-hdfs-cong-dan-consumer',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'debezium-json',
    'debezium-json.schema-include'  = 'true'
);

CREATE TABLE IF NOT EXISTS kafka_cdc_phan_tich_log (
    id                  BIGINT,
    log_time            STRING,
    log_level           STRING,
    source_table        STRING,
    action_type         STRING,
    record_count        BIGINT,
    message             STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'topic_hdfs.hdfs.Phan_tich_log',
    'properties.bootstrap.servers'  = 'kafka:9092',
    'properties.group.id'           = 'flink-hdfs-log-consumer',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'debezium-json',
    'debezium-json.schema-include'  = 'true'
);

-- ─── 4. INSERT JOBS (STATEMENT SET) ──────────────────────────

BEGIN STATEMENT SET;

-- Job 1: Ingest bảng gốc công dân → Iceberg
INSERT INTO Bang_tong_hop_cong_dan
SELECT
    huyen_thanh_pho,
    xa_phuong,
    gioi_tinh,
    so_luong,
    tuoi_trung_binh,
    nghe_nghiep_pho_bien,
    so_nghe_nghiep_khac,
    CURRENT_TIMESTAMP AS ingested_at
FROM kafka_cdc_cong_dan;

-- Job 2: Ingest log phân tích → Iceberg
INSERT INTO Phan_tich_log
SELECT
    id,
    TO_TIMESTAMP(log_time, 'yyyy-MM-dd HH:mm:ss'),
    log_level,
    source_table,
    action_type,
    record_count,
    message,
    CURRENT_TIMESTAMP AS ingested_at
FROM kafka_cdc_phan_tich_log;

-- Job 3: Thống kê dân số theo huyện (real-time aggregation)
INSERT INTO stats_dan_so_theo_huyen
SELECT
    huyen_thanh_pho,
    SUM(so_luong)                                               AS tong_dan_so,
    ROUND(
        SUM(CASE WHEN gioi_tinh = 'Nam' THEN so_luong ELSE 0 END) * 100.0
        / NULLIF(SUM(so_luong), 0), 2)                         AS ti_le_nam,
    ROUND(
        SUM(CASE WHEN gioi_tinh = 'Nữ' THEN so_luong ELSE 0 END) * 100.0
        / NULLIF(SUM(so_luong), 0), 2)                         AS ti_le_nu,
    ROUND(AVG(CAST(tuoi_trung_binh AS DOUBLE)), 1)             AS tuoi_tb_chung,
    COUNT(DISTINCT xa_phuong)                                   AS so_xa_phuong,
    -- Nghề phổ biến nhất theo huyện (lấy nghề xuất hiện nhiều nhất)
    FIRST_VALUE(nghe_nghiep_pho_bien)
        OVER (PARTITION BY huyen_thanh_pho
              ORDER BY so_luong DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nghe_pho_bien_nhat,
    CURRENT_TIMESTAMP                                           AS cap_nhat_luc
FROM kafka_cdc_cong_dan
GROUP BY huyen_thanh_pho;

-- Job 4: Thống kê theo giới tính toàn tỉnh
INSERT INTO stats_gioi_tinh
SELECT
    gioi_tinh,
    SUM(so_luong)                                       AS tong_dan_so,
    ROUND(AVG(CAST(tuoi_trung_binh AS DOUBLE)), 1)     AS tuoi_trung_binh,
    ROUND(AVG(CAST(so_nghe_nghiep_khac AS DOUBLE)), 1) AS so_nghe_nghiep_avg,
    CURRENT_TIMESTAMP                                   AS cap_nhat_luc
FROM kafka_cdc_cong_dan
GROUP BY gioi_tinh;

-- Job 5: Top nghề nghiệp phổ biến
INSERT INTO stats_nghe_nghiep
SELECT
    nghe_nghiep_pho_bien                                        AS nghe_nghiep,
    COUNT(DISTINCT xa_phuong)                                   AS so_xa_co_nghe_nay,
    SUM(so_luong)                                               AS tong_dan_lam_nghe,
    ROUND(
        SUM(so_luong) * 100.0
        / SUM(SUM(so_luong)) OVER (), 2)                        AS ty_le_tren_tong_dan,
    CURRENT_TIMESTAMP                                           AS cap_nhat_luc
FROM kafka_cdc_cong_dan
GROUP BY nghe_nghiep_pho_bien;

END;
