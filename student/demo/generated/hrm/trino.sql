CREATE SCHEMA IF NOT EXISTS minio.db_hrm WITH (location = 's3://hrm/iceberg-data/db_hrm/');

DROP TABLE IF EXISTS minio.db_hrm.employees;
CREATE TABLE minio.db_hrm.employees (
  emp_id INTEGER,
  name VARCHAR,
  department VARCHAR,
  salary DOUBLE
) WITH (
  external_location = 's3://hrm/iceberg-data/db_hrm/employees/data/',
  format = 'PARQUET'
);

DROP TABLE IF EXISTS minio.db_hrm.attendance;
CREATE TABLE minio.db_hrm.attendance (
  id INTEGER,
  emp_id INTEGER,
  check_in BIGINT,
  check_out BIGINT,
  status VARCHAR
) WITH (
  external_location = 's3://hrm/iceberg-data/db_hrm/attendance/data/',
  format = 'PARQUET'
);

DROP TABLE IF EXISTS minio.db_hrm.leave_requests;
CREATE TABLE minio.db_hrm.leave_requests (
  id INTEGER,
  emp_id INTEGER,
  leave_type VARCHAR,
  start_date INTEGER,
  end_date INTEGER,
  days INTEGER,
  status VARCHAR,
  reason VARCHAR
) WITH (
  external_location = 's3://hrm/iceberg-data/db_hrm/leave_requests/data/',
  format = 'PARQUET'
);

DROP TABLE IF EXISTS minio.db_hrm.payroll;
CREATE TABLE minio.db_hrm.payroll (
  id INTEGER,
  emp_id INTEGER,
  month VARCHAR,
  base_salary DOUBLE,
  deduction DOUBLE,
  bonus DOUBLE,
  net_salary DOUBLE
) WITH (
  external_location = 's3://hrm/iceberg-data/db_hrm/payroll/data/',
  format = 'PARQUET'
);

