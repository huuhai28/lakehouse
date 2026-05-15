-- GENERATED PIPELINE FOR PROJECT: hrm
SET 'execution.checkpointing.interval' = '60s';
SET 'table.exec.sink.upsert-materialize' = 'AUTO';

CREATE CATALOG hrm WITH (
  'type'                 = 'iceberg',
  'catalog-impl'         = 'org.apache.iceberg.rest.RESTCatalog',
  'uri'                  = 'http://polaris:8181/api/catalog',
  'credential'           = '1a03dd5485878fa3:f76b3ebb36d7ad1d0322e7e1c97575d9',
  'warehouse'            = 'hrm',
  'header.X-Polaris-Realm' = 'POLARIS',
  'scope'                = 'PRINCIPAL_ROLE:ALL',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'password'
);
USE CATALOG hrm;
CREATE DATABASE IF NOT EXISTS db_hrm;

DROP TABLE IF EXISTS db_hrm.employees;
DROP TABLE IF EXISTS db_hrm.`employees`;
CREATE TABLE IF NOT EXISTS db_hrm.`employees` (
  `emp_id` INT,
  `name` STRING,
  `department` STRING,
  `salary` DOUBLE
  , PRIMARY KEY (`emp_id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`employees_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`employees_src` (
  `emp_id` INT,
  `name` STRING,
  `department` STRING,
  `salary` DOUBLE
  , PRIMARY KEY (`emp_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'hrm_mysql.hrm.employees',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-hrm-employees',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

INSERT INTO hrm.db_hrm.`employees`
SELECT * FROM default_catalog.default_database.`employees_src`;

--------------------------------------------------
DROP TABLE IF EXISTS db_hrm.attendance;
DROP TABLE IF EXISTS db_hrm.`attendance`;
CREATE TABLE IF NOT EXISTS db_hrm.`attendance` (
  `id` INT,
  `emp_id` INT,
  `check_in` BIGINT,
  `check_out` BIGINT,
  `status` STRING
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`attendance_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`attendance_src` (
  `id` INT,
  `emp_id` INT,
  `check_in` BIGINT,
  `check_out` BIGINT,
  `status` STRING
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'hrm_mysql.hrm.attendance',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-hrm-attendance',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

INSERT INTO hrm.db_hrm.`attendance`
SELECT * FROM default_catalog.default_database.`attendance_src`;

--------------------------------------------------
DROP TABLE IF EXISTS db_hrm.leave_requests;
DROP TABLE IF EXISTS db_hrm.`leave_requests`;
CREATE TABLE IF NOT EXISTS db_hrm.`leave_requests` (
  `id` INT,
  `emp_id` INT,
  `leave_type` STRING,
  `start_date` INT,
  `end_date` INT,
  `days` INT,
  `status` STRING,
  `reason` STRING
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`leave_requests_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`leave_requests_src` (
  `id` INT,
  `emp_id` INT,
  `leave_type` STRING,
  `start_date` INT,
  `end_date` INT,
  `days` INT,
  `status` STRING,
  `reason` STRING
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'hrm_mysql.hrm.leave_requests',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-hrm-leave_requests',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

INSERT INTO hrm.db_hrm.`leave_requests`
SELECT * FROM default_catalog.default_database.`leave_requests_src`;

--------------------------------------------------
DROP TABLE IF EXISTS db_hrm.payroll;
DROP TABLE IF EXISTS db_hrm.`payroll`;
CREATE TABLE IF NOT EXISTS db_hrm.`payroll` (
  `id` INT,
  `emp_id` INT,
  `month` STRING,
  `base_salary` DOUBLE,
  `deduction` DOUBLE,
  `bonus` DOUBLE,
  `net_salary` DOUBLE
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('write.upsert.enabled'='true','format-version'='2');

DROP TABLE IF EXISTS default_catalog.default_database.`payroll_src`;
CREATE TABLE IF NOT EXISTS default_catalog.default_database.`payroll_src` (
  `id` INT,
  `emp_id` INT,
  `month` STRING,
  `base_salary` DOUBLE,
  `deduction` DOUBLE,
  `bonus` DOUBLE,
  `net_salary` DOUBLE
  , PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'hrm_mysql.hrm.payroll',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-hrm-payroll',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'debezium-json',
  'debezium-json.schema-include' = 'true'
);

INSERT INTO hrm.db_hrm.`payroll`
SELECT * FROM default_catalog.default_database.`payroll_src`;

--------------------------------------------------
