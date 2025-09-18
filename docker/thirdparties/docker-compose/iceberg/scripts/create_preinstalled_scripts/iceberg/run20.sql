create database if not exists iceberg_procedure_db;

use iceberg_procedure_db;

SET TIME ZONE '+08:00';
-- Fast Forward Snapshot
CREATE TABLE iceberg_procedure_db.test_fast_forward (
    id BIGINT,
    name STRING,
    value INT
) USING ICEBERG;

INSERT INTO iceberg_procedure_db.test_fast_forward VALUES
    (1, 'record1', 100);
ALTER TABLE iceberg_procedure_db.test_fast_forward
CREATE BRANCH feature_branch;

INSERT INTO iceberg_procedure_db.test_fast_forward VALUES
    (2, 'record2', 200);
ALTER TABLE iceberg_procedure_db.test_fast_forward
CREATE TAG feature_tag;

INSERT INTO iceberg_procedure_db.test_fast_forward VALUES
    (3, 'record3', 300);

-- Cherrypick Snapshot
CREATE TABLE iceberg_procedure_db.test_cherrypick (
    id BIGINT,
    data STRING,
    status INT
) USING ICEBERG;

-- First INSERT operation - generates initial snapshot
-- Establishes baseline table state for cherry-pick operations
INSERT INTO iceberg_procedure_db.test_cherrypick VALUES
    (1, 'data1', 1);
-- Second INSERT operation - creates incremental snapshot
-- Represents intermediate state for selective recovery scenarios
INSERT INTO iceberg_procedure_db.test_cherrypick VALUES
    (2, 'data2', 2);

-- Third INSERT operation - establishes final snapshot
-- Target state for demonstrating non-linear snapshot application
INSERT INTO iceberg_procedure_db.test_cherrypick VALUES
    (3, 'data3', 3);

-- Rollback Snapshot
CREATE TABLE iceberg_procedure_db.test_rollback (
    id BIGINT,
    version STRING,
    timestamp TIMESTAMP
) USING ICEBERG;

INSERT INTO iceberg_procedure_db.test_rollback VALUES
    (1, 'v1.0', TIMESTAMP '2024-01-01 10:00:00');
INSERT INTO iceberg_procedure_db.test_rollback VALUES
    (2, 'v1.1', TIMESTAMP '2024-01-02 11:00:00');
INSERT INTO iceberg_procedure_db.test_rollback VALUES
    (3, 'v1.2', TIMESTAMP '2024-01-03 12:00:00');

-- Set Current Snapshot
CREATE TABLE iceberg_procedure_db.test_current_snapshot (
    id BIGINT,
    content STRING
) USING ICEBERG;

INSERT INTO iceberg_procedure_db.test_current_snapshot VALUES
    (1, 'content1');
INSERT INTO iceberg_procedure_db.test_current_snapshot VALUES
    (2, 'content2');
ALTER TABLE iceberg_procedure_db.test_current_snapshot CREATE BRANCH `dev_branch`;
INSERT INTO iceberg_procedure_db.test_current_snapshot VALUES
    (3, 'content3');
ALTER TABLE iceberg_procedure_db.test_current_snapshot CREATE TAG `dev_tag`;
INSERT INTO iceberg_procedure_db.test_current_snapshot VALUES
    (4, 'content4');





