-- Temp table INSERT OVERWRITE scenario
-- Assumptions:
--  - Database tdb_temp_test exists; create if missing
--  - Backend replication_num=1 is acceptable for test

SET NAMES utf8;
CREATE DATABASE IF NOT EXISTS tdb_temp_test;
USE tdb_temp_test;

DROP TABLE IF EXISTS test123_emp;
CREATE TEMPORARY TABLE test123_emp PROPERTIES('replication_num' = '1') AS SELECT 1 AS id;
SELECT 'Scenario2_after_create_as' AS tag, COUNT(*) AS cnt FROM test123_emp;

INSERT INTO test123_emp VALUES (2);
SELECT 'Scenario2_after_insert' AS tag, COUNT(*) AS cnt FROM test123_emp;

INSERT OVERWRITE TABLE test123_emp VALUES (3);
SELECT 'Scenario2_after_overwrite' AS tag, COUNT(*) AS cnt FROM test123_emp;

-- Cleanup
DROP TABLE IF EXISTS test123_emp;