-- Temp table LIKE scenario (approximation of AUTO LIST PARTITION source)
-- Assumptions:
--  - Database tdb_temp_test exists; create if missing
--  - Backend replication_num=1 is acceptable for test

SET NAMES utf8;
CREATE DATABASE IF NOT EXISTS tdb_temp_test;
USE tdb_temp_test;

-- Base partitioned source table (AUTO LIST PARTITION)
DROP TABLE IF EXISTS contacts_src_like;
CREATE TABLE contacts_src_like (
  id INT,
  country VARCHAR(16)
)
ENGINE=OLAP
DUPLICATE KEY(id)
AUTO PARTITION BY LIST (country)
(
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- Create TEMP table LIKE source (should resolve display name correctly)
DROP TABLE IF EXISTS Contacts_Test_GKL;
CREATE TEMPORARY TABLE Contacts_Test_GKL LIKE contacts_src_like;

-- Insert and validate counts
INSERT INTO Contacts_Test_GKL VALUES (1, 'US');
SELECT 'Scenario1_after_first_insert' AS tag, COUNT(*) AS cnt FROM Contacts_Test_GKL;

INSERT INTO Contacts_Test_GKL VALUES (2, 'CN');
SELECT 'Scenario1_after_second_insert' AS tag, COUNT(*) AS cnt FROM Contacts_Test_GKL;

-- Cleanup
DROP TABLE IF EXISTS Contacts_Test_GKL;
DROP TABLE IF EXISTS contacts_src_like;