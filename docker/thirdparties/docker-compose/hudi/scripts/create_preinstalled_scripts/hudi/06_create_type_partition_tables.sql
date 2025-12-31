-- Create partition tables with different partition column types
use regression_hudi;

DROP TABLE IF EXISTS one_partition_tb;
CREATE TABLE one_partition_tb (
    id INT,
    name string
)
USING HUDI
PARTITIONED BY (part1 INT);
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (1, 'Alice');
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (2, 'Bob');
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (3, 'Charlie');
INSERT INTO one_partition_tb PARTITION (part1=2025) VALUES (4, 'David');
INSERT INTO one_partition_tb PARTITION (part1=2025) VALUES (5, 'Eva');

DROP TABLE IF EXISTS two_partition_tb;
CREATE TABLE two_partition_tb (
    id INT,
    name string
)
USING HUDI
PARTITIONED BY (part1 STRING, part2 int);
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (1, 'Alice');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (2, 'Bob');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (3, 'Charlie');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=2) VALUES (4, 'David');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=2) VALUES (5, 'Eva');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=1) VALUES (6, 'Frank');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=1) VALUES (7, 'Grace');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (8, 'Hannah');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (9, 'Ivy');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (10, 'Jack');

DROP TABLE IF EXISTS three_partition_tb;
CREATE TABLE three_partition_tb (
    id INT,
    name string
)
USING HUDI
PARTITIONED BY (part1 STRING, part2 INT, part3 STRING);
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (1, 'Alice');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (2, 'Bob');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (3, 'Charlie');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q2') VALUES (4, 'David');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q2') VALUES (5, 'Eva');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2025, part3='Q1') VALUES (6, 'Frank');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2025, part3='Q2') VALUES (7, 'Grace');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2024, part3='Q1') VALUES (8, 'Hannah');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2024, part3='Q1') VALUES (9, 'Ivy');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q2') VALUES (10, 'Jack');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q2') VALUES (11, 'Leo');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q3') VALUES (12, 'Mia');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q1') VALUES (13, 'Nina');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q2') VALUES (14, 'Oscar');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q3') VALUES (15, 'Paul');

-- partition pruning with different data types
-- boolean
DROP TABLE IF EXISTS boolean_partition_tb;
CREATE TABLE boolean_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 BOOLEAN);

INSERT INTO boolean_partition_tb PARTITION (part1=true) VALUES (1, 'Alice');
INSERT INTO boolean_partition_tb PARTITION (part1=true) VALUES (2, 'Bob');
INSERT INTO boolean_partition_tb PARTITION (part1=false) VALUES (3, 'Charlie');
INSERT INTO boolean_partition_tb PARTITION (part1=false) VALUES (4, 'David');

-- tinyint
DROP TABLE IF EXISTS tinyint_partition_tb;
CREATE TABLE tinyint_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 TINYINT);

INSERT INTO tinyint_partition_tb PARTITION (part1=1) VALUES (1, 'Alice');
INSERT INTO tinyint_partition_tb PARTITION (part1=1) VALUES (2, 'Bob');
INSERT INTO tinyint_partition_tb PARTITION (part1=2) VALUES (3, 'Charlie');
INSERT INTO tinyint_partition_tb PARTITION (part1=2) VALUES (4, 'David');

-- smallint
DROP TABLE IF EXISTS smallint_partition_tb;
CREATE TABLE smallint_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 SMALLINT);

INSERT INTO smallint_partition_tb PARTITION (part1=10) VALUES (1, 'Alice');
INSERT INTO smallint_partition_tb PARTITION (part1=10) VALUES (2, 'Bob');
INSERT INTO smallint_partition_tb PARTITION (part1=20) VALUES (3, 'Charlie');
INSERT INTO smallint_partition_tb PARTITION (part1=20) VALUES (4, 'David');

-- int
DROP TABLE IF EXISTS int_partition_tb;
CREATE TABLE int_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 INT);

INSERT INTO int_partition_tb PARTITION (part1=100) VALUES (1, 'Alice');
INSERT INTO int_partition_tb PARTITION (part1=100) VALUES (2, 'Bob');
INSERT INTO int_partition_tb PARTITION (part1=200) VALUES (3, 'Charlie');
INSERT INTO int_partition_tb PARTITION (part1=200) VALUES (4, 'David');

-- bigint
DROP TABLE IF EXISTS bigint_partition_tb;
CREATE TABLE bigint_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 BIGINT);

INSERT INTO bigint_partition_tb PARTITION (part1=1234567890) VALUES (1, 'Alice');
INSERT INTO bigint_partition_tb PARTITION (part1=1234567890) VALUES (2, 'Bob');
INSERT INTO bigint_partition_tb PARTITION (part1=9876543210) VALUES (3, 'Charlie');
INSERT INTO bigint_partition_tb PARTITION (part1=9876543210) VALUES (4, 'David');

-- string
DROP TABLE IF EXISTS string_partition_tb;
CREATE TABLE string_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 STRING);

INSERT INTO string_partition_tb PARTITION (part1='RegionA') VALUES (1, 'Alice');
INSERT INTO string_partition_tb PARTITION (part1='RegionA') VALUES (2, 'Bob');
INSERT INTO string_partition_tb PARTITION (part1='RegionB') VALUES (3, 'Charlie');
INSERT INTO string_partition_tb PARTITION (part1='RegionB') VALUES (4, 'David');

-- date
DROP TABLE IF EXISTS date_partition_tb;
CREATE TABLE date_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 DATE);

INSERT INTO date_partition_tb PARTITION (part1=DATE '2023-12-01') VALUES (1, 'Alice');
INSERT INTO date_partition_tb PARTITION (part1=DATE '2023-12-01') VALUES (2, 'Bob');
INSERT INTO date_partition_tb PARTITION (part1=DATE '2024-01-01') VALUES (3, 'Charlie');
INSERT INTO date_partition_tb PARTITION (part1=DATE '2024-01-01') VALUES (4, 'David');

-- timestamp
DROP TABLE IF EXISTS timestamp_partition_tb;
CREATE TABLE timestamp_partition_tb (
    id INT,
    name STRING
)
USING HUDI
PARTITIONED BY (part1 TIMESTAMP);

INSERT INTO timestamp_partition_tb PARTITION (part1=TIMESTAMP '2023-12-01 08:00:00') VALUES (1, 'Alice');
INSERT INTO timestamp_partition_tb PARTITION (part1=TIMESTAMP '2023-12-01 08:00:00') VALUES (2, 'Bob');
INSERT INTO timestamp_partition_tb PARTITION (part1=TIMESTAMP '2024-01-01 10:00:00') VALUES (3, 'Charlie');
INSERT INTO timestamp_partition_tb PARTITION (part1=TIMESTAMP '2024-01-01 10:00:00') VALUES (4, 'David');