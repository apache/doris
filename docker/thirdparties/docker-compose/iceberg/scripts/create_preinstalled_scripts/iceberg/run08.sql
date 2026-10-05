create database if not exists demo.test_db;
use demo.test_db;
CREATE TABLE no_partition (
    id INT,
    name STRING,
    create_date DATE
) USING iceberg;
INSERT INTO no_partition VALUES(1, 'Alice', DATE '2023-12-01'),(2, 'Bob', DATE '2023-12-02');

CREATE TABLE not_support_trans (
    id INT,
    name STRING,
    create_date DATE
) USING iceberg
PARTITIONED BY (bucket(10, create_date));
INSERT INTO not_support_trans VALUES(1, 'Alice', DATE '2023-12-01'),(2, 'Bob', DATE '2023-12-02');

CREATE TABLE add_partition1 (
    id INT,
    name STRING,
    create_date DATE
) USING iceberg;
INSERT INTO add_partition1 VALUES(1, 'Alice', DATE '2023-12-01'),(2, 'Bob', DATE '2023-12-02');
ALTER TABLE add_partition1 ADD PARTITION FIELD month(create_date);
INSERT INTO add_partition1 VALUES(3, 'Lara', DATE '2023-12-03');

CREATE TABLE add_partition2 (
    id INT,
    name STRING,
    create_date1 DATE,
    create_date2 DATE
) USING iceberg
PARTITIONED BY (month(create_date1));
INSERT INTO add_partition2 VALUES(1, 'Alice', DATE '2023-12-01', DATE '2023-12-01'),(2, 'Bob', DATE '2023-12-02', DATE '2023-12-02');
ALTER TABLE add_partition2 ADD PARTITION FIELD year(create_date2);
INSERT INTO add_partition2 VALUES(3, 'Lara', DATE '2023-12-03', DATE '2023-12-03');

CREATE TABLE drop_partition1 (
    id INT,
    name STRING,
    create_date DATE
) USING iceberg
PARTITIONED BY (month(create_date));
INSERT INTO drop_partition1 VALUES(1, 'Alice', DATE '2023-12-01'),(2, 'Bob', DATE '2023-12-02');
ALTER TABLE drop_partition1 DROP PARTITION FIELD month(create_date);

CREATE TABLE drop_partition2 (
    id INT,
    name STRING,
    create_date1 DATE,
    create_date2 DATE
) USING iceberg
PARTITIONED BY (month(create_date1), year(create_date2));
INSERT INTO drop_partition2 VALUES(1, 'Alice', DATE '2023-12-01', DATE '2023-12-01'),(2, 'Bob', DATE '2023-12-02', DATE '2023-12-02');
ALTER TABLE drop_partition2 DROP PARTITION FIELD year(create_date2);
INSERT INTO drop_partition2 VALUES(3, 'Lara', DATE '2023-12-03', DATE '2023-12-03');

CREATE TABLE replace_partition1 (
    id INT,
    name STRING,
    create_date1 DATE,
    create_date2 DATE
) USING iceberg
PARTITIONED BY (month(create_date1));
INSERT INTO replace_partition1 VALUES(1, 'Alice', DATE '2023-12-01', DATE '2023-12-01'),(2, 'Bob', DATE '2023-12-02', DATE '2023-12-02');
ALTER TABLE replace_partition1 REPLACE PARTITION FIELD month(create_date1) WITH year(create_date2);
INSERT INTO replace_partition1 VALUES(3, 'Lara', DATE '2023-12-03', DATE '2023-12-03');

CREATE TABLE replace_partition2(
  ts TIMESTAMP COMMENT 'ts',
  value INT COMMENT 'col1')
USING iceberg
PARTITIONED BY (month(ts));
insert into replace_partition2 values (to_timestamp('2024-10-26 11:02:03', 'yyyy-MM-dd HH:mm:ss'), 1), (to_timestamp('2024-11-27 21:02:03', 'yyyy-MM-dd HH:mm:ss'), 2);
ALTER TABLE replace_partition2 REPLACE PARTITION FIELD ts_month WITH day(ts);
insert into replace_partition2 values (to_timestamp('2024-12-03 14:02:03', 'yyyy-MM-dd HH:mm:ss'), 3);

CREATE TABLE replace_partition3(
  ts TIMESTAMP COMMENT 'ts',
  value INT COMMENT 'col1')
USING iceberg
PARTITIONED BY (month(ts));
insert into replace_partition3 values (to_timestamp('2024-11-26 11:02:03', 'yyyy-MM-dd HH:mm:ss'), 1); 
ALTER TABLE replace_partition3 REPLACE PARTITION FIELD month(ts) WITH day(ts);
insert into replace_partition3 values (to_timestamp('2024-11-02 21:02:03', 'yyyy-MM-dd HH:mm:ss'), 2), (to_timestamp('2024-11-03 11:02:03', 'yyyy-MM-dd HH:mm:ss'), 3), (to_timestamp('2024-12-02 19:02:03', 'yyyy-MM-dd HH:mm:ss'), 4);

CREATE TABLE replace_partition4(
  ts TIMESTAMP COMMENT 'ts',
  value INT COMMENT 'col1')
USING iceberg
PARTITIONED BY (month(ts));
insert into replace_partition4 values (to_timestamp('2024-10-26 11:02:03', 'yyyy-MM-dd HH:mm:ss'), 1), (to_timestamp('2024-11-26 21:02:03', 'yyyy-MM-dd HH:mm:ss'), 2);
ALTER TABLE replace_partition4 REPLACE PARTITION FIELD month(ts) WITH day(ts);
insert into replace_partition4 values (to_timestamp('2024-11-02 13:02:03', 'yyyy-MM-dd HH:mm:ss'), 3), (to_timestamp('2024-11-03 10:02:03', 'yyyy-MM-dd HH:mm:ss'), 4);

CREATE TABLE replace_partition5(
  ts TIMESTAMP COMMENT 'ts',
  value INT COMMENT 'col1')
USING iceberg
PARTITIONED BY (month(ts));
insert into replace_partition5 values (to_timestamp('2024-10-26 11:02:03', 'yyyy-MM-dd HH:mm:ss'), 1), (to_timestamp('2024-11-26 13:02:03', 'yyyy-MM-dd HH:mm:ss'), 2);
ALTER TABLE replace_partition5 REPLACE PARTITION FIELD month(ts) WITH day(ts);
insert into replace_partition5 values (to_timestamp('2024-10-12 09:02:03', 'yyyy-MM-dd HH:mm:ss'), 3), (to_timestamp('2024-12-21 21:02:03', 'yyyy-MM-dd HH:mm:ss'), 4);
ALTER TABLE replace_partition5 REPLACE PARTITION FIELD day(ts) WITH hour(ts);
insert into replace_partition5 values (to_timestamp('2024-12-21 11:02:03', 'yyyy-MM-dd HH:mm:ss'), 5);
insert into replace_partition5 values (to_timestamp('2025-01-01 11:02:03', 'yyyy-MM-dd HH:mm:ss'), 6);
