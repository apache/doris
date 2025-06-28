use paimon;

create database if not exists test_paimon_schema_change;

use test_paimon_schema_change;

CREATE TABLE sc_orc_pk (
    id INT,
    name STRING,
    age INT
) USING paimon
TBLPROPERTIES ('primary-key' = 'id', "file.format" = "orc",'deletion-vectors.enabled' = 'true');

INSERT INTO sc_orc_pk (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25);
INSERT INTO sc_orc_pk (id, name, age) VALUES (3, 'Charlie', 28);
ALTER TABLE sc_orc_pk ADD COLUMNS (city STRING);
INSERT INTO sc_orc_pk (id, name, age, city) VALUES (4, 'Charlie', 28, 'New York');
INSERT INTO sc_orc_pk (id, name, age, city) VALUES (5, 'David', 32, 'Los Angeles');
ALTER TABLE sc_orc_pk RENAME COLUMN name TO full_name;
INSERT INTO sc_orc_pk (id, full_name, age, city) VALUES (6, 'David', 35, 'Los Angeles');
INSERT INTO sc_orc_pk (id, full_name, age, city) VALUES (7, 'Eve', 27, 'San Francisco');
ALTER TABLE sc_orc_pk DROP COLUMN age;
INSERT INTO sc_orc_pk (id, full_name, city) VALUES (8, 'Eve', 'San Francisco');
INSERT INTO sc_orc_pk (id, full_name, city) VALUES (9, 'Frank', 'Chicago');
ALTER TABLE sc_orc_pk CHANGE COLUMN id id BIGINT;
INSERT INTO sc_orc_pk (id, full_name, city) VALUES (10000000000, 'Frank', 'Chicago');
INSERT INTO sc_orc_pk (id, full_name, city) VALUES (10, 'Grace', 'Seattle');

ALTER TABLE sc_orc_pk ADD COLUMN salary DECIMAL(10,2) FIRST;
INSERT INTO sc_orc_pk (id, full_name, city, salary) VALUES (11, 'Grace', 'Seattle', 5000.00);
INSERT INTO sc_orc_pk (id, full_name, city, salary) VALUES (12, 'Heidi', 'Boston', 6000.00);

ALTER TABLE sc_orc_pk RENAME COLUMN city TO location;
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (13, 'Heidi', 'Boston', 6000.00);
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (14, 'Ivan', 'Miami', 7000.00);

ALTER TABLE sc_orc_pk CHANGE COLUMN salary salary DECIMAL(12,2);
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (15, 'Ivan', 'Miami', 7000.00);
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (16, 'Judy', 'Denver', 8000.00);

ALTER TABLE sc_orc_pk ALTER COLUMN salary AFTER location;
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (17, 'Stm', 'ttttt', 8000.00);
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (18, 'Ken', 'Austin', 9000.00);

ALTER TABLE sc_orc_pk ALTER COLUMN full_name FIRST;
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (19, 'AAAA', 'BBBB', 9000.00);
INSERT INTO sc_orc_pk (id, full_name, location, salary) VALUES (20, 'Laura', 'Portland', 10000.00);





CREATE TABLE sc_parquet_pk (
    id INT,
    name STRING,
    age INT
) USING paimon
TBLPROPERTIES ('primary-key' = 'id',"file.format" = "parquet",'deletion-vectors.enabled' = 'true');

INSERT INTO sc_parquet_pk (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25);
INSERT INTO sc_parquet_pk (id, name, age) VALUES (3, 'Charlie', 28);

ALTER TABLE sc_parquet_pk ADD COLUMNS (city STRING);
INSERT INTO sc_parquet_pk (id, name, age, city) VALUES (3, 'Charlie', 28, 'New York');
INSERT INTO sc_parquet_pk (id, name, age, city) VALUES (4, 'David', 32, 'Los Angeles');

ALTER TABLE sc_parquet_pk RENAME COLUMN name TO full_name;
INSERT INTO sc_parquet_pk (id, full_name, age, city) VALUES (4, 'David', 35, 'Los Angeles');
INSERT INTO sc_parquet_pk (id, full_name, age, city) VALUES (5, 'Eve', 27, 'San Francisco');

ALTER TABLE sc_parquet_pk DROP COLUMN age;
INSERT INTO sc_parquet_pk (id, full_name, city) VALUES (5, 'Eve', 'San Francisco');
INSERT INTO sc_parquet_pk (id, full_name, city) VALUES (6, 'Frank', 'Chicago');

ALTER TABLE sc_parquet_pk CHANGE COLUMN id id BIGINT;
INSERT INTO sc_parquet_pk (id, full_name, city) VALUES (10000000000, 'Frank', 'Chicago');
INSERT INTO sc_parquet_pk (id, full_name, city) VALUES (7, 'Grace', 'Seattle');

ALTER TABLE sc_parquet_pk ADD COLUMN salary DECIMAL(10,2) FIRST;
INSERT INTO sc_parquet_pk (id, full_name, city, salary) VALUES (6, 'Grace', 'Seattle', 5000.00);
INSERT INTO sc_parquet_pk (id, full_name, city, salary) VALUES (8, 'Heidi', 'Boston', 6000.00);

ALTER TABLE sc_parquet_pk RENAME COLUMN city TO location;
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (7, 'Heidi', 'Boston', 6000.00);
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (9, 'Ivan', 'Miami', 7000.00);

ALTER TABLE sc_parquet_pk CHANGE COLUMN salary salary DECIMAL(12,2);
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (8, 'Ivan', 'Miami', 7000.00);
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (10, 'Judy', 'Denver', 8000.00);

ALTER TABLE sc_parquet_pk ALTER COLUMN salary AFTER location;
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (9, 'Stm', 'ttttt', 8000.00);
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (11, 'Ken', 'Austin', 9000.00);

ALTER TABLE sc_parquet_pk ALTER COLUMN full_name FIRST;
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (10, 'AAAA', 'BBBB', 9000.00);
INSERT INTO sc_parquet_pk (id, full_name, location, salary) VALUES (12, 'Laura', 'Portland', 10000.00);








create table sc_parquet (
    k int,
    vVV string,
    col1 array<int>,
    col2 struct<a:int,b:string>,
    col3 map<string,int>
) tblproperties (
    "file.format" = "parquet"
);
INSERT INTO sc_parquet (k,vVV,col1,col2,col3) VALUES 
    (1, 'hello', array(1,2,3), named_struct('a', 10, 'b', 'world'), map('key1', 100, 'key2', 200));

ALTER TABLE sc_parquet RENAME COLUMN col1 TO new_col1;
ALTER TABLE sc_parquet RENAME COLUMN col2 TO new_col2;
ALTER TABLE sc_parquet RENAME COLUMN col3 TO new_col3;
ALTER TABLE sc_parquet RENAME COLUMN vVV to  vv;
alter table sc_parquet  ALTER COLUMN new_col2 AFTER new_col3;
alter table sc_parquet  ALTER COLUMN new_col1 AFTER new_col2;

INSERT INTO sc_parquet  (k,vv,new_col1,new_col2,new_col3) VALUES 
    (2, 'test', array(4,5,6), named_struct('a', 20, 'b', 'spark'), map('key3', 300)),
    (3, 'example', array(7,8,9), named_struct('a', 30, 'b', 'hive'), map('key4', 400, 'key5', 500));







create table sc_orc (
    k int,
    vVV string,
    col1 array<int>,
    col2 struct<a:int,b:string>,
    col3 map<string,int>
) tblproperties (
    "file.format" = "orc"
);

INSERT INTO sc_orc (k,vVV,col1,col2,col3) VALUES 
    (1, 'hello', array(1,2,3), named_struct('a', 10, 'b', 'world'), map('key1', 100, 'key2', 200));

ALTER TABLE sc_orc RENAME COLUMN col1 TO new_col1;
ALTER TABLE sc_orc RENAME COLUMN col2 TO new_col2;
ALTER TABLE sc_orc RENAME COLUMN col3 TO new_col3;
ALTER TABLE sc_orc RENAME COLUMN vVV to  vv;
alter table sc_orc  ALTER COLUMN new_col2 AFTER new_col3;
alter table sc_orc  ALTER COLUMN new_col1 AFTER new_col2;

INSERT INTO sc_orc  (k,vv,new_col1,new_col2,new_col3) VALUES 
    (2, 'test', array(4,5,6), named_struct('a', 20, 'b', 'spark'), map('key3', 300)),
    (3, 'example', array(7,8,9), named_struct('a', 30, 'b', 'hive'), map('key4', 400, 'key5', 500));


