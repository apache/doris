CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

create table fixed_char_table (
  i int,
  c char(2)
) stored as orc;

insert into fixed_char_table values(1,'a'),(2,'b '), (3,'cd');

create table type_changed_table (
  id int,
  name string 
) stored as orc;
insert into type_changed_table values (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
ALTER TABLE type_changed_table CHANGE COLUMN id id STRING;

CREATE TABLE table_a (
    id INT,
    age INT
) STORED AS ORC;

INSERT INTO table_a VALUES
(1, null),
(2, 18),
(3, null),
(4, 25);

CREATE TABLE table_b (
    id INT,
    age INT
) STORED AS ORC;

INSERT INTO table_b VALUES
(1, null),
(2, null),
(3, 1000000),
(4, 100);
