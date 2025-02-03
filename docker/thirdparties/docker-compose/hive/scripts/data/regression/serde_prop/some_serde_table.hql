create database if not exists regression;
use regression;

CREATE TABLE `serde_test1`(
  `id` int,
  `name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='',
  'serialization.format'='')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE `serde_test2`(
  `id` int, 
  `name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='', 
  'serialization.format'='') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
   'field.delim'='|'
);

CREATE TABLE `serde_test3`(
  `id` int, 
  `name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (  
  'serialization.format'='g') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE `serde_test4`(
  `id` int, 
  `name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'field.delim' = 'gg',
  "line.delim" = "hh")
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE `serde_test5`(
  `id` int, 
  `name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'field.delim' = '16',
  "line.delim" = "21")
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE `serde_test6`(
  `id` int, 
  `name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'field.delim' = '\16',
  "line.delim" = "\21")
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE `serde_test7`(
  `id` int, 
  `name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'field.delim' = 'a',
  'escape.delim' = '|',
  'serialization.null.format' = 'null'
)
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE `serde_test8` like `serde_test7`;

insert into serde_test1 values(1, "abc"),(2, "def");
insert into serde_test2 values(1, "abc"),(2, "def");
insert into serde_test3 values(1, "abc"),(2, "def");
insert into serde_test4 values(1, "abc"),(2, "def");
insert into serde_test5 values(1, "abc"),(2, "def");
insert into serde_test6 values(1, "abc"),(2, "def");
insert into serde_test7 values(1, null),(2, "|||"),(3, "aaa"),(4, "\"null\"");

CREATE TABLE test_open_csv_default_prop (
    id INT,
    name STRING,
    age INT,
    salary DOUBLE,
    is_active BOOLEAN,
    hire_date DATE,
    last_login TIMESTAMP,
    rating FLOAT,
    description STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;

CREATE TABLE test_open_csv_standard_prop (
    id INT,
    name STRING,
    age INT,
    salary DOUBLE,
    is_active BOOLEAN,
    hire_date DATE,
    last_login TIMESTAMP,
    rating FLOAT,
    description STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE;

CREATE TABLE test_open_csv_custom_prop (
    id INT,
    name STRING,
    age INT,
    salary DOUBLE,
    is_active BOOLEAN,
    hire_date DATE,
    last_login TIMESTAMP,
    rating FLOAT,
    description STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\t",
    "quoteChar"     = "\'",
    "escapeChar"    = "|"
)
STORED AS TEXTFILE;

INSERT INTO TABLE test_open_csv_default_prop VALUES 
(1, 'John Doe', 28, 50000.75, true, '2022-01-15', '2023-10-21 14:30:00', 4.5, 'Senior Developer'),
(2, 'Jane,Smith', NULL, NULL, false, '2020-05-20', NULL, NULL, '\"Project Manager\"');

INSERT INTO TABLE test_open_csv_standard_prop VALUES 
(1, 'John Doe', 28, 50000.75, true, '2022-01-15', '2023-10-21 14:30:00', 4.5, 'Senior Developer'),
(2, 'Jane,Smith', NULL, NULL, false, '2020-05-20', NULL, NULL, '\"Project Manager\"');

INSERT INTO TABLE test_open_csv_custom_prop VALUES 
(1, 'John Doe', 28, 50000.75, true, '2022-01-15', '2023-10-21 14:30:00', 4.5, 'Senior Developer'),
(2, 'Jane,Smith', NULL, NULL, false, '2020-05-20', NULL, NULL, '\"Project Manager\"');
