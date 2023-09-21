-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

ALTER SYSTEM ENABLE RESTRICTED SESSION;
ALTER DATABASE CHARACTER SET  UNICODE;
ALTER SYSTEM DISABLE RESTRICTED SESSION;
create table doris_test.student (
id number(5),
name varchar2(20),
age number(2),
score number(3,1)
);

create table doris_test.test_num (
id int,
n1 number,
n2 number(38),
n3 number(9,2),
n4 int,
n5 smallint,
n6 decimal(5,2),
n7 float,
n8 float(2),
n9 real
);

create table doris_test.test_int (
id int,
tinyint_value1 number(2,0),
smallint_value1 number(4,0),
int_value1 number(9,0),
bigint_value1 number(18,0),
tinyint_value2 number(3,0),
smallint_value2 number(5,0),
int_value2 number(10,0),
bigint_value2 number(19,0)
);

create table doris_test.test_char (
id int,
country char,
city nchar(6),
address varchar2(4000),
name nvarchar2(6),
remark long
);

create table doris_test.test_raw (
id int,
raw_value raw(20),
long_raw_value long raw
);

create table doris_test.test_date (
id int,
t1 date,
t2 interval year(3) to month,
t3 interval day(3) to second(6)
);

create table doris_test.test_timestamp(
id int,
t1 date,
t2 timestamp(3),
t3 timestamp(6),
t4 timestamp(9),
t5 timestamp,
t6 interval year(3) to month,
t7 interval day(3) to second(6)
);

create table doris_test.test_insert(
id varchar2(128),
name varchar2(128),
age number(5)
);

create table doris_test.test_number(
    id number(11) not null primary key,
    num1 NUMBER(5,2),
    num2 NUMBER(5, -2),
    num4 NUMBER(5,7)
);

create table doris_test.test_number2(
    id number(11) not null primary key,
    num1 NUMBER(38, -5)
);

create table doris_test.test_number3 (
  id number(11) not null primary key,
  num1 NUMBER(38, -84)
);

create table doris_test.test_number4 (
  id number(11) not null primary key,
  num1 NUMBER(5,-7)
);

create table doris_test.test_clob (
  id number(11) not null primary key,
  num1 clob
);

create table doris_test."AA/D" (
     id number(5),
     name varchar2(20),
     age number(2),
     score number(3,1)
);

create table doris_test.aaad (
     id number(5),
     name varchar2(20)
);


CREATE TABLE doris_test.test_numerical_type (
    real_01 REAL,
    real_02 REAL,
    real_03 REAL,
    real_04 REAL,
    real_05 REAL,
    double_01 DOUBLE PRECISION,
    double_02 DOUBLE PRECISION,
    double_03 DOUBLE PRECISION,
    double_04 DOUBLE PRECISION,
    double_05 DOUBLE PRECISION,
	double_06 DOUBLE PRECISION,
    float_01 FLOAT,
    float_02 FLOAT,
    float_03 FLOAT,
    float_04 FLOAT,
    float_05 FLOAT(126),
    float_06 FLOAT(126),
    float_07 FLOAT(126),
    float_08 FLOAT(126),
    float_09 FLOAT(1),
    float_10 FLOAT(7)
);


CREATE TABLE doris_test.test_vachar_type (
    varchar_01   varchar(10),
    varchar_02   varchar(20),
    varchar_03   varchar(1000),
    varchar_04   varchar(5),
    varchar2_01 varchar2(5 char),
    varchar2_02 varchar2(10 char),
    varchar2_03 varchar2(20 char),
    varchar2_04 varchar2(1000 char),
    varchar2_05 varchar2(5 byte),
    varchar2_06 varchar2(10 byte),
    varchar2_07 varchar2(20 byte),
    varchar2_08 varchar2(4000 byte),
    nvarchar2_01 nvarchar2(5),
    nvarchar2_02 nvarchar2(10),
    nvarchar2_03 nvarchar2(20),
    nvarchar2_04 nvarchar2(2000)
);

CREATE TABLE doris_test.tcc_and_emoji (
    varchar2_01    varchar2(150 char),
    varchar2_02    varchar2(150 char),
    varchar2_03    varchar2(4000 char),
    varchar2_04    varchar2(150 char),
    varchar2_05    varchar2(150 char),
    varchar2_2_01    nvarchar2(50),
    varchar2_2_02    nvarchar2(50),
    varchar2_2_03    nvarchar2(2000),
    varchar2_2_04    nvarchar2(150),
    varchar2_2_05    nvarchar2(150),
    varchar_3_01    varchar(1001),
    varchar_3_02    varchar(1001),
    char_01     char(501),
    char_02     char(501),
    char_2_01 char(150),
    char_2_02 char(150),
    char_2_03 char(500),
    char_2_04 char(150),
    char_2_05 char(150),
    char_c2_01 char(50 char),
    char_c2_02  char(50 char),
    char_c2_03  char(2000 char),
    char_c2_04  char(150 char),
    char_c2_05  char(150 char),
    char_b2_01 char(150 byte),
    char_b2_02 char(150 byte),
    char_b2_03 char(2000 byte),
    char_b2_04 char(150 byte),
    char_b2_05 char(150 byte),
    char_n2_01 nchar(50),
    char_n2_02 nchar(150),
    char_n2_03 nchar(1000),
    char_n2_04 nchar(50),
    char_n2_05 nchar(70)
);


CREATE TABLE doris_test.test_nvachar_2_type (
    varchar_3_03    varchar(1001),
    varchar_3_04    varchar(1001),
    char_03     char(501),
    char_04     char(501)
);


CREATE TABLE doris_test.test_clob_type (
    clob_03 clob,
    clob_04 clob,
    clob_05 clob,
    nclob_03 nclob,
    nclob_04 nclob,
    nclob_05 nclob
);


CREATE TABLE doris_test.test_char_type_01 (
    char_01 char(10),
    char_02 char(20),
    char_03 char(500),
    char_04 char(5),
    char_c_01 char(5 char),
    char_c_02 char(10 char),
    char_c_03 char(20 char),
    char_c_04 char(500 char),
    char_b_01 char(5 byte),
    char_b_02 char(10 byte),
    char_b_03 char(20 byte),
    char_b_04 char(500 byte)
);


CREATE TABLE doris_test.test_char_type_02 (
    char_n_01 nchar(5),
    char_n_02 nchar(10),
    char_n_03 nchar(20),
    char_n_04 nchar(1000)
);



CREATE TABLE doris_test.test_decimal_type_01 (
    decimal_01 decimal(3, 0),
    decimal_02 decimal(3, 0),
    decimal_03 decimal(3, 0),
    decimal_04 decimal(3, 1),
    decimal_05 decimal(3, 1),
    decimal_06 decimal(3, 1),
    decimal_07 decimal(4, 2),
    decimal_08 decimal(4, 2)
);

CREATE TABLE doris_test.test_decimal_type_02 (
    decimal_09 decimal(24, 2),
    decimal_10 decimal(24, 2),
    decimal_11 decimal(24, 2),
    decimal_12 decimal(24, 4),
    decimal_13 decimal(30, 5),
    decimal_14 decimal(30, 5)
);

CREATE TABLE doris_test.test_decimal_2_type (
    decimal_01 decimal(3, 0),
    decimal_02 decimal(3, 0),
    decimal_03 decimal(3, 0),
    decimal_04 decimal(3, 1),
    decimal_05 decimal(3, 1),
    decimal_06 decimal(3, 1),
    decimal_07 decimal(4, 2),
    decimal_08 decimal(4, 2),
    decimal_09 decimal(24, 2),
    decimal_10 decimal(24, 2),
    decimal_11 decimal(24, 2),
    decimal_12 decimal(24, 4),
    decimal_13 decimal(30, 5),
    decimal_14 decimal(30, 5),
    decimal_15 decimal(38, 0),
    decimal_16 decimal(38, 0),
    decimal_17 decimal(30, 38),
    decimal_18 decimal(30, 38),
    decimal_19 decimal(10, 3)
);



CREATE TABLE doris_test.test_number_type (
    number_01 number(1),
    number_02 number(2),
    number_03 number(38),
    number_04 number(38),
    number_05 number,
    number_06 number,
    number_07 number,
    number_08 number,
    number_09 number(37, -1),
    number_10 number(37, -1),
    number_11 number(37, -1),
    number_12 number(37, -1),
    number_13 number(1, -1),
    number_14 number(1, -1),
    number_15 number(2, -4),
    number_16 number(2, -4),
    number_17 number(8, -3),
    number_18 number(8, -3),
    number_19 number(14, -14),
    number_20 number(14, -14),
    number_21 number(5, -33),
    number_22 number(5, -33),
    number_23 number(1, -37),
    number_24 number(1, -37),
    number_25 number(38,40),
    number_26 number(18,40),
    number_27 number(38,80),
    number_28 number(38,-60),
    number_29 number(18,60),
    number_30 number
);



CREATE TABLE doris_test.test_blob_type(
    blob_01 blob,
    blob_02 blob,
    blob_03 blob,
    blob_04 blob,
    blob_05 blob,
    blob_06 blob,
    blob_07 blob
);



CREATE TABLE doris_test.test_raw_type(
    raw_01 raw(2000),
    raw_02 raw(2000),
    raw_03 raw(2000),
    raw_04 raw(2000),
    raw_05 raw(2000),
    raw_06 raw(2000)
);


CREATE TABLE doris_test.test_date_type(
    date_01 DATE,
    date_02 DATE,
    date_03 DATE,
    date_04 DATE,
    date_05 DATE,
    date_06 DATE,
    date_07 DATE,
    date_08 DATE,
    date_09 DATE,
    date_10 DATE,
    date_11 DATE,
    date_12 DATE,
    date_13 DATE
);

CREATE TABLE doris_test.test_timestamp_type(
    timestamp_01 timestamp,
    timestamp_02 timestamp,
    timestamp_03 timestamp,
    timestamp_04 timestamp,
    timestamp_05 timestamp,
    timestamp_06 timestamp,
    timestamp_07 timestamp,
    timestamp_08 timestamp,
    timestamp_09 timestamp,
    timestamp_10 timestamp
);

CREATE TABLE doris_test_02.nation (
  n_nationkey NUMBER(11) NOT NULL,
  n_name varchar(25) NOT NULL,
  n_regionkey NUMBER(11) NOT NULL,
  n_comment varchar(152) NOT NULL
);

CREATE  VIEW  nation_view_02 AS
SELECT n_nationkey,  n_name,n_regionkey,n_comment
FROM DORIS_TEST_02.NATION;

CREATE TABLE doris_test.nation_load (
  n_nationkey NUMBER(11) NOT NULL,
  n_name varchar(25) NOT NULL,
  n_regionkey NUMBER(11) NOT NULL,
  n_comment varchar(152) NOT NULL
);


CREATE TABLE doris_test.nation_read_and_write (
  n_nationkey NUMBER(11) NOT NULL,
  n_name varchar(256) NOT NULL,
  n_regionkey NUMBER(11) NOT NULL,
  n_comment varchar(152) NOT NULL
);


CREATE  VIEW  doris_test.nation_read_and_write_view AS
SELECT n_nationkey,n_name,n_regionkey,n_comment
FROM DORIS_TEST.NATION_READ_AND_WRITE;

create table doris_test.student2 (
id number(5),
name varchar2(20),
age number(2),
score number(3,1)
);

