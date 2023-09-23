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

create table doris_test."student2" (
id number(5),
name varchar2(20),
age number(2),
score number(3,1)
);
