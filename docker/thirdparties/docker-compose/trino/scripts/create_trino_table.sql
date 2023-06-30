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

create schema hive.doris_test;
create table hive.doris_test.orc_basic_data_type
(
    T_BOOLEAN   BOOLEAN,
    T_TINYINT   TINYINT,
    T_SMALLINT  SMALLINT,
    T_INTEGER   INTEGER,
    T_BIGINT    BIGINT,
    T_REAL      REAL,
    T_DOUBLE    DOUBLE,
    T_DECIMAL   DECIMAL(38,12),
    T_CHAR      CHAR,
    T_VARCHAR   VARCHAR,
    T_DATE      DATE,
    T_TIMESTAMP TIMESTAMP
) WITH (format = 'ORC');

create table hive.doris_test.parquet_basic_data_type
(
    T_BOOLEAN   BOOLEAN,
    T_TINYINT   TINYINT,
    T_SMALLINT  SMALLINT,
    T_INTEGER   INTEGER,
    T_BIGINT    BIGINT,
    T_REAL      REAL,
    T_DOUBLE    DOUBLE,
    T_DECIMAL   DECIMAL(38,12),
    T_CHAR      CHAR,
    T_VARCHAR   VARCHAR,
    T_DATE      DATE,
    T_TIMESTAMP TIMESTAMP
) WITH (format = 'PARQUET');


insert into hive.doris_test.orc_basic_data_type
values (true, tinyint '1', smallint '1', integer '126', bigint '123456789', real '12.34', double '12.3456', decimal
        '12.3456789', char 'A', varchar 'Beijing,Shanghai', date '2023-05-23', timestamp '2023-05-24 12:00:00.123'),
       (false, tinyint '1', smallint '1', integer '126', bigint '1234567890123456', real '12.34', double '12.3456', decimal
        '12.345678901', char 'A', varchar 'Beijing,Shanghai', date '2023-05-24', timestamp '2023-05-24 13:00:00.123'),
       (false, tinyint '1', smallint '1', integer '126', bigint '123456789012345678', real '12', double '10', decimal
        '12.3456789012', char 'A', varchar 'Beijing,Shanghai', date '2023-05-25', timestamp '2023-05-24 13:00:00.123'),
       (null, null, null, null, null, null, null, null, null, null, null, null);

insert into hive.doris_test.parquet_basic_data_type
select *
from hive.doris_test.orc_basic_data_type;


CREATE TABLE hive.doris_test.orc_array_data_type
(
    t_int_array       array(integer),
    t_tinyint_array   array(tinyint),
    t_smallint_array  array(smallint),
    t_bigint_array    array(bigint),
    t_real_array      array(real),
    t_double_array    array(double),
    t_string_array    array(varchar),
    t_boolean_array   array(boolean),
    t_timestamp_array array(timestamp (3)),
    t_date_array      array(date),
    t_decimal_array   array(decimal (38, 12))
)
    WITH (
        format = 'ORC'
        );

CREATE TABLE hive.doris_test.parquet_array_data_type
(
    t_int_array       array( integer),
    t_tinyint_array   array(tinyint),
    t_smallint_array  array( smallint),
    t_bigint_array    array(bigint),
    t_real_array      array( real),
    t_double_array    array( double),
    t_string_array    array( varchar),
    t_boolean_array   array(boolean),
    t_timestamp_array array( timestamp (3)),
    t_date_array      array( date),
    t_decimal_array   array( decimal (38, 12))
)
    WITH (
        format = 'PARQUET'
        );

insert into hive.doris_test.orc_array_data_type
values (ARRAY[1,2,3,4,5,6,7],ARRAY[1,2,3,4,5,6,7],ARRAY[1,2,3,4,5,6,7],ARRAY[1234567890123,12345678901234],
        ARRAY[45.123,123.45,11.0],ARRAY[45.12344,123.4544,11.0],ARRAY['TEST','TEST#12123123'],ARRAY[TRUE,FALSE,TRUE,FALSE],
        ARRAY[TIMESTAMP '2023-05-24 13:00:00.123',TIMESTAMP '2023-05-24 14:00:00.123'],
        ARRAY[DATE '2023-05-24',DATE '2023-05-26'],
        ARRAY[DECIMAL '10001.11122233344']);

insert into hive.doris_test.parquet_array_data_type select * from hive.doris_test.orc_array_data_type;


create table hive.doris_test.orc_string_complex
(
    t_string_array  array(varchar),
    t_string_map    map(varchar,varchar),
    t_string_struct row(f_string varchar,f_int varchar)
)WITH (
     FORMAT = 'ORC'
     );

create table hive.doris_test.parquet_string_complex
(
    t_string_array  array(varchar),
    t_string_map    map(varchar,varchar),
    t_string_struct row(f_string varchar,f_int varchar)
)WITH (
     FORMAT = 'PARQUET'
     );

insert into hive.doris_test.orc_string_complex
values (array['1', '2', '3', '北京', 'beijing'],
        map(array['1', '2', '3'], array['1', 'beijing', '北京']),
        row('beijing', '1'));

insert into hive.doris_test.parquet_string_complex
select *
from hive.doris_test.orc_string_complex;

CREATE TABLE hive.doris_test.orc_supplier_partitioned
(
    suppkey   bigint,
    name      varchar(25),
    address   varchar(40),
    phone     varchar(15),
    acctbal   double,
    comment   varchar(101),
    nationkey bigint
)
    WITH (
        format = 'ORC',
        partitioned_by = ARRAY['nationkey']
        );

CREATE TABLE hive.doris_test.parquet_supplier_partitioned
(
    suppkey   bigint,
    name      varchar(25),
    address   varchar(40),
    phone     varchar(15),
    acctbal   double,
    comment   varchar(101),
    nationkey bigint
)
    WITH (
        format = 'PARQUET',
        partitioned_by = ARRAY['nationkey']
        );

insert into hive.doris_test.orc_supplier_partitioned
select suppkey, name, address, phone, acctbal, comment, nationkey
from tpch.sf100.supplier;

insert into hive.doris_test.parquet_supplier_partitioned
select *
from hive.doris_test.orc_supplier_partitioned;

-- partition and bucket
CREATE TABLE hive.doris_test.orc_supplier_partitioned_bucketed
(
    suppkey   bigint,
    name      varchar(25),
    address   varchar(40),
    phone     varchar(15),
    acctbal   double,
    comment   varchar(101),
    nationkey bigint
)
    WITH (
        format = 'ORC',
        partitioned_by = ARRAY['nationkey'],
        bucketed_by = ARRAY['suppkey'],
        bucket_count = 10
        );

CREATE TABLE hive.doris_test.parquet_supplier_partitioned_bucketed
(
    suppkey   bigint,
    name      varchar(25),
    address   varchar(40),
    phone     varchar(15),
    acctbal   double,
    comment   varchar(101),
    nationkey bigint
)
    WITH (
        format = 'PARQUET',
        partitioned_by = ARRAY['nationkey'],
        bucketed_by = ARRAY['suppkey'],
        bucket_count = 10
        );

insert into hive.doris_test.orc_supplier_partitioned_bucketed
select suppkey, name, address, phone, acctbal, comment, nationkey
from tpch.sf100.supplier;

insert into hive.doris_test.parquet_supplier_partitioned_bucketed
select *
from hive.doris_test.orc_supplier_partitioned_bucketed;




