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

use doris_test;

CREATE TABLE dbo.student (
    id int PRIMARY KEY NOT NULL,
    name varchar(10) NOT NULL, 
    age int NULL
);

CREATE TABLE dbo.test_int (
    id int PRIMARY KEY NOT NULL,
    tinyint_value tinyint NOT NULL,
    smallint_value smallint NULL,
    bigint_value bigint NULL
);

CREATE TABLE dbo.test_float (
    id int PRIMARY KEY NOT NULL,
    real_value real NOT NULL,
    float_value float NULL,
    floatn_value float(5) NULL,
    decimal_value decimal(10,5) NULL,
    numeric_value numeric(10,5) NULL
);

CREATE TABLE dbo.test_decimal (
    id int PRIMARY KEY NOT NULL,
    decimal_value decimal(38,0) NULL,
    numeric_value numeric(38,0) NULL,
    decimal_value2 decimal(38,10) NULL,
    numeric_value2 numeric(38,10) NULL
);

CREATE TABLE dbo.test_char (
    id int PRIMARY KEY NOT NULL,
    char_value char(20) NOT NULL,
    varchar_value varchar(20) NULL,
    varcharmax_value varchar(max) NULL,
    nchar_value nchar(20) NULL,
    nvarchar_value nvarchar(20) NULL,
    nvarcharmax_value nvarchar(max) NULL
);

CREATE TABLE dbo.test_time (
    id int PRIMARY KEY NOT NULL,
    date_value date NOT NULL,
    time_value time NULL,
    datetime_value datetime NULL,
    datetime2_value datetime2 NULL,
    smalldatetime_value smalldatetime NULL,
    datetimeoffset_value datetimeoffset NULL
);

CREATE TABLE dbo.test_text (
    id int PRIMARY KEY NOT NULL,
    text_value text NOT NULL,
    ntext_value ntext NULL
);

CREATE TABLE dbo.test_money (
    id int PRIMARY KEY NOT NULL,
    money_value money NOT NULL,
    smallmoney_value smallmoney NULL
);

CREATE TABLE dbo.test_binary (
    id int PRIMARY KEY NOT NULL,
    bit_value bit NOT NULL,
    binary_value binary(20) NULL,
    varbinary_value varbinary(20) NULL
);

CREATE TABLE dbo.DateAndTime
(
    DateColumn DATE,
    TimeColumn TIME,
    DateTimeColumn DATETIME,
    SmallDateTimeColumn SMALLDATETIME,
    DateTime2Column DATETIME2,
    DateTimeOffsetColumn DATETIMEOFFSET
);

CREATE TABLE dbo.t_id (
    ID uniqueidentifier PRIMARY KEY,
    Name nvarchar(100)
);

CREATE TABLE dbo.all_type (
    id int PRIMARY KEY NOT NULL,
    name varchar(10) NULL,
    age int NULL,
    tinyint_value tinyint NULL,
    smallint_value smallint NULL,
    bigint_value bigint NULL,
    real_value real NULL,
    float_value float NULL,
    floatn_value float(5) NULL,
    decimal_value decimal(38,0) NULL,
    numeric_value numeric(38,0) NULL,
    decimal_value2 decimal(38,10) NULL,
    numeric_value2 numeric(38,10) NULL,
    char_value char(20) NULL,
    varchar_value varchar(20) NULL,
    varcharmax_value varchar(max) NULL,
    nchar_value nchar(20) NULL,
    nvarchar_value nvarchar(20) NULL,
    nvarcharmax_value nvarchar(max) NULL,
    date_value date NULL,
    time_value time NULL,
    datetime_value datetime NULL,
    datetime2_value datetime2 NULL,
    smalldatetime_value smalldatetime NULL,
    datetimeoffset_value datetimeoffset NULL,
    text_value text NULL,
    ntext_value ntext NULL,
    money_value money NULL,
    smallmoney_value smallmoney NULL,
    bit_value bit NULL
);

CREATE TABLE dbo.test_timestamp (
id_col int PRIMARY KEY NOT NULL,
timestamp_col timestamp NULL
);
