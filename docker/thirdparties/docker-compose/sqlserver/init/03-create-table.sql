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

CREATE TABLE dbo.TEST_LOWER (
    id int PRIMARY KEY NOT NULL,
    name varchar(10) NOT NULL
);

CREATE TABLE dbo.extreme_test
(
    ID BIGINT NOT NULL PRIMARY KEY,
    -- Signed integer types:
    -- For Int8 simulation, we use SMALLINT with a CHECK constraint between -128 and 127.
    Int8_Col       SMALLINT NOT NULL CHECK (Int8_Col BETWEEN -128 AND 127),
    Int8_Nullable  SMALLINT NULL CHECK (Int8_Nullable BETWEEN -128 AND 127),
    -- Int16
    Int16_Col      SMALLINT NOT NULL,
    Int16_Nullable SMALLINT NULL,
    -- Int32
    Int32_Col      INT NOT NULL,
    Int32_Nullable INT NULL,
    -- Int64
    Int64_Col      BIGINT NOT NULL,
    Int64_Nullable BIGINT NULL,
    -- Unsigned integer types (simulate using a larger type plus a CHECK constraint):
    -- UInt8 (0 to 255) – SQL Server’s TINYINT is naturally unsigned.
    UInt8_Col       TINYINT NOT NULL,
    UInt8_Nullable  TINYINT NULL,
    -- UInt16 (0 to 65535); use INT with a check constraint.
    UInt16_Col       INT NOT NULL CHECK (UInt16_Col BETWEEN 0 AND 65535),
    UInt16_Nullable  INT NULL CHECK (UInt16_Nullable BETWEEN 0 AND 65535),
    -- UInt32 (0 to 4294967295); use BIGINT with a check constraint.
    UInt32_Col       BIGINT NOT NULL CHECK (UInt32_Col BETWEEN 0 AND 4294967295),
    UInt32_Nullable  BIGINT NULL CHECK (UInt32_Nullable BETWEEN 0 AND 4294967295),
    -- UInt64; SQL Server has no 64-bit unsigned, so we use DECIMAL(20,0).
    UInt64_Col       DECIMAL(20,0) NOT NULL CHECK (UInt64_Col >= 0),
    UInt64_Nullable  DECIMAL(20,0) NULL CHECK (UInt64_Nullable >= 0),
    -- Floating point types:
    Float32_Col      REAL NOT NULL,
    Float32_Nullable REAL NULL,
    Float64_Col      FLOAT NOT NULL,
    Float64_Nullable FLOAT NULL,
    -- Decimal types with various precisions and scales:
    Decimal_Col1       DECIMAL(18,2) NOT NULL,
    Decimal_Nullable1  DECIMAL(18,2) NULL,
    Decimal_Col2       DECIMAL(10,5) NOT NULL,
    Decimal_Nullable2  DECIMAL(10,5) NULL,
    Decimal_Col3       DECIMAL(38,10) NOT NULL,
    Decimal_Nullable3  DECIMAL(38,10) NULL,
    -- Date and DateTime types:
    Date_Col        DATE NOT NULL,
    Date_Nullable   DATE NULL,
    Datetime_Col    DATETIME NOT NULL,
    Datetime_Nullable DATETIME NULL,
    -- String types:
    String_Col         VARCHAR(100) NOT NULL,
    String_Nullable    VARCHAR(100) NULL,
    FixedString_Col       CHAR(10) NOT NULL,
    FixedString_Nullable  CHAR(10) NULL,
    -- Enum simulation (using CHAR(1) with a CHECK constraint):
    Enum_Col         CHAR(1) NOT NULL CHECK (Enum_Col IN ('A','B','C')),
    Enum_Nullable    CHAR(1) NULL CHECK (Enum_Nullable IN ('A','B','C') OR Enum_Nullable IS NULL),
    -- UUID (SQL Server’s UNIQUEIDENTIFIER):
    UUID_Col         UNIQUEIDENTIFIER NOT NULL,
    UUID_Nullable    UNIQUEIDENTIFIER NULL,
    -- IP address simulation:
    IPv4_Col         VARCHAR(15) NOT NULL,  -- e.g., '255.255.255.255'
    IPv4_Nullable    VARCHAR(15) NULL,
    IPv6_Col         VARCHAR(39) NOT NULL,  -- e.g., 'FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF'
    IPv6_Nullable    VARCHAR(39) NULL
);


CREATE TABLE dbo.extreme_test_multi_block
(
    ID BIGINT NOT NULL,
    -- Signed integer types:
    -- For Int8 simulation, we use SMALLINT with a CHECK constraint between -128 and 127.
    Int8_Col       SMALLINT NOT NULL CHECK (Int8_Col BETWEEN -128 AND 127),
    Int8_Nullable  SMALLINT NULL CHECK (Int8_Nullable BETWEEN -128 AND 127),
    -- Int16
    Int16_Col      SMALLINT NOT NULL,
    Int16_Nullable SMALLINT NULL,
    -- Int32
    Int32_Col      INT NOT NULL,
    Int32_Nullable INT NULL,
    -- Int64
    Int64_Col      BIGINT NOT NULL,
    Int64_Nullable BIGINT NULL,
    -- Unsigned integer types (simulate using a larger type plus a CHECK constraint):
    -- UInt8 (0 to 255) – SQL Server’s TINYINT is naturally unsigned.
    UInt8_Col       TINYINT NOT NULL,
    UInt8_Nullable  TINYINT NULL,
    -- UInt16 (0 to 65535); use INT with a check constraint.
    UInt16_Col       INT NOT NULL CHECK (UInt16_Col BETWEEN 0 AND 65535),
    UInt16_Nullable  INT NULL CHECK (UInt16_Nullable BETWEEN 0 AND 65535),
    -- UInt32 (0 to 4294967295); use BIGINT with a check constraint.
    UInt32_Col       BIGINT NOT NULL CHECK (UInt32_Col BETWEEN 0 AND 4294967295),
    UInt32_Nullable  BIGINT NULL CHECK (UInt32_Nullable BETWEEN 0 AND 4294967295),
    -- UInt64; SQL Server has no 64-bit unsigned, so we use DECIMAL(20,0).
    UInt64_Col       DECIMAL(20,0) NOT NULL CHECK (UInt64_Col >= 0),
    UInt64_Nullable  DECIMAL(20,0) NULL CHECK (UInt64_Nullable >= 0),
    -- Floating point types:
    Float32_Col      REAL NOT NULL,
    Float32_Nullable REAL NULL,
    Float64_Col      FLOAT NOT NULL,
    Float64_Nullable FLOAT NULL,
    -- Decimal types with various precisions and scales:
    Decimal_Col1       DECIMAL(18,2) NOT NULL,
    Decimal_Nullable1  DECIMAL(18,2) NULL,
    Decimal_Col2       DECIMAL(10,5) NOT NULL,
    Decimal_Nullable2  DECIMAL(10,5) NULL,
    Decimal_Col3       DECIMAL(38,10) NOT NULL,
    Decimal_Nullable3  DECIMAL(38,10) NULL,
    -- Date and DateTime types:
    Date_Col        DATE NOT NULL,
    Date_Nullable   DATE NULL,
    Datetime_Col    DATETIME NOT NULL,
    Datetime_Nullable DATETIME NULL,
    -- String types:
    String_Col         VARCHAR(100) NOT NULL,
    String_Nullable    VARCHAR(100) NULL,
    FixedString_Col       CHAR(10) NOT NULL,
    FixedString_Nullable  CHAR(10) NULL,
    -- Enum simulation (using CHAR(1) with a CHECK constraint):
    Enum_Col         CHAR(1) NOT NULL CHECK (Enum_Col IN ('A','B','C')),
    Enum_Nullable    CHAR(1) NULL CHECK (Enum_Nullable IN ('A','B','C') OR Enum_Nullable IS NULL),
    -- UUID (SQL Server’s UNIQUEIDENTIFIER):
    UUID_Col         UNIQUEIDENTIFIER NOT NULL,
    UUID_Nullable    UNIQUEIDENTIFIER NULL,
    -- IP address simulation:
    IPv4_Col         VARCHAR(15) NOT NULL,  -- e.g., '255.255.255.255'
    IPv4_Nullable    VARCHAR(15) NULL,
    IPv6_Col         VARCHAR(39) NOT NULL,  -- e.g., 'FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF'
    IPv6_Nullable    VARCHAR(39) NULL
);

CREATE TABLE dbo.test_identity_decimal (
	id decimal(18,0) IDENTITY(1,1),
	col int
);
