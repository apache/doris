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

CREATE TABLE dbo.test_date_filter (
    id int PRIMARY KEY NOT NULL,
    date_value date NULL,
    datetime_value datetime NULL,
    datetime2_value datetime2 NULL
);

-- User-defined alias types (CREATE TYPE ... FROM base_type). DatabaseMetaData.getColumns()
-- reports such columns with TYPE_NAME set to the alias name, see #67793.
-- They must exist before the tables below are created, hence the batch separator.
CREATE TYPE dbo.doris_alias_varchar FROM varchar(50) NOT NULL;
CREATE TYPE dbo.doris_alias_varcharmax FROM varchar(max) NULL;
CREATE TYPE dbo.doris_alias_nvarchar FROM nvarchar(20) NULL;
CREATE TYPE dbo.doris_alias_nvarcharmax FROM nvarchar(max) NULL;
CREATE TYPE dbo.doris_alias_char FROM char(10) NULL;
CREATE TYPE dbo.doris_alias_nchar FROM nchar(10) NULL;
CREATE TYPE dbo.doris_alias_text FROM text NULL;
CREATE TYPE dbo.doris_alias_ntext FROM ntext NULL;
CREATE TYPE dbo.doris_alias_bit FROM bit NULL;
CREATE TYPE dbo.doris_alias_tinyint FROM tinyint NULL;
CREATE TYPE dbo.doris_alias_smallint FROM smallint NULL;
CREATE TYPE dbo.doris_alias_int FROM int NULL;
CREATE TYPE dbo.doris_alias_bigint FROM bigint NULL;
CREATE TYPE dbo.doris_alias_real FROM real NULL;
CREATE TYPE dbo.doris_alias_float FROM float NULL;
CREATE TYPE dbo.doris_alias_decimal FROM decimal(10, 2) NULL;
CREATE TYPE dbo.doris_alias_numeric FROM numeric(38, 10) NULL;
CREATE TYPE dbo.doris_alias_money FROM money NULL;
CREATE TYPE dbo.doris_alias_smallmoney FROM smallmoney NULL;
CREATE TYPE dbo.doris_alias_date FROM date NULL;
CREATE TYPE dbo.doris_alias_time FROM time NULL;
CREATE TYPE dbo.doris_alias_datetime FROM datetime NULL;
CREATE TYPE dbo.doris_alias_datetime2 FROM datetime2(3) NULL;
CREATE TYPE dbo.doris_alias_datetime2_default FROM datetime2 NULL;
CREATE TYPE dbo.doris_alias_smalldatetime FROM smalldatetime NULL;
CREATE TYPE dbo.doris_alias_guid FROM uniqueidentifier NULL;
CREATE TYPE dbo.doris_alias_identity FROM int NOT NULL;
-- Aliases over types that the JDBC catalog can not resolve by type code. They must stay UNSUPPORTED.
CREATE TYPE dbo.doris_alias_binary FROM binary(20) NULL;
CREATE TYPE dbo.doris_alias_varbinary FROM varbinary(20) NULL;
CREATE TYPE dbo.doris_alias_image FROM image NULL;
CREATE TYPE dbo.doris_alias_datetimeoffset FROM datetimeoffset NULL;
CREATE TYPE dbo.doris_alias_variant FROM sql_variant NULL;
-- Alias names that start with a system type name: they are reported as they are and must not be
-- mistaken for that system type.
CREATE TYPE dbo.[int alias] FROM varchar(50) NULL;
CREATE TYPE dbo.[decimal(18,0) identity] FROM nvarchar(20) NULL;
CREATE TYPE dbo.[int identity] FROM varchar(10) NULL;
GO

-- Every supported base type family behind an alias, plus sysname (a built-in alias over nvarchar(128)).
CREATE TABLE dbo.test_alias_type (
    id int PRIMARY KEY NOT NULL,
    plain_col varchar(50) NULL,
    alias_varchar_col dbo.doris_alias_varchar NULL,
    alias_varcharmax_col dbo.doris_alias_varcharmax NULL,
    alias_nvarchar_col dbo.doris_alias_nvarchar NULL,
    alias_nvarcharmax_col dbo.doris_alias_nvarcharmax NULL,
    alias_char_col dbo.doris_alias_char NULL,
    alias_nchar_col dbo.doris_alias_nchar NULL,
    alias_text_col dbo.doris_alias_text NULL,
    alias_ntext_col dbo.doris_alias_ntext NULL,
    alias_bit_col dbo.doris_alias_bit NULL,
    alias_tinyint_col dbo.doris_alias_tinyint NULL,
    alias_smallint_col dbo.doris_alias_smallint NULL,
    alias_int_col dbo.doris_alias_int NULL,
    alias_bigint_col dbo.doris_alias_bigint NULL,
    alias_real_col dbo.doris_alias_real NULL,
    alias_float_col dbo.doris_alias_float NULL,
    alias_decimal_col dbo.doris_alias_decimal NULL,
    alias_numeric_col dbo.doris_alias_numeric NULL,
    alias_money_col dbo.doris_alias_money NULL,
    alias_smallmoney_col dbo.doris_alias_smallmoney NULL,
    alias_date_col dbo.doris_alias_date NULL,
    alias_time_col dbo.doris_alias_time NULL,
    alias_datetime_col dbo.doris_alias_datetime NULL,
    alias_datetime2_col dbo.doris_alias_datetime2 NULL,
    alias_datetime2_default_col dbo.doris_alias_datetime2_default NULL,
    alias_smalldatetime_col dbo.doris_alias_smalldatetime NULL,
    alias_guid_col dbo.doris_alias_guid NULL,
    sysname_col sysname NULL
);

-- IDENTITY on an alias typed column: the driver reports the plain alias name as TYPE_NAME.
CREATE TABLE dbo.test_alias_identity (
    id dbo.doris_alias_identity IDENTITY(1,1) PRIMARY KEY,
    val dbo.doris_alias_varchar NULL
);

-- Alias names that start with a system type name, next to a real IDENTITY column.
CREATE TABLE dbo.test_alias_name (
    id int IDENTITY(1,1) PRIMARY KEY,
    alias_named_int_col dbo.[int alias] NULL,
    alias_named_decimal_identity_col dbo.[decimal(18,0) identity] NULL,
    alias_named_int_identity_col dbo.[int identity] NULL
);

-- Negative cases: aliases over binary types, datetimeoffset and sql_variant, and the xml / CLR system
-- types. All of them must be reported as UNSUPPORTED while the other columns stay readable.
CREATE TABLE dbo.test_alias_unsupported (
    id int PRIMARY KEY NOT NULL,
    plain_col varchar(50) NULL,
    alias_binary_col dbo.doris_alias_binary NULL,
    alias_varbinary_col dbo.doris_alias_varbinary NULL,
    alias_image_col dbo.doris_alias_image NULL,
    alias_datetimeoffset_col dbo.doris_alias_datetimeoffset NULL,
    alias_variant_col dbo.doris_alias_variant NULL,
    xml_col xml NULL,
    geometry_col geometry NULL,
    hierarchyid_col hierarchyid NULL
);
