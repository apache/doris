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

CREATE TABLE doris_test.type
(
    `k1`  Bool,
    `k2`  Date,
    `k3`  Date32,
    `k4`  Datetime,
    `k5`  Datetime64,
    `k6`  Float32,
    `k7`  Float64,
    `k8`  Int8,
    `k9`  Int16,
    `k10` Int32,
    `k11` Int64,
    `k12` Int128,
    `k13` Int256,
    `k14` UInt8,
    `k15` UInt16,
    `k16` UInt32,
    `k17` UInt64,
    `k18` UInt128,
    `k19` UInt256,
    `k20` Decimal(9,2),
    `k21` Decimal(18,2),
    `k22` Decimal(38,2),
    `k23` Decimal(76,2),
    `k24` Enum('hello' = 1, 'world' = 2),
    `k25` IPv4,
    `k26` IPv6,
    `k27` UUID,
    `k28` String,
    `k29` FixedString(2)
)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE doris_test.type_null
(
    `id`  String,
    `k1`  Nullable(Bool),
    `k2`  Nullable(Date),
    `k3`  Nullable(Date32),
    `k4`  Nullable(Datetime),
    `k5`  Nullable(Datetime64),
    `k6`  Nullable(Float32),
    `k7`  Nullable(Float64),
    `k8`  Nullable(Int8),
    `k9`  Nullable(Int16),
    `k10` Nullable(Int32),
    `k11` Nullable(Int64),
    `k12` Nullable(Int128),
    `k13` Nullable(Int256),
    `k14` Nullable(UInt8),
    `k15` Nullable(UInt16),
    `k16` Nullable(UInt32),
    `k17` Nullable(UInt64),
    `k18` Nullable(UInt128),
    `k19` Nullable(UInt256),
    `k20` Nullable(Decimal(9,2)),
    `k21` Nullable(Decimal(18,2)),
    `k22` Nullable(Decimal(38,2)),
    `k23` Nullable(Decimal(76,2)),
    `k24` Nullable(Enum('hello' = 1, 'world' = 2)),
    `k25` Nullable(IPv4),
    `k26` Nullable(IPv6),
    `k27` Nullable(UUID),
    `k28` Nullable(String),
    `k29` Nullable(FixedString(2))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE doris_test.type_ins
(
    `id`  String,
    `k1`  Nullable(Bool),
    `k2`  Nullable(Date),
    `k3`  Nullable(Date32),
    `k4`  Nullable(Datetime),
    `k5`  Nullable(Datetime64),
    `k6`  Nullable(Float32),
    `k7`  Nullable(Float64),
    `k8`  Nullable(Int8),
    `k9`  Nullable(Int16),
    `k10` Nullable(Int32),
    `k11` Nullable(Int64),
    `k12` Nullable(Int128),
    `k13` Nullable(Int256),
    `k14` Nullable(UInt8),
    `k15` Nullable(UInt16),
    `k16` Nullable(UInt32),
    `k17` Nullable(UInt64),
    `k18` Nullable(UInt128),
    `k19` Nullable(UInt256),
    `k20` Nullable(Decimal(9,2)),
    `k21` Nullable(Decimal(18,2)),
    `k22` Nullable(Decimal(38,2)),
    `k23` Nullable(Decimal(76,2)),
    `k24` Nullable(Enum('hello' = 1, 'world' = 2)),
    `k25` Nullable(IPv4),
    `k26` Nullable(IPv6),
    `k27` Nullable(UUID),
    `k28` Nullable(String),
    `k29` Nullable(FixedString(2))
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE doris_test.number
(
    `k6`  Float32,
    `k7`  Float64,
    `k8`  Int8,
    `k9`  Int16,
    `k10` Int32,
    `k11` Int64,
    `k12` Int128,
    `k13` Int256,
    `k14` UInt8,
    `k15` UInt16,
    `k16` UInt32,
    `k17` UInt64,
    `k18` UInt128,
    `k19` UInt256
)
    ENGINE = MergeTree
ORDER BY k6;



CREATE TABLE doris_test.student
(
    id Int16,
    name String, 
    age Int16
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE doris_test.arr
(
    `id`   String,
    `arr1` Array(Bool),
    `arr2` Array(Date),
    `arr3` Array(Date32),
    `arr4` Array(Float32),
    `arr5` Array(Float64),
    `arr6` Array(Int8),
    `arr7` Array(Int16),
    `arr8` Array(Int32),
    `arr9` Array(Int64),
    `arr10` Array(Int128),
    `arr11` Array(Int256),
    `arr12` Array(UInt8),
    `arr13` Array(UInt16),
    `arr14` Array(UInt32),
    `arr15` Array(UInt64),
    `arr16` Array(UInt128),
    `arr17` Array(UInt256),
    `arr18` Array(Decimal(9,2)),
    `arr19` Array(Enum('hello' = 1, 'world' = 2)),
    `arr20` Array(IPv4),
    `arr21` Array(IPv6),
    `arr22` Array(UUID),
    `arr23` Array(Nullable(Int8)),
    `arr24` Array(String), 
    `arr25` Array(LowCardinality(String)),
    `arr26` Array(Datetime),
    `arr27` Array(Datetime64)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE doris_test.arr_null
(
    `id`   String,
    `arr1` Array(Nullable(Bool)),
    `arr2` Array(Nullable(Date)),
    `arr3` Array(Nullable(Date32)),
    `arr4` Array(Nullable(Float32)),
    `arr5` Array(Nullable(Float64)),
    `arr6` Array(Nullable(Int8)),
    `arr7` Array(Nullable(Int16)),
    `arr8` Array(Nullable(Int32)),
    `arr9` Array(Nullable(Int64)),
    `arr10` Array(Nullable(Int128)),
    `arr11` Array(Nullable(Int256)),
    `arr12` Array(Nullable(UInt8)),
    `arr13` Array(Nullable(UInt16)),
    `arr14` Array(Nullable(UInt32)),
    `arr15` Array(Nullable(UInt64)),
    `arr16` Array(Nullable(UInt128)),
    `arr17` Array(Nullable(UInt256)),
    `arr18` Array(Nullable(Decimal(9,2))),
    `arr19` Array(Nullable(Enum('hello' = 1, 'world' = 2))),
    `arr20` Array(Nullable(IPv4)),
    `arr21` Array(Nullable(IPv6)),
    `arr22` Array(Nullable(UUID)),
    `arr23` Array(Nullable(Int8)),
    `arr24` Array(Nullable(String)),
    `arr25` Array(LowCardinality(String)),
    `arr26` Array(Nullable(Datetime)),
    `arr27` Array(Nullable(Datetime64))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE doris_test.arr_ins
(
    `id`   String,
    `arr1` Array(Nullable(Bool)),
    `arr2` Array(Nullable(Date)),
    `arr3` Array(Nullable(Date32)),
    `arr4` Array(Nullable(Float32)),
    `arr5` Array(Nullable(Float64)),
    `arr6` Array(Nullable(Int8)),
    `arr7` Array(Nullable(Int16)),
    `arr8` Array(Nullable(Int32)),
    `arr9` Array(Nullable(Int64)),
    `arr10` Array(Nullable(Int128)),
    `arr11` Array(Nullable(Int256)),
    `arr12` Array(Nullable(UInt8)),
    `arr13` Array(Nullable(UInt16)),
    `arr14` Array(Nullable(UInt32)),
    `arr15` Array(Nullable(UInt64)),
    `arr16` Array(Nullable(UInt128)),
    `arr17` Array(Nullable(UInt256)),
    `arr18` Array(Nullable(Decimal(9,2))),
    `arr19` Array(Nullable(Enum('hello' = 1, 'world' = 2))),
    `arr20` Array(Nullable(IPv4)),
    `arr21` Array(Nullable(IPv6)),
    `arr22` Array(Nullable(UUID)),
    `arr23` Array(Nullable(Int8)),
    `arr24` Array(Nullable(String)),
    `arr25` Array(LowCardinality(String)),
    `arr26` Array(Nullable(Datetime)),
    `arr27` Array(Nullable(Datetime64))
)
ENGINE = MergeTree
ORDER BY id;

set allow_experimental_object_type = 1;
CREATE TABLE doris_test.json
(
    `id` String,
    `o` JSON
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE doris_test.final_test
(
    key Int64,
    some String
)
    ENGINE = ReplacingMergeTree
ORDER BY key;

CREATE TABLE doris_test.ts
(
    id Int64,
    ts Int64
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE doris_test.dt_with_tz
(
    id Int64,
    dt1 DateTime('Asia/Shanghai'),
    dt2 DateTime64(6, 'Asia/Shanghai')
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE doris_test.extreme_test
(
    id UInt64,
    -- Integer types (signed and unsigned) and their Nullable versions
    int8_col         Int8,
    int8_nullable    Nullable(Int8),
    int16_col        Int16,
    int16_nullable   Nullable(Int16),
    int32_col        Int32,
    int32_nullable   Nullable(Int32),
    int64_col        Int64,
    int64_nullable   Nullable(Int64),
    uint8_col        UInt8,
    uint8_nullable   Nullable(UInt8),
    uint16_col       UInt16,
    uint16_nullable  Nullable(UInt16),
    uint32_col       UInt32,
    uint32_nullable  Nullable(UInt32),
    uint64_col       UInt64,
    uint64_nullable  Nullable(UInt64),
    -- Floating point types
    float32_col      Float32,
    float32_nullable Nullable(Float32),
    float64_col      Float64,
    float64_nullable Nullable(Float64),
    -- Decimal type (defined here with precision 18 and scale 2)
    decimal_col      Decimal(18,2),
    decimal_nullable Nullable(Decimal(18,2)),
    -- Date and DateTime types
    date_col         Date,
    date_nullable    Nullable(Date),
    datetime_col     DateTime,
    datetime_nullable Nullable(DateTime),
    -- String types
    string_col         String,
    string_nullable    Nullable(String),
    fixedstring_col    FixedString(10),
    fixedstring_nullable Nullable(FixedString(10)),
    -- Enum type (Enum8 valid range is -128 to 127; here three enum values are defined)
    enum_col         Enum8('A' = 1, 'B' = 2, 'C' = 3),
    enum_nullable    Nullable(Enum8('A' = 1, 'B' = 2, 'C' = 3)),
    -- UUID type
    uuid_col         UUID,
    uuid_nullable    Nullable(UUID),
    -- IP address types
    ipv4_col         IPv4,
    ipv4_nullable    Nullable(IPv4),
    ipv6_col         IPv6,
    ipv6_nullable    Nullable(IPv6)
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE doris_test.extreme_test_multi_block
(
    id UInt64,
    -- Integer types (signed and unsigned) and their Nullable versions
    int8_col         Int8,
    int8_nullable    Nullable(Int8),
    int16_col        Int16,
    int16_nullable   Nullable(Int16),
    int32_col        Int32,
    int32_nullable   Nullable(Int32),
    int64_col        Int64,
    int64_nullable   Nullable(Int64),
    uint8_col        UInt8,
    uint8_nullable   Nullable(UInt8),
    uint16_col       UInt16,
    uint16_nullable  Nullable(UInt16),
    uint32_col       UInt32,
    uint32_nullable  Nullable(UInt32),
    uint64_col       UInt64,
    uint64_nullable  Nullable(UInt64),
    -- Floating point types
    float32_col      Float32,
    float32_nullable Nullable(Float32),
    float64_col      Float64,
    float64_nullable Nullable(Float64),
    -- Decimal type (defined here with precision 18 and scale 2)
    decimal_col      Decimal(18,2),
    decimal_nullable Nullable(Decimal(18,2)),
    -- Date and DateTime types
    date_col         Date,
    date_nullable    Nullable(Date),
    datetime_col     DateTime,
    datetime_nullable Nullable(DateTime),
    -- String types
    string_col         String,
    string_nullable    Nullable(String),
    fixedstring_col    FixedString(10),
    fixedstring_nullable Nullable(FixedString(10)),
    -- Enum type (Enum8 valid range is -128 to 127; here three enum values are defined)
    enum_col         Enum8('A' = 1, 'B' = 2, 'C' = 3),
    enum_nullable    Nullable(Enum8('A' = 1, 'B' = 2, 'C' = 3)),
    -- UUID type
    uuid_col         UUID,
    uuid_nullable    Nullable(UUID),
    -- IP address types
    ipv4_col         IPv4,
    ipv4_nullable    Nullable(IPv4),
    ipv6_col         IPv6,
    ipv6_nullable    Nullable(IPv6)
) ENGINE = MergeTree()
ORDER BY id;