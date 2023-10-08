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
    ts UInt64
)
ENGINE = MergeTree
ORDER BY id;