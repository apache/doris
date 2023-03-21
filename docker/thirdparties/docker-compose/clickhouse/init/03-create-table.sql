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
