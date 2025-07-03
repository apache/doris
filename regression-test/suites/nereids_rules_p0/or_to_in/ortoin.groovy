// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


suite("ortoin") {
    sql """
    drop table if exists table_10_undef_partitions2_keys3_properties4_distributed_by53;
    CREATE TABLE `table_10_undef_partitions2_keys3_properties4_distributed_by53` (
    `pk` int NULL,
    `col_varchar_10__undef_signed` varchar(10) NULL,
    `col_bigint_undef_signed` bigint NULL,
    `col_varchar_64__undef_signed` varchar(64) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`pk`, `col_varchar_10__undef_signed`)
    DISTRIBUTED BY HASH(`pk`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1");
    """

    // OrToIn Rule set state flag in Or expression. SimplifyRange Rule may remove this flag, and hence 
    // OrToIn and SimplifyRange Rules go into dead loop.
    // to fix this bug, if the input expression and output expression of SimplifyRange Rule are the same,
    // SimplifyRange will return input expression to keep expression state.
    // 
    sql """
    explain
    SELECT
        *
    FROM
        table_10_undef_partitions2_keys3_properties4_distributed_by53
    WHERE
    (
            `pk` IN (2, -114)
            AND `pk` IS NOT NULL
        )
        OR 
        (
            col_varchar_64__undef_signed IS NULL
            AND  `pk` IN (7, 1)
            AND `pk` IS NOT NULL
        )
        OR
        (
            (`pk` <> -89)
        )
        ; 
    """
}