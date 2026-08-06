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

suite("test_simplify_in_predicate") {
    sql "set enable_nereids_planner=true"
    sql 'set enable_fallback_to_original_planner=false;'
    sql 'drop table if exists test_simplify_in_predicate_t'
    sql """CREATE TABLE IF NOT EXISTS `test_simplify_in_predicate_t` (
            a DATE NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY (`a`)
            DISTRIBUTED BY HASH(`a`) BUCKETS 120
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "compression" = "LZ4"
            );"""
    sql """insert into test_simplify_in_predicate_t values( "2023-06-06" );"""

    explain {
        sql "verbose select * from test_simplify_in_predicate_t where a in ('1992-01-31', '1992-02-01', '1992-02-02', '1992-02-03', '1992-02-04');"
        notContains "CAST"
    }

    sql 'drop table if exists test_simplify_in_predicate_datetimev2_t'
    sql """CREATE TABLE IF NOT EXISTS `test_simplify_in_predicate_datetimev2_t` (
            id INT NOT NULL,
            ts6 DATETIMEV2(6) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            );"""
    sql """INSERT INTO test_simplify_in_predicate_datetimev2_t VALUES
            (1, '2024-01-01 12:34:56.123456'),
            (2, '2024-01-01 09:30:00.999999'),
            (3, '2024-01-01 09:30:01.000000'),
            (4, '2024-01-01 22:00:00.000001');"""

    order_qt_datetimev2_narrow_cast """
            SELECT id
            FROM test_simplify_in_predicate_datetimev2_t
            WHERE CAST(ts6 AS DATETIMEV2(3)) IN (
                CAST('2024-01-01 12:34:56.123000' AS DATETIMEV2(6)),
                CAST('2024-01-01 09:30:01.000000' AS DATETIMEV2(6)),
                CAST('2024-01-01 22:00:00.000000' AS DATETIMEV2(6))
            )
            ORDER BY 1
            """

}