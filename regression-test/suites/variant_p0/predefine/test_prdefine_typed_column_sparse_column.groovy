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

suite("test_predefine_typed_sparse", "p0"){ 

    def tableName = "test_predefine_typed_sparse"
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'a' : date,
            MATCH_NAME 'b' : decimal(20,12),
            MATCH_NAME 'c' : datetime,
            MATCH_NAME 'd' : date,
            properties("variant_max_subcolumns_count" = "2")
        > NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    for (int i = 0; i < 10; i++) {
        sql """insert into ${tableName} values(1, '{"a": "2025-04-16", "b": 123.123456789012, "c": "2025-04-17T09:09:09Z", "d": "2025-04-18", "e": "2025-04-19", "f": "2025-04-20", "g": "2025-04-21", "h": "2025-04-22", "i": "2025-04-23", "j": "2025-04-24", "k": "2025-04-25", "l": "2025-04-26", "m": "2025-04-27", "n": "2025-04-28", "o": "2025-04-29", "p": "2025-04-30"}');"""
        sql """insert into ${tableName} values(2, '{"a": "2025-04-16", "b": 123.123456789012, "c": "2025-04-17T09:09:09Z", "d": "2025-04-18", "e": "2025-04-19", "f": "2025-04-20", "g": "2025-04-21", "h": "2025-04-22", "i": "2025-04-23", "j": "2025-04-24", "k": "2025-04-25", "l": "2025-04-26", "m": "2025-04-27", "n": "2025-04-28", "o": "2025-04-29", "p": "2025-04-30"}');"""
    }

    qt_sql """ select variant_type(var) from ${tableName} order by id """
    qt_sql """ select * from ${tableName} order by id """
    trigger_and_wait_compaction(tableName, "cumulative")
    qt_sql """ select * from ${tableName} order by id """

    qt_sql """ select variant_type(var) from ${tableName} order by id """
}