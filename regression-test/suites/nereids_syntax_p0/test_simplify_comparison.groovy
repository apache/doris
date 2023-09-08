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

suite("test_simplify_comparison") {
    sql "set enable_nereids_planner=true"
    sql 'set enable_fallback_to_original_planner=false;'
    sql 'drop table if exists log_items_test'
    sql """CREATE TABLE IF NOT EXISTS `log_items_test` (
            a DATETIME NOT NULL,
            b decimal(10,2)
            ) ENGINE=OLAP
            UNIQUE KEY (`a`)
            DISTRIBUTED BY HASH(`a`) BUCKETS 120
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "compression" = "LZ4",
            "storage_cooldown_time" = "9999-12-31 23:59:59",
            "enable_unique_key_merge_on_write" = "true"
            );"""
    sql """insert into log_items_test values( "2023-06-06", 111.11 );"""

    explain {
        sql "verbose select * from log_items_test where a < '2023-06-15 23:59:59.999' and b < 111.111;"
        notContains "CAST"
        contains "< 111.12"
        contains "< '2023-06-16 00:00:00'"
    }

    explain {
        sql "verbose select * from log_items_test where a <= '2023-06-15 23:59:59.999' and b <= 111.111;"
        notContains "CAST"
        contains "<= 111.11"
        contains "<= '2023-06-15 23:59:59'"
    }

    explain {
        sql "verbose select * from log_items_test where a = '2023-06-15 23:59:59.999' and b = 111.111;"
        notContains "CAST"
        notContains "111.12"
        notContains "2023-06-16 00:00:00"
        notContains "111.11"
        notContains "2023-06-15 23:59:59"
    }

    explain {
        sql "verbose select * from log_items_test where a > '2023-06-15 23:59:59.999' and b > 111.111;"
        notContains "CAST"
        contains "> 111.11"
        contains "> '2023-06-15 23:59:59'"
    }

    explain {
        sql "verbose select * from log_items_test where a >= '2023-06-15 23:59:59.999' and b >= 111.111;"
        notContains "CAST"
        contains ">= 111.12"
        contains ">= '2023-06-16 00:00:00'"
    }

    sql "select cast('1234' as decimalv3(18,4)) > 2000;"

    sql 'drop table if exists simple_test_table_t;'
    sql """CREATE TABLE IF NOT EXISTS `simple_test_table_t` (
            a tinyint,
            b smallint,
            c int,
            d bigint,
            e largeint
            ) ENGINE=OLAP
            UNIQUE KEY (`a`)
            DISTRIBUTED BY HASH(`a`) BUCKETS 120
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "compression" = "LZ4"
            );"""

    explain {
        sql "verbose select * from simple_test_table_t where a = cast(1.0 as double) and b = cast(1.0 as double) and c = cast(1.0 as double) and d = cast(1.0 as double);"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e = cast(1.0 as double);"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a > cast(1.0 as double) and b > cast(1.0 as double) and c > cast(1.0 as double) and d > cast(1.0 as double);"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e > cast(1.0 as double);"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a < cast(1.0 as double) and b < cast(1.0 as double) and c < cast(1.0 as double) and d < cast(1.0 as double);"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e < cast(1.0 as double);"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a >= cast(1.0 as double) and b >= cast(1.0 as double) and c >= cast(1.0 as double) and d >= cast(1.0 as double);"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e >= cast(1.0 as double);"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a <= cast(1.0 as double) and b <= cast(1.0 as double) and c <= cast(1.0 as double) and d <= cast(1.0 as double);"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e <= cast(1.0 as double);"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a = cast(1.1 as double) and b = cast(1.1 as double) and c = cast(1.1 as double) and d = cast(1.1 as double);"
        contains "a[#0] IS NULL"
        contains "b[#1] IS NULL"
        contains "c[#2] IS NULL"
        contains "d[#3] IS NULL"
        contains "AND NULL"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e = cast(1.1 as double);"
        contains "CAST(e[#4] AS DOUBLE) = 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a > cast(1.1 as double) and b > cast(1.1 as double) and c > cast(1.1 as double) and d > cast(1.1 as double);"
        contains "a[#0] > 1"
        contains "b[#1] > 1"
        contains "c[#2] > 1"
        contains "d[#3] > 1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e > cast(1.1 as double);"
        contains "CAST(e[#4] AS DOUBLE) > 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a < cast(1.1 as double) and b < cast(1.1 as double) and c < cast(1.1 as double) and d < cast(1.1 as double);"
        contains "a[#0] < 2"
        contains "b[#1] < 2"
        contains "c[#2] < 2"
        contains "d[#3] < 2"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e < cast(1.1 as double);"
        contains "CAST(e[#4] AS DOUBLE) < 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a >= cast(1.1 as double) and b >= cast(1.1 as double) and c >= cast(1.1 as double) and d >= cast(1.1 as double);"
        contains "a[#0] >= 2"
        contains "b[#1] >= 2"
        contains "c[#2] >= 2"
        contains "d[#3] >= 2"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e >= cast(1.1 as double);"
        contains "CAST(e[#4] AS DOUBLE) >= 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a <= cast(1.1 as double) and b <= cast(1.1 as double) and c <= cast(1.1 as double) and d <= cast(1.1 as double);"
        contains "a[#0] <= 1"
        contains "b[#1] <= 1"
        contains "c[#2] <= 1"
        contains "d[#3] <= 1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e <= cast(1.1 as double);"
        contains "CAST(e[#4] AS DOUBLE) <= 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a = 1.0 and b = 1.0 and c = 1.0 and d = 1.0;"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e = 1.0;"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a > 1.0 and b > 1.0 and c > 1.0 and d > 1.0;"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e > 1.0;"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a < 1.0 and b < 1.0 and c < 1.0 and d < 1.0;"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e < 1.0;"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a >= 1.0 and b >= 1.0 and c >= 1.0 and d >= 1.0;"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e >= 1.0;"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a <= 1.0 and b <= 1.0 and c <= 1.0 and d <= 1.0;"
        notContains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e <= 1.0;"
        contains "CAST"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a = 1.1 and b = 1.1 and c = 1.1 and d = 1.1;"
        contains "a[#0] IS NULL"
        contains "b[#1] IS NULL"
        contains "c[#2] IS NULL"
        contains "d[#3] IS NULL"
        contains "AND NULL"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e = 1.1;"
        contains "CAST(e[#4] AS DOUBLE) = 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a > 1.1 and b > 1.1 and c > 1.1 and d > 1.1;"
        contains "a[#0] > 1"
        contains "b[#1] > 1"
        contains "c[#2] > 1"
        contains "d[#3] > 1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e > 1.1;"
        contains "CAST(e[#4] AS DOUBLE) > 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a < 1.1 and b < 1.1 and c < 1.1 and d < 1.1;"
        contains "a[#0] < 2"
        contains "b[#1] < 2"
        contains "c[#2] < 2"
        contains "d[#3] < 2"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e < 1.1;"
        contains "CAST(e[#4] AS DOUBLE) < 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a >= 1.1 and b >= 1.1 and c >= 1.1 and d >= 1.1;"
        contains "a[#0] >= 2"
        contains "b[#1] >= 2"
        contains "c[#2] >= 2"
        contains "d[#3] >= 2"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e >= 1.1;"
        contains "CAST(e[#4] AS DOUBLE) >= 1.1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where a <= 1.1 and b <= 1.1 and c <= 1.1 and d <= 1.1;"
        contains "a[#0] <= 1"
        contains "b[#1] <= 1"
        contains "c[#2] <= 1"
        contains "d[#3] <= 1"
    }

    explain {
        sql "verbose select * from simple_test_table_t where e <= 1.1;"
        contains "CAST(e[#4] AS DOUBLE) <= 1.1"
    }
}