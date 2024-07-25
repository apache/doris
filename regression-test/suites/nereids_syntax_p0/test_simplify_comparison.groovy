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
    sql """insert into simple_test_table_t values( 10, 100, 1000, 10000, 100000);"""

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
        sql "verbose select * from simple_test_table_t where e = cast(1.1 as double);"
        contains "CAST(e[#4] AS double) = 1.1"
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
        contains "CAST(e[#4] AS double) > 1.1"
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
        contains "CAST(e[#4] AS double) < 1.1"
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
        contains "CAST(e[#4] AS double) >= 1.1"
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
        contains "CAST(e[#4] AS double) <= 1.1"
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
        sql "verbose select * from simple_test_table_t where e = 1.1;"
        contains "CAST(e[#4] AS double) = 1.1"
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
        contains "CAST(e[#4] AS double) > 1.1"
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
        contains "CAST(e[#4] AS double) < 1.1"
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
        contains "CAST(e[#4] AS double) >= 1.1"
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
        contains "CAST(e[#4] AS double) <= 1.1"
    }
    qt_select1 """select * from simple_test_table_t where cast(a as decimal(5,1)) = 10.0;"""
    qt_select2 """select a.col1, cast(a.col1 as decimal(7,2)) col3, case when a.col1 is null then 15 when cast(a.col1 as decimal(7,2)) < -99997.99 then 18 when cast(a.col1 as decimal(7,2)) < 1.001 then 3 else -55 end col2 from (select 1 as col1) a;"""
}
