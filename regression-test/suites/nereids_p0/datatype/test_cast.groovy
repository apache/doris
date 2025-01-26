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

suite("test_cast") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    def tbl = "test_cast"

    sql """ DROP TABLE IF EXISTS ${tbl}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tbl} (
            `k0` int
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """ INSERT INTO ${tbl} VALUES (101);"""

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 1 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 12 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then -12 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 0 else 1 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 != 101 then 0 else 1 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then '1' else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then '12' else 0 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 'false' else 0 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 'true' else 1 end"
        result([[101]])
    }

    test {
        sql "select cast(false as datetime);"
        result([[null]])
    }

    test {
        sql "select cast(true as datetime);"
        result([[null]])
    }

    test {
        sql "select cast(false as date);"
        result([[null]])
    }

    test {
        sql "select cast(true as date);"
        result([[null]])
    }

    explain {
        sql("select cast('12:00:00' as time);")
        notContains "cast("
    }

    testFoldConst("select cast(cast('16:32:04' as time) as string);")

    check_fold_consistency "cast(cast('11:11:11' as time) as bigint);"
    check_fold_consistency "cast(cast('11:11:11' as time) as char);"
    check_fold_consistency "cast(cast('11:11:11' as time) as double);"
    check_fold_consistency "cast(cast('11:11:11' as time) as float);"
    check_fold_consistency "cast(cast('11:11:11' as time) as int);"
    check_fold_consistency "cast(cast('11:11:11' as time) as integer);"
    check_fold_consistency "cast(cast('11:11:11' as time) as json);"
    check_fold_consistency "cast(cast('11:11:11' as time) as jsonb);"
    check_fold_consistency "cast(cast('11:11:11' as time) as largeint);"
    check_fold_consistency "cast(cast('11:11:11' as time) as smallint);"
    check_fold_consistency "cast(cast('11:11:11' as time) as string);"
    check_fold_consistency "cast(cast('11:11:11' as time) as text);"
    check_fold_consistency "cast(cast('11:11:11' as time) as tinyint);"
    check_fold_consistency "cast(cast('11:11:11' as time) as varchar);"
    check_fold_consistency "cast(cast('11:11:11' as time) as variant);"

    test {
        sql "select cast(cast('11:11:11' as time) as bitmap);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as decimal);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as decimalv2);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as decimalv3);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as hll);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as quantile_state);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as datetime);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as date);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('11:11:11' as time) as quantile_state);"
        exception "cannot cast"
    }

    qt_sql "select cast(11111111 as time);"
    qt_sql "select cast(1111111 as time);"
    qt_sql "select cast('839:00:00' as time);"
    qt_sql "select cast('8385959' as time);"
    qt_sql "select cast(cast('838:59:59' as variant) as time);"
    qt_sql "select cast(cast('838:59:59' as text) as time);"
    qt_sql "select cast(cast('838:59:59' as string) as time);"
    qt_sql "select cast(cast('838:59:59' as char) as time);"
    qt_sql "select cast(cast('838:59:59' as varchar) as time);"
    qt_sql "select cast('111:11' as time);"
    qt_sql "select cast('1111:11' as time);"
    check_fold_consistency "cast(111111 as time);"
    check_fold_consistency "cast(11111 as time);"
    check_fold_consistency "cast(1111 as time);"
    check_fold_consistency "cast(111 as time);"
    check_fold_consistency "cast(11 as time);"
    check_fold_consistency "cast(1 as time);"
    check_fold_consistency "cast(cast(1111.1 as float) as time);"
    check_fold_consistency "cast(cast(1111.1 as double) as time);"
    check_fold_consistency "cast(cast(111111 as json) as time);"
    check_fold_consistency "cast(cast(111111 as jsonb) as time);"
    check_fold_consistency "cast('-01:00:00' as time);"
    check_fold_consistency "cast('11-11-11' as time);"
    check_fold_consistency "cast('11@11@11' as time);"
    check_fold_consistency "cast('11.11.11' as time);"
    check_fold_consistency "cast('11_11_11' as time);"
    check_fold_consistency "cast('11,11,11' as time);"

    test {
        sql "select cast(true as time);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('2025-01-25 11:11:11' as datetime) as time);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast('2025-01-25' as date) as time);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast(1111 as decimalv2) as time);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast(1111 as decimalv3) as time);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast(1111 as ipv4) as time);"
        exception "cannot cast"
    }
    test {
        sql "select cast(cast(1111 as ipv6) as time);"
        exception "cannot cast"
    }

    sql """ DROP TABLE IF EXISTS table_decimal38_4;"""
    sql """
        CREATE TABLE IF NOT EXISTS table_decimal38_4 (
            `k0` decimal(38, 4)
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1");
        """

    sql """ DROP TABLE IF EXISTS table_decimal27_9;"""
    sql """
        CREATE TABLE IF NOT EXISTS table_decimal27_9 (
            `k0` decimal(27, 9)
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1");
        """
    explain {
        sql """select k0 from table_decimal38_4 union all select k0 from table_decimal27_9;"""
        contains """AS decimalv3(38,4)"""
    }
}
