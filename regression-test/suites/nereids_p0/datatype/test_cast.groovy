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

    /*
    behaviour changed: not supported anymore from bool to datetime/date
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
    */

    testFoldConst("select cast('12:00:00' as time);")
    testFoldConst("select cast('111111' as time);")

    qt_sql1 "select cast(cast('11:11:11' as time) as char);"
    qt_sql4 "select cast(cast('11:11:11' as time) as string);"
    qt_sql5 "select cast(cast('11:11:11' as time) as text);"
    qt_sql6 "select cast(cast('11:11:11' as time) as varchar);"
    // TODO(lihangyu): need to be fixed
    // qt_sql7 "select cast(cast('11:11:11' as time) as variant);"

    check_fold_consistency "cast(cast('11:11:11' as time) as char);"
    check_fold_consistency "cast(cast('11:11:11' as time) as string);"
    check_fold_consistency "cast(cast('11:11:11' as time) as text);"
    check_fold_consistency "cast(cast('11:11:11' as time) as varchar);"
    check_fold_consistency "cast(cast('11:11:11' as time) as variant);"

    qt_sql8 "select cast('-01:00:00' as time);"
    qt_sql9 "select cast('00:-01:00' as time);"
    qt_sql10 "select cast('00:00:-01' as time);"
    qt_sql11 "select cast('00:00:00' as time);"
    qt_sql12 "select cast('838:59:59' as time);"
    qt_sql13 "select cast('838:60:59' as time);"
    qt_sql14 "select cast('838:59:60' as time);"
    qt_sql15 "select cast('839:00:00' as time);"

    qt_sql16 "select cast('-010000' as time);"
    qt_sql17 "select cast('-000100' as time);"
    qt_sql18 "select cast('-000001' as time);"
    qt_sql19 "select cast('000000' as time);"
    qt_sql20 "select cast('8385959' as time);"
    qt_sql21 "select cast('8386059' as time);"
    qt_sql22 "select cast('8385960' as time);"
    qt_sql23 "select cast('8390000' as time);"

    qt_sql24 "select cast(-010000 as time);"
    qt_sql25 "select cast(-000100 as time);"
    qt_sql26 "select cast(-000001 as time);"
    qt_sql27 "select cast(000000 as time);"
    qt_sql28 "select cast(8385959 as time);"
    qt_sql29 "select cast(8386059 as time);"
    qt_sql30 "select cast(8385960 as time);"
    qt_sql31 "select cast(8390000 as time);"

    //test {
    //    sql "select cast(cast('838:59:59' as variant) as time);"
    //    exception "cannot cast VARIANT to TIMEV2"
    //}
    qt_sql33 "select cast(cast('838:59:59' as text) as time);"
    qt_sql34 "select cast(cast('838:59:59' as string) as time);"
    qt_sql35 "select cast(cast('838:59:59' as char) as time);"
    qt_sql36 "select cast(cast('838:59:59' as varchar) as time);"

    qt_sql37 "select cast('00:00' as time);"
    qt_sql38 "select cast('838:60' as time);"
    qt_sql39 "select cast('838:59' as time);"
    qt_sql40 "select cast('839:00' as time);"

    qt_sql41 "select cast(11111111 as time);"
    qt_sql42 "select cast(1111111 as time);"
    qt_sql43 "select cast(111111 as time);"
    qt_sql44 "select cast(11111 as time);"
    qt_sql45 "select cast(1111 as time);"
    qt_sql46 "select cast(111 as time);"
    qt_sql47 "select cast(11 as time);"
    qt_sql48 "select cast(1 as time);"
    qt_sql49 "select cast(cast(1111.1 as float) as time);"
    qt_sql50 "select cast(cast(1111.1 as double) as time);"
    //test {
    //    sql "select cast(cast(111111 as json) as time);"
    //    exception "cannot cast JSON to TIMEV2"
    //}
    //test {
    //    sql "select cast(cast(111111 as jsonb) as time);"
    //    exception "cannot cast JSON to TIMEV2"
    //}
    // invalid formats
    qt_sql53 "select cast('11-11-11' as time);"
    qt_sql54 "select cast('11@11@11' as time);"
    qt_sql55 "select cast('11.11.11' as time);"
    qt_sql56 "select cast('11_11_11' as time);"
    qt_sql57 "select cast('11,11,11' as time);"

    check_fold_consistency "cast(111111 as time);"
    check_fold_consistency "cast(11111 as time);"
    check_fold_consistency "cast(1111 as time);"
    check_fold_consistency "cast(111 as time);"
    check_fold_consistency "cast(11 as time);"
    check_fold_consistency "cast(1 as time);"
    check_fold_consistency "cast(cast(1111.1 as float) as time);"
    check_fold_consistency "cast(cast(1111.1 as double) as time);"
    // check_fold_consistency "cast(cast(111111 as json) as time);"
    // check_fold_consistency "cast(cast(111111 as jsonb) as time);"
    check_fold_consistency "cast('11-11-11' as time);"
    check_fold_consistency "cast('11@11@11' as time);"
    check_fold_consistency "cast('11.11.11' as time);"
    check_fold_consistency "cast('11_11_11' as time);"
    check_fold_consistency "cast('11,11,11' as time);"

    //test {
    //    sql "select cast(true as time);"
    //    exception "not supported"
    //}
    qt_sql "select cast(cast('2025-01-25 11:11:11' as datetime) as time);"
    //test {
    //    sql "select cast(cast('2025-01-25' as date) as time);"
    //    exception "cannot cast"
    //}
    qt_sql "select cast(cast(1111 as decimalv2) as time);"
    qt_sql "select cast(cast(1111 as decimalv3) as time);"
    //test {
    //    sql "select cast(cast(1111 as ipv4) as time);"
    //    exception "cannot cast"
    //}
    //test {
    //    sql "select cast(cast(1111 as ipv6) as time);"
    //    exception "cannot cast"
    //}
    qt_sql "select cast(cast('2025-01-25 11:11:11' as datetimev2) as time);"
    //test {
    //    sql "select cast(cast('2025-01-25' as datev2) as time);"
    //    exception "cannot cast"
    //}

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
