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

suite("test_decimalv3_overflow") {
    sql """ set check_overflow_for_decimal=true; """

    def tblName1 = "test_decimalv3_overflow1"
    sql "drop table if exists ${tblName1}"
	sql """ CREATE  TABLE ${tblName1} (
            `c1` decimalv3(22, 4)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`c1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
	sql "insert into ${tblName1} values(104665062791137173.7169)"

	def tblName2 = "test_decimalv3_overflow2"
	sql "drop table if exists ${tblName2}"
    sql """ CREATE  TABLE ${tblName2} (
              `c2`  decimalv3(20, 2)
          ) ENGINE=OLAP
        UNIQUE KEY(`c2`)
        DISTRIBUTED BY HASH(`c2`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
    sql "insert into ${tblName2} values(705091149953414452.46)"

    qt_sql1 """ select c2 / 10000 * c1 from ${tblName1}, ${tblName2}; """

    //=======================================
    // decimal32
    //=======================================
    /*
    >>> 0xffffffff
    4294967295
    >>> len('4294967295')
    10
    >>> 0x7fffffff
    2147483647
    >>> len('2147483647')
    10
    */
    sql "drop TABLE IF EXISTS test_decimal32_overflow1;"
    sql """
        CREATE TABLE test_decimal32_overflow1(
          k1 decimalv3(1, 0),
          k2 decimalv3(8, 1)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into test_decimal32_overflow1 values
            (1, 1234567.8),
            (9, 9999999.9);
    """

    // result type: (9,1)
    qt_decimal32_overflow1 """
        select k1, k2, k1 * k2 from test_decimal32_overflow1 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal32_overflow2;"
    sql """
        CREATE TABLE test_decimal32_overflow2(
          k1 decimalv3(2, 1),
          k2 decimalv3(9, 8)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal32_overflow2 values
            (1.2, 1.23456789),
            (9.9, 9.99999999);
    """
    // result type: (11,2)
    qt_decimal32_overflow2 """
        select k1, k2, k1 * k2 from test_decimal32_overflow2 order by 1,2;
    """

    //=======================================
    // decimal64
    //=======================================
    /*
    >>> 0xffffffffffffffff
    18446744073709551615
    >>> len('18446744073709551615')
    20
    >>> 0x7fffffffffffffff
    9223372036854775807
    >>> len('9223372036854775807')
    19
    */
    sql "drop TABLE IF EXISTS test_decimal64_overflow1;"
    sql """
        CREATE TABLE test_decimal64_overflow1(
          k1 decimalv3(8, 7),
          k2 decimalv3(10, 9)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal64_overflow1 values
            (1.2345678, 1.234567809),
            (9.9999999, 9.999999999);
    """
    qt_decimal64_overflow1 """
        select k1, k2, k1 * k2 from test_decimal64_overflow1 order by 1,2;
    """

    //=======================================
    // decimal128
    //=======================================
    /*
    >>> 0xffffffffffffffffffffffffffffffff
    340282366920938463463374607431768211455
    >>> len('340282366920938463463374607431768211455')
    39
    >>> 0x7fffffffffffffffffffffffffffffff
    170141183460469231731687303715884105727
    >>> len('170141183460469231731687303715884105727')
    39
    */
    sql """
        drop TABLE IF EXISTS test_decimal128_overflow1;
    """
    sql """
        CREATE TABLE test_decimal128_overflow1(
          k1 decimalv3(6, 1),
          k2 decimalv3(38, 32)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow1 values
            (1.0, 9.0),
            (12345.6, 170141.18346046923173168730371588410572),
            (99999.9, 999999.99999999999999999999999999999999);
    """
    // mulplty result len 39:      9.000000000000000000000000000000000
    //                        170141.183460469231731687303715884105720
    //                        999999.999999999999999999999999999999990
    //                   99999899999.999999999999999999999999999000001
    // original result type: (44, 33)
    // int: 11, scale: 27, 11.27
    // (38, 27)
    qt_decimal128_overflow1 """
        select k1, k2, k1 * k2 from test_decimal128_overflow1 order by 1,2;
    """
    sql "set enable_decimal256=true;"
    qt_decimal128_overflow1_2 """
        select k1, k2, k1 * k2 from test_decimal128_overflow1 order by 1,2;
    """
    sql "set enable_decimal256=false;"


    sql """
        drop TABLE IF EXISTS test_decimal128_overflow1;
    """
    sql """
        CREATE TABLE test_decimal128_overflow1(
          k1 decimalv3(6, 0),
          k2 decimalv3(38, 0)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow1 values
            (10, 17014118346046923173168730371588410572);
    """
    // multiply result not overflow, but result is overflow
    test {
        sql """
        select k1, k2, k1 * k2 from test_decimal128_overflow1 order by 1,2;
        """
        exception "Arithmetic overflow"
    }

    sql """
        drop TABLE IF EXISTS test_decimal128_overflow1;
    """
    sql """
        CREATE TABLE test_decimal128_overflow1(
          k1 decimalv3(6, 1),
          k2 decimalv3(38, 32)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow1 values
            (-99999.9, 999999.99999999999999999999999999999999);
    """
    qt_decimal128_overflow1_3 """
        select k1, k2, k1 * k2 from test_decimal128_overflow1 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow2;"
    sql """
        CREATE TABLE test_decimal128_overflow2(
          k1 decimalv3(38, 1),
          k2 decimalv3(38, 1)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow2 values
        (9999999999999999999999999999999999999.9,9999999999999999999999999999999999999.9);
    """
    // multiply result len 76: 9999999999999999999999999999999999999800000000000000000000000000000000000001
    // original result type: (76, 2)
    // final result: (38, 2)
    test {
        sql "select k1, k2, k1 * k2 from test_decimal128_overflow2 order by 1,2,3;"
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=true;"
    qt_decimal128_overflow2 """
        select k1, k2, k1 * k2 from test_decimal128_overflow2 order by 1,2,3;
    """
    sql "set enable_decimal256=false;"

    // decimal and int
    sql "drop TABLE IF EXISTS test_decimal128_overflow3;"
    sql """
        CREATE TABLE test_decimal128_overflow3(
          k1 int,
          k2 decimalv3(38, 32)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into test_decimal128_overflow3 values
            (1234567890, 999999.99999999999999999999999999999999),
            (2147483647, 999999.99999999999999999999999999999999),
            (10, 999999.99999999999999999999999999999999);
    """
    // (38, 22)
    qt_decimal128_overflow3 """
        select k1, k2, k1 * k2 from test_decimal128_overflow3 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow4;"
    sql """
        CREATE TABLE test_decimal128_overflow4(
          k1 decimalv3(15, 2),
          k2 decimalv3(35, 5)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow4 values
            (100, 123456789012345678901234567890.11112);
    """
    //(38, 6)
    qt_decimal128_overflow4 """
        select k1, k2, k1 * k2 from test_decimal128_overflow4 order by 1,2;
    """
    sql """
        insert into test_decimal128_overflow4 values
            (1000, 123456789012345678901234567890.11112);
    """
    test {
        sql """
        select k1, k2, k1 * k2 from test_decimal128_overflow4 order by 1,2;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=true;"
    qt_decimal128_overflow4_2 """
        select k1, k2, k1 * k2 from test_decimal128_overflow4 order by 1,2;
    """
    sql "set enable_decimal256=false;"

    sql "drop TABLE IF EXISTS test_decimal128_overflow5;"
    sql """
        CREATE TABLE test_decimal128_overflow5(
          k1 decimalv3(15, 6),
          k2 decimalv3(38, 5)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow5 values
            (0.0001, 123456789012345678901234567890123.11112);
    """
    // (38, 6)
    qt_decimal128_overflow5 """
        select k1, k2, k1 * k2 from test_decimal128_overflow5 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow6;"
    sql """
        CREATE TABLE test_decimal128_overflow6(
          k1 decimalv3(15, 8),
          k2 decimalv3(30, 5)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow6 values
            (-0.0000001, 1234564.11112),
            (-0.0000001, 1234567.11112),
            (0.0000001, 1234564.11112),
            (0.0000001, 1234567.11112);
    """
    // (38, 6)
    qt_decimal128_overflow6 """
        select k1, k2, k1 * k2 from test_decimal128_overflow6 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow7;"
    sql """
        CREATE TABLE test_decimal128_overflow7(
          k1 decimalv3(15, 8),
          k2 decimalv3(15, 5)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow7 values
            (0.0000001, 1234567.11112);
    """
    // (38, 6)
    qt_decimal128_overflow7 """
        select k1, k2, k1 * k2 from test_decimal128_overflow7 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow8;"
    sql """
        CREATE TABLE test_decimal128_overflow8(
          k1 decimalv3(34, 4),
          k2 decimalv3(34, 4)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow8 values
            (1000.1234, 1234567.1234);
    """
    // (38, 6)
    qt_decimal128_overflow8 """
        select k1, k2, k1 * k2 from test_decimal128_overflow8 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow9;"
    sql """
        CREATE TABLE test_decimal128_overflow9(
          k1 decimalv3(34, 3),
          k2 decimalv3(34, 3)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow9 values
            (1000.1234, 1234567.1234);
    """
    // (38, 6)
    qt_decimal128_overflow9 """
        select k1, k2, k1 * k2 from test_decimal128_overflow9 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow10;"
    sql """
        CREATE TABLE test_decimal128_overflow10(
          k1 decimalv3(38, 1),
          k2 decimalv3(38, 12)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow10 values
            (1.1, 1234567.11112),
            (9999999999999999999999999999999999999.1, 0.11112);
    """

    // plus: e1:（38,1），e2: (38,12) -> (37 + 12 + 1, 12) -> (50, 12)
    // final type: (38, 1)
    qt_decimal128_overflow10 """
        select k1, k2, k1 + k2 from test_decimal128_overflow10 order by 1,2;
    """
    sql """
        insert into test_decimal128_overflow10 values
            (9999999999999999999999999999999999999.1, 1.11112);
    """
    // TODO: result 10000000000000000000000000000000000000.2 is actually overflowed
    test {
        sql """
        select k1, k2, k1 + k2 from test_decimal128_overflow10 order by 1,2;
        """
        exception "Arithmetic overflow"
    }

    qt_decimal128_overflow10_3 """
        select k1, k2, k1 - k2 from test_decimal128_overflow10 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow11;"
    sql """
        CREATE TABLE test_decimal128_overflow11(
          k1 decimalv3(37, 1),
          k2 decimalv3(37, 36)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow11 values
            (1.1, 1.11112),
            (123456789012345678901234567890123456.7, 1.234567890123456789012345678901234567);
    """
    // plus result type: (36 + 36 + 1=73, 36) -> (38, 38 - 36) -> (38, 2)
    qt_decimal128_overflow11 """
        select k1, k2, k1 + k2 from test_decimal128_overflow11 order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimal128_overflow12;"
    sql """
        CREATE TABLE test_decimal128_overflow12(
          k1 decimalv3(37, 1),
          k2 decimalv3(37, 36)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow12 values
            (999999999999999999999999999999999999.9, 9.999999999999999999999999999999999999);
    """
    // plus result type: (36 + 36 + 1=73, 36) -> (38, 38 - 36) -> (38, 2)
    // 1000000000000000000000000000000000009.90 actually overflowed
    test {
        sql """
        select k1, k2, k1 + k2 from test_decimal128_overflow12 order by 1,2;
        """
        exception "Arithmetic overflow"
    }



    sql "drop TABLE IF EXISTS test_decimal128_overflow13;"
    sql """
        CREATE TABLE test_decimal128_overflow13(
          k1 decimalv3(38, 0),
          k2 decimalv3(38, 0)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    // integer plus result not overflow, but is overflow for the final result
    sql """
        insert into test_decimal128_overflow13 values
            (99999999999999999999999999999999999999, 70141183460469231731687303715884105728);
    """
    test {
        sql """
        select k1, k2, k1 + k2 from test_decimal128_overflow13 order by 1,2;
        """
        exception "Arithmetic overflow"
    }

    sql "drop TABLE IF EXISTS test_decimal128_overflow13;"
    sql """
        CREATE TABLE test_decimal128_overflow13(
          k1 decimalv3(38, 0),
          k2 decimalv3(38, 0)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    // integer plus result not overflow, but is overflow for the final result
    sql """
        insert into test_decimal128_overflow13 values
            (-99999999999999999999999999999999999999, -70141183460469231731687303715884105728);
    """
    test {
        sql """
        select k1, k2, k1 + k2 from test_decimal128_overflow13 order by 1,2;
        """
        exception "Arithmetic overflow"
    }

    sql "drop TABLE IF EXISTS test_decimal128_overflow13;"
    sql """
        CREATE TABLE test_decimal128_overflow13(
          k1 decimalv3(38, 1),
          k2 decimalv3(38, 1)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into test_decimal128_overflow13 values
            (9999999999999999999999999999999999999.9, 9999999999999999999999999999999999999.9);
    """
    // plus result type: (37 + 1 + 1=39, 1) -> (38, 38 - 37) -> (38, 1)
    test {
        sql """
        select k1, k2, k1 + k2 from test_decimal128_overflow13 order by 1,2;
        """
        exception "Arithmetic overflow"
    }

    // decimal256
    /*
    >>> 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
    115792089237316195423570985008687907853269984665640564039457584007913129639935
    >>> len('115792089237316195423570985008687907853269984665640564039457584007913129639935')
    78
    >>> 0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
    57896044618658097711785492504343953926634992332820282019728792003956564819967
    >>> len('57896044618658097711785492504343953926634992332820282019728792003956564819967')
    77
    */

    sql "drop TABLE IF EXISTS test_decimalv3_tb1;"
    sql "drop TABLE IF EXISTS test_decimalv3_tb2;"

    sql """
        create table test_decimalv3_tb1(
            k1 decimalv3(38, 37)
        ) DISTRIBUTED BY HASH(`k1`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        create table test_decimalv3_tb2(
            k2 decimalv3(38, 1) not null
        ) DISTRIBUTED BY HASH(`k2`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into test_decimalv3_tb1 values(9.9999999999999999999999999), (9.9);
    """

    sql """
        insert into test_decimalv3_tb2 values(999999999999999999.9), (9.9);
    """

    qt_union1 """
        select * from (select * from test_decimalv3_tb1 union select * from test_decimalv3_tb2) t order by 1;
    """

    qt_union2 """
        select * from (select * from test_decimalv3_tb2 union select * from test_decimalv3_tb1) t order by 1;
    """

    qt_intersect1 """
        select * from (select * from test_decimalv3_tb1 intersect select * from test_decimalv3_tb2) t order by 1;
    """

    qt_intersect2 """
        select * from (select * from test_decimalv3_tb2 intersect select * from test_decimalv3_tb1) t order by 1;
    """

    qt_except1 """
        select * from (select * from test_decimalv3_tb1 except select * from test_decimalv3_tb2) t order by 1;
    """

    qt_except2 """
        select * from (select * from test_decimalv3_tb2 except select * from test_decimalv3_tb1) t order by 1;
    """
    sql """ CREATE TABLE IF NOT EXISTS test4
            (
                id        BIGINT          NOT NULL COMMENT '',
                price     DECIMAL(38, 18) NOT NULL COMMENT '',
                size      DECIMAL(38, 18) NOT NULL COMMENT ''
            ) UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES(
                "replication_num"="1",
                "compaction_policy" = "time_series",
                "enable_unique_key_merge_on_write" = "true"
            ); """
    sql """ insert into test4 values(1, 62324, 0.00273) """
    qt_sql """ select price, size, price * size from test4; """
    sql "drop table if exists test4"
}
