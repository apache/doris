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

suite("test_decimalv2_overflow", "nonConcurrent") {
    sql """
        admin set frontend config("enable_decimal_conversion" = "false");
    """

    sql """ set check_overflow_for_decimal=false; """

    def tblName1 = "test_decimalv2_overflow1"
    sql "drop table if exists ${tblName1}"
	sql """ CREATE  TABLE ${tblName1} (
            `c1` decimalv2(22, 4)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`c1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
	sql "insert into ${tblName1} values(104665062791137173.7169)"

	def tblName2 = "test_decimalv2_overflow2"
	sql "drop table if exists ${tblName2}"
    sql """ CREATE  TABLE ${tblName2} (
              `c2`  decimalv2(20, 2)
          ) ENGINE=OLAP
        UNIQUE KEY(`c2`)
        DISTRIBUTED BY HASH(`c2`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
    sql "insert into ${tblName2} values(705091149953414452.46)"

    // qt_sql1 """ select c2 / 10000 * c1 from ${tblName1}, ${tblName2}; """

    qt_sql1 """ select c2 / 10000 * c1 from ${tblName1}, ${tblName2}; """

    sql "drop TABLE IF EXISTS test_decimalv2_overflow;"
    sql """
        CREATE TABLE test_decimalv2_overflow(
          k1 decimalv2(18, 6),
          k2 decimalv2(16, 8)
        )
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into test_decimalv2_overflow values
            (123456789000.123456, 12345678.12345678),
            (999999999999.999999, 99999999.99999999);
    """

    // result type: (9,1)
    qt_decimalv2_calc_overflow """
        select k1, k2, k1 * k2 from test_decimalv2_overflow order by 1,2;
    """

    sql "drop TABLE IF EXISTS test_decimalv2_tb1;"
    sql "drop TABLE IF EXISTS test_decimalv2_tb2;"

    sql """
        create table test_decimalv2_tb1(
            k1 decimalv2(27, 9)
        ) DISTRIBUTED BY HASH(`k1`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        create table test_decimalv2_tb2(
            k2 decimalv2(19, 9)
        ) DISTRIBUTED BY HASH(`k2`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into test_decimalv2_tb1 values(99999999999999999.999999999), (9.9);
    """

    sql """
        insert into test_decimalv2_tb2 values(9999999.999999999), (9.9);
    """

    qt_union1 """
        select * from (select * from test_decimalv2_tb1 union select * from test_decimalv2_tb2) t order by 1;
    """

    qt_union2 """
        select * from (select * from test_decimalv2_tb2 union select * from test_decimalv2_tb1) t order by 1;
    """

    qt_intersect1 """
        select * from (select * from test_decimalv2_tb1 intersect select * from test_decimalv2_tb2) t order by 1;
    """

    qt_intersect2 """
        select * from (select * from test_decimalv2_tb2 intersect select * from test_decimalv2_tb1) t order by 1;
    """

    qt_except1 """
        select * from (select * from test_decimalv2_tb1 except select * from test_decimalv2_tb2) t order by 1;
    """

    qt_except2 """
        select * from (select * from test_decimalv2_tb2 except select * from test_decimalv2_tb1) t order by 1;
    """

    sql """ set check_overflow_for_decimal=true; """
    def prepare_decimalv2_overflow_test = {
        sql """
            drop TABLE if exists decimalv2_overflow_test;
        """
        sql """
        CREATE TABLE decimalv2_overflow_test (
            k1 decimalv2(27, 9),
            k2 decimalv2(27, 9)
        ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`)
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    // max int128: 170141183460469231731687303715884105727, 39 digits

    // add
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(999999999999999999.999999998, 0.000000001);
    """
    // const + const
    qt_add_overflow1 """
        select cast(999999999999999999.999999998 as decimalv2(27,9)) + cast(0.000000001 as decimalv2(27,9));
    """
    // vector + const
    qt_add_overflow2 """
        select k1, 0.000000001, k1 + cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test order by 1,2,3;
    """
    // const + vector
    qt_add_overflow3 """
        select 0.000000001, k1, cast(0.000000001 as decimalv2(27,9)) + k1 from decimalv2_overflow_test order by 1,2,3;
    """
    // vector + vector
    qt_add_overflow3 """
        select k1, k2, k1 + k2 from decimalv2_overflow_test order by 1,2,3;
    """

    // add overflow
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(999999999999999999.999999999, 0.000000001);
    """
    test {
        sql """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) + cast(0.000000001 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, 0.000000001, k1 + cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select 0.000000001, k1, cast(0.000000001 as decimalv2(27,9)) + k1 from decimalv2_overflow_test order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k2, k1 + k2 from decimalv2_overflow_test order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }

    // sub
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(-999999999999999999.999999998, 0.000000001);
    """
    // const - const
    qt_sub_overflow1 """
        select cast(-999999999999999999.999999998 as decimalv2(27,9)) - cast(0.000000001 as decimalv2(27,9));
    """
    // vector - const
    qt_sub_overflow2 """
        select k1, 0.000000001, k1 - cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test order by 1,2,3;
    """
    // const - vector
    qt_sub_overflow3 """
        select 0.000000001, k1, cast(0.000000001 as decimalv2(27,9)) - k1 from decimalv2_overflow_test order by 1,2,3;
    """
    // vector - vector
    qt_sub_overflow3 """
        select k1, k2, k1 - k2 from decimalv2_overflow_test order by 1,2,3;
    """

    // sub overflow
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(-999999999999999999.999999999, 0.000000001);
    """
    test {
        sql """
        select cast(-999999999999999999.999999999 as decimalv2(27,9)) - cast(0.000000001 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, 0.000000001, k1 - cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select 0.000000001, k1, cast(0.100000000 as decimalv2(27,9)) - k1 from decimalv2_overflow_test order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k2, k1 - k2 from decimalv2_overflow_test order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }

    // multiply
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(999999999999999999.999999999, 1.0);
    """

    // const * const
    qt_multi_overflow1 """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) * cast(1.0 as decimalv2(27,9));
    """
    // vector * const
    qt_multi_overflow2 """
        select k1, k1 * 1.000000000 from decimalv2_overflow_test order by 1, 2;
    """
    // const * vector
    qt_multi_overflow3 """
        select k1, 1.000000000 * k1 from decimalv2_overflow_test order by 1, 2;
    """
    // vector * vector
    qt_multi_overflow4 """
        select k1, k2, k1 * k2 from decimalv2_overflow_test order by 1, 2;
    """

    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(999999999999999999.999999999, 1.1);
    """
    test {
        sql """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) * cast(1.1 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) * cast(999999999999999999.999999999 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k1 * 1.1 from decimalv2_overflow_test order by 1, 2;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, 1.1 * k1 from decimalv2_overflow_test order by 1, 2;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k2, k1 * k2 from decimalv2_overflow_test order by 1, 2;
        """
        exception "Arithmetic overflow"
    }

    // divide
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(99999999999999999.999999999, 0.1);
    """
    qt_div_overflow1 """
        select k1, k2, k1 / k2 from decimalv2_overflow_test order by 1, 2;
    """
    qt_div_overflow2 """
        select cast(99999999999999999.999999999 as decimalv2(27,9)) / cast(0.1 as decimalv2(27,9));
    """
    qt_div_overflow3 """
        select k1, 0.1, k1 / 0.1 from decimalv2_overflow_test order by 1, 2, 3;
    """
    qt_div_overflow4 """
        select cast(99999999999999999.999999999 as decimalv2(27,9)) / k2 from decimalv2_overflow_test order by 1;
    """

    // divide overflow
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(999999999999999999.999999999, 0.1);
    """
    test {
        sql """
        select k1, k2, k1 / k2 from decimalv2_overflow_test;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) / cast(0.1 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) / cast(0.000000001 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, 0.1, k1 / 0.1 from decimalv2_overflow_test;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) / k2 from decimalv2_overflow_test;
        """
        exception "Arithmetic overflow"
    }

    // mod
    prepare_decimalv2_overflow_test()
    sql """
        insert into decimalv2_overflow_test values(99999999999999999.999999999, 0.1);
    """
    qt_mod1 """
        select k1, k2, k1 % k2 from decimalv2_overflow_test order by 1, 2;
    """
    qt_mod2 """
        select cast(99999999999999999.999999999 as decimalv2(27,9)) % cast(0.1 as decimalv2(27,9));
    """
    qt_mod3 """
        select k1, 0.1, k1 % 0.1 from decimalv2_overflow_test order by 1, 2, 3;
    """
    qt_mod4 """
        select cast(99999999999999999.999999999 as decimalv2(27,9)) % k2 from decimalv2_overflow_test order by 1;
    """

    // TODO
    // decimalv2 +-*/ integer
    // integer +-*/ decimalv2

    // decimalv2 +-*/ decimalv3
    // decimalv3 +-*/ decimalv2

    // decimalv2 mod, pmod

    // decimalv2 largeint

    // decimalv3 +-*/ integer
    // integer +-*/ decimalv3

    /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
    // {decimalv2 | decimalv3} +-*/ {float, double}
}
