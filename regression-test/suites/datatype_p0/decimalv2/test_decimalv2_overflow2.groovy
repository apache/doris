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

suite("test_decimalv2_overflow2") {
    sql """ set check_overflow_for_decimal=true; """
    def prepare_decimalv2_overflow_test2 = {
        sql """
            drop TABLE if exists decimalv2_overflow_test2;
        """
        sql """
        CREATE TABLE decimalv2_overflow_test2 (
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
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(999999999999999999.999999998, 0.000000001);
    """
    // const + const
    qt_add_overflow1 """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(999999999999999999.999999998 as decimalv2(27,9)) + cast(0.000000001 as decimalv2(27,9));
    """
    // vector + const
    qt_add_overflow2 """
        select k1, 0.000000001, k1 + cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test2 order by 1,2,3;
    """
    // const + vector
    qt_add_overflow3 """
        select 0.000000001, k1, cast(0.000000001 as decimalv2(27,9)) + k1 from decimalv2_overflow_test2 order by 1,2,3;
    """
    // vector + vector
    qt_add_overflow3 """
        select k1, k2, k1 + k2 from decimalv2_overflow_test2 order by 1,2,3;
    """

    // add overflow
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(999999999999999999.999999999, 0.000000001);
    """
    test {
        sql """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(999999999999999999.999999999 as decimalv2(27,9)) + cast(0.000000001 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, 0.000000001, k1 + cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test2 order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select 0.000000001, k1, cast(0.000000001 as decimalv2(27,9)) + k1 from decimalv2_overflow_test2 order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k2, k1 + k2 from decimalv2_overflow_test2 order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }

    // sub
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(-999999999999999999.999999998, 0.000000001);
    """
    // const - const
    qt_sub_overflow1 """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(-999999999999999999.999999998 as decimalv2(27,9)) - cast(0.000000001 as decimalv2(27,9));
    """
    // vector - const
    qt_sub_overflow2 """
        select k1, 0.000000001, k1 - cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test2 order by 1,2,3;
    """
    // const - vector
    qt_sub_overflow3 """
        select 0.000000001, k1, cast(0.000000001 as decimalv2(27,9)) - k1 from decimalv2_overflow_test2 order by 1,2,3;
    """
    // vector - vector
    qt_sub_overflow3 """
        select k1, k2, k1 - k2 from decimalv2_overflow_test2 order by 1,2,3;
    """

    // sub overflow
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(-999999999999999999.999999999, 0.000000001);
    """
    test {
        sql """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(-999999999999999999.999999999 as decimalv2(27,9)) - cast(0.000000001 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, 0.000000001, k1 - cast(0.000000001 as decimalv2(27,9)) from decimalv2_overflow_test2 order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select 0.000000001, k1, cast(0.100000000 as decimalv2(27,9)) - k1 from decimalv2_overflow_test2 order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k2, k1 - k2 from decimalv2_overflow_test2 order by 1,2,3;
        """
        exception "Arithmetic overflow"
    }

    // multiply
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(999999999999999999.999999999, 1.0);
    """

    // const * const
    qt_multi_overflow1 """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(999999999999999999.999999999 as decimalv2(27,9)) * cast(1.0 as decimalv2(27,9));
    """
    // vector * const
    qt_multi_overflow2 """
        select k1, k1 * 1.000000000 from decimalv2_overflow_test2 order by 1, 2;
    """
    // const * vector
    qt_multi_overflow3 """
        select k1, 1.000000000 * k1 from decimalv2_overflow_test2 order by 1, 2;
    """
    // vector * vector
    qt_multi_overflow4 """
        select k1, k2, k1 * k2 from decimalv2_overflow_test2 order by 1, 2;
    """

    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(999999999999999999.999999999, 1.1);
    """
    test {
        sql """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(999999999999999999.999999999 as decimalv2(27,9)) * cast(1.1 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(999999999999999999.999999999 as decimalv2(27,9)) * cast(999999999999999999.999999999 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k1 * cast(1.1 as decimalv2(27,9)) from decimalv2_overflow_test2 order by 1, 2;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, cast(1.1 as decimalv2(27,9)) * k1 from decimalv2_overflow_test2 order by 1, 2;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, k2, k1 * k2 from decimalv2_overflow_test2 order by 1, 2;
        """
        exception "Arithmetic overflow"
    }

    // divide
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(99999999999999999.999999999, 0.1);
    """
    qt_div_overflow1 """
        select k1, k2, k1 / k2 from decimalv2_overflow_test2 order by 1, 2;
    """
    qt_div_overflow2 """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(99999999999999999.999999999 as decimalv2(27,9)) / cast(0.1 as decimalv2(27,9));
    """
    qt_div_overflow3 """
        select k1, 0.1, k1 / 0.1 from decimalv2_overflow_test2 order by 1, 2, 3;
    """
    qt_div_overflow4 """
        select cast(99999999999999999.999999999 as decimalv2(27,9)) / k2 from decimalv2_overflow_test2 order by 1;
    """

    // divide overflow
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(999999999999999999.999999999, 0.1);
    """
    test {
        sql """
        select k1, k2, k1 / k2 from decimalv2_overflow_test2;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(999999999999999999.999999999 as decimalv2(27,9)) / cast(0.1 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select /*+SET_VAR(enable_fold_constant_by_be=true)*/ cast(999999999999999999.999999999 as decimalv2(27,9)) / cast(0.000000001 as decimalv2(27,9));
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select k1, 0.1, k1 / cast(0.1 as decimalv2(27,9)) from decimalv2_overflow_test2;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(999999999999999999.999999999 as decimalv2(27,9)) / k2 from decimalv2_overflow_test2;
        """
        exception "Arithmetic overflow"
    }

    // mod
    prepare_decimalv2_overflow_test2()
    sql """
        insert into decimalv2_overflow_test2 values(99999999999999999.999999999, 0.1);
    """
    qt_mod1 """
        select k1, k2, k1 % k2 from decimalv2_overflow_test2 order by 1, 2;
    """
    qt_mod2 """
        select cast(99999999999999999.999999999 as decimalv2(27,9)) % cast(0.1 as decimalv2(27,9));
    """
    qt_mod3 """
        select k1, 0.1, k1 % 0.1 from decimalv2_overflow_test2 order by 1, 2, 3;
    """
    qt_mod4 """
        select cast(99999999999999999.999999999 as decimalv2(27,9)) % k2 from decimalv2_overflow_test2 order by 1;
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
